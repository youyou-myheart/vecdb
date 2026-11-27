"""Measure fixed-arrival, single-vector writes through both gRPC Gateways of a native onebox."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import grpc

from capacity_explore import CapacityTrial
from follower_catchup_probe import summarize_capture
from onebox_e2e import ROOT
from onebox_freshness import publishers_ready
from pipeline_probe import collect, interval
from quality import save
from resource_guard import ResourceGuard, clean_owned_state, host_sample, limit_reason
from vectorstore import Client


class BenchGuard(ResourceGuard):
    limit_owned_disk = False
    limit_busy_cpu = False

    def processes(self):
        return super().processes() + list(self.fixture.drivers)


def aggregate(reports, seconds):
    started = sum(x['Started'] for x in reports)
    ack = sum(x['Ack'] for x in reports)
    errors, examples, histogram = Counter(), {}, Counter()
    timeline = [dict(sent=0, ack=0, failed=0) for _ in range(seconds + 5)]
    for report in reports:
        assert report['Target'] == report['Started'] + report['Unsent']
        assert report['Started'] == report['Ack'] + sum(report['Errors'].values())
        assert sum(report['AckShard']) == report['Ack']
        errors.update(report['Errors'])
        examples.update(report['ErrorExample'])
        histogram.update({int(bound): n for bound, n in report['LatencyHistogramUS'].items()})
        for row, sample in zip(timeline, report['BySecond']):
            row['sent'] += sample['Started']
            row['ack'] += sample['Ack']
            row['failed'] += sample['Failed']

    def quantile(q):
        seen = 0
        for bound, count in sorted(histogram.items()):
            seen += count
            if ack and seen >= q * ack:
                return bound / 1000

    assert sum(histogram.values()) == ack == sum(row['ack'] for row in timeline)
    assert sum(row['sent'] for row in timeline) == started
    assert sum(row['failed'] for row in timeline) == sum(errors.values())
    finished = all(not x['Interrupted'] for x in reports)
    elapsed = [x['ElapsedSeconds'] for x in reports]
    duration = seconds if finished else min(seconds, max(elapsed))
    in_window = sum(row['ack'] for row in timeline[:seconds])
    return dict(
        planned=sum(x['Target'] for x in reports), invoked=started, durable_acks=ack,
        not_sent=sum(x['Unsent'] for x in reports), rpc_failures=dict(errors), error_examples=examples,
        full_window=finished, generation_seconds=duration, invoked_per_generation_second=started / duration,
        sent_qps=started / seconds if finished else None,
        ack_qps_in_window=in_window / seconds if finished else None,
        ack_after_window=ack - in_window, per_second=timeline,
        p50_ack_ms_upper=quantile(.50), p95_ack_ms_upper=quantile(.95), p99_ack_ms_upper=quantile(.99),
        peak_inflight_per_client=[x['PeakInflight'] for x in reports], elapsed_per_client=elapsed,
        last_ack_ns=max(x['LastAckNS'] for x in reports),
        ack_shards=[sum(x['AckShard'][i] for x in reports) for i in range(16)],
    )


def follower_sample(result, attempt, sent, received, t0):
    def pos(p):
        return dict(hash=p.hash, sequence=p.sequence, data_offset=p.data_offset)

    reads = [dict(index=r.index_partition, replica=r.replica, session=r.session, epoch=r.owner_epoch,
                  node=r.node, target=pos(r.target), applied=pos(r.applied), query_ms=r.query_ms,
                  queue_ms=r.queue_ms, capture_ms=r.leader_capture_ms, wait_apply_ms=r.wait_apply_ms)
             for r in result.follower_reads]
    return dict(attempt=attempt, sent_ms=(sent - t0) * 1000, received_ms=(received - t0) * 1000,
                hit=[h.id for h in result.hits], reads=reads)


def catchup(client, load, *, centers, read_state, check_background, logs, drain_seconds):
    if not load['last_ack_ns']:
        raise RuntimeError('no durable ACK to use as the catch-up origin')
    # The Go clients and this observer share the same host clock.
    t0 = time.monotonic() - (time.time_ns() - load['last_ack_ns']) / 1e9
    samples, observations = [], []
    index_ids = {f'index-{i:04d}' for i in range(len(centers))}
    pinned, frontier = None, None
    until = time.monotonic() + drain_seconds
    check_at = 0
    status_read = None
    attempt, first_probe_ms = 0, None
    def timed_state():
        started = time.monotonic()
        state = read_state()
        return state, (time.monotonic() - started) * 1000
    # Keep probing while the control plane reads all source and graph positions.
    observer = ThreadPoolExecutor(max_workers=1)
    try:
        while time.monotonic() < until:
            check_background()
            if status_read is None and time.monotonic() >= check_at:
                status_read = observer.submit(timed_state)
            sent = time.monotonic()
            attempt += 1
            sent_ms = (sent - t0) * 1000
            if first_probe_ms is None:
                first_probe_ms = sent_ms
            try:
                result = client.follower_search(centers[0], k=1, timeout=2)
            except grpc.RpcError as err:
                observations.append(dict(attempt=attempt, sent_ms=sent_ms, at_ms=(time.monotonic() - t0) * 1000,
                                         grpc_error=err.code().name))
            else:
                samples.append(follower_sample(result, attempt, sent, time.monotonic(), t0))
            if status_read is not None and status_read.done():
                try:
                    state, status_ms = status_read.result()
                except TimeoutError as err:
                    observations.append(dict(at_ms=(time.monotonic() - t0) * 1000, status_error=str(err)))
                    status_read = None
                    check_at = time.monotonic() + .5
                    continue
                status_read = None
                data = state['data_progress']
                assert set(data) == {f'data-{i:04d}' for i in range(len(load['ack_shards']))}
                if any(not d.get('progress_fresh', d.get('owner_available', False)) for d in data.values()):
                    observations.append(dict(at_ms=(time.monotonic() - t0) * 1000, status_ms=status_ms,
                                             status_error='current data progress unavailable'))
                    check_at = time.monotonic() + .5
                    continue
                heads = {k: v['head'] for k, v in data.items()}
                if heads != pinned:
                    # Timeouts can have committed. Include observed durable events
                    # from those requests and account for every acknowledged write.
                    assert all(heads[f'data-{i:04d}'] >= n for i, n in enumerate(load['ack_shards']))
                    pinned, frontier = heads, None
                assert all(0 <= d['completed_lsn'] <= heads[k] for k, d in data.items())
                pending = sum(pinned[k] - d['completed_lsn'] for k, d in data.items())
                observations.append(dict(at_ms=(time.monotonic() - t0) * 1000, status_ms=status_ms,
                                         pending=pending, head=sum(heads.values())))
                if pending == 0 and frontier is None:
                    graph_heads = state.get('graph_heads', {})
                    if set(graph_heads) != index_ids:
                        observations.append(dict(at_ms=(time.monotonic() - t0) * 1000,
                                                 status_error='current graph progress unavailable'))
                        check_at = time.monotonic() + .5
                        continue
                    frontier = {k: v['committed'] for k, v in graph_heads.items()}
                check_at = time.monotonic() + .5
            if frontier:
                hit = summarize_capture(samples, frontier)
                if hit and hit['hit']:
                    hit['first_probe_ms'] = first_probe_ms
                    return dict(status='confirmed', **hit, pinned_durable_events=sum(pinned.values()),
                                probe_errors=sum('grpc_error' in obs for obs in observations))
            time.sleep(.1)
        return dict(status='not_confirmed', first_probe_ms=first_probe_ms,
                    probe_errors=sum('grpc_error' in obs for obs in observations),
                    last_observation=observations[-1] if observations else None,
                    budget_seconds=drain_seconds)
    finally:
        observer.shutdown(wait=True)
        save(logs / 'drain.json', dict(pinned=pinned, frontier=frontier, observations=observations,
                                       samples=samples))


def ready_follower(client, centers):
    try:
        result = client.follower_search(centers[0], k=1, timeout=3)
    except grpc.RpcError as err:
        if err.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            return False
        raise
    return len(result.follower_reads) == len(centers)


def worker_stage_sample(urls, token):
    started = time.time_ns()
    nodes = collect(urls, token)
    return dict(started_ns=started, captured_ns=time.time_ns(), nodes=nodes)


def capture_drain_stages(stop, urls, token, logs, last_ack_ns, before_stop=None):
    """Sample worker histograms off the Follower Read and status paths."""
    with (logs / 'worker-stages.jsonl').open('w') as out:
        def write(sample):
            sample['since_last_ack_s'] = (sample['captured_ns'] - last_ack_ns) / 1e9
            out.write(json.dumps(sample) + '\n')
            out.flush()

        if before_stop is not None:
            write(before_stop.result())
        while True:
            write(worker_stage_sample(urls, token))
            if stop.is_set():
                return
            stop.wait(5)


def write_window(args, logs, token, drivers, check_background, before_end=None):
    outputs = []
    barrier = logs / 'start-ns'
    env = dict(os.environ, API_TOKEN=token, GOMAXPROCS='2')
    for i in range(args.clients):
        out = logs / f'client-{i}.json'
        outputs.append(out)
        cmd = [str(ROOT / '.tools/writebench'), '-endpoint', f'127.0.0.1:{args.base_port + 180 + i % args.write_gateways}',
               '-client', str(i), '-rate', str(args.qps // args.clients), '-seconds', str(args.seconds),
               '-inflight', str(args.inflight), '-output', str(out), '-barrier', str(barrier)]
        if args.write_routing != 'client':
            cmd += ['-peer', f'127.0.0.1:{args.base_port + 181 - i % 2}']
            if args.write_routing == 'partition':
                cmd.append('-by-partition')
        with out.with_suffix('.log').open('w') as log:
            drivers.append(subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT))
    deadline = time.monotonic() + 15
    while not all(Path(str(p) + '.ready').exists() for p in outputs):
        check_background()
        if any(p.poll() is not None for p in drivers):
            raise RuntimeError('load driver exited before start')
        if time.monotonic() >= deadline:
            raise TimeoutError('load driver readiness')
        time.sleep(.05)
    tmp = barrier.with_suffix('.tmp')
    start_ns = time.time_ns() + 1_000_000_000
    sample_at = start_ns + max(0, args.seconds - 2) * 1_000_000_000
    tmp.write_text(str(start_ns))
    tmp.replace(barrier)
    print('WRITING', args.qps, 'planned RPC/s for', args.seconds, 'seconds', flush=True)
    deadline = time.monotonic() + args.seconds + 12
    while any(p.poll() is None for p in drivers):
        if before_end is not None and time.time_ns() >= sample_at:
            before_end()
            before_end = None
        if time.monotonic() > deadline:
            raise TimeoutError('load drivers')
        time.sleep(.01)
    if not all(p.returncode == 0 for p in drivers):
        check_background()
        raise RuntimeError('load driver failed')
    return aggregate([json.loads(p.read_text()) for p in outputs], args.seconds)


def stop_drivers(drivers):
    for process in drivers:
        if process.poll() is None:
            process.terminate()
    for process in drivers:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run(args):
    args.report_root = ROOT / 'reports' / 'writebench'
    args.state_root = ROOT / '.state' / 'writebench'
    args.dimension = 64
    args.centers = [[i * 20.] + [0.] * 63 for i in range(8)]
    args.keep_running = False
    args.steady_seconds = args.seconds
    args.rounds = 1
    args.latency_only = True
    args.max_seconds = args.seconds + args.drain_seconds + 130
    reason = limit_reason(host_sample(ROOT), limit_owned_disk=BenchGuard.limit_owned_disk)
    if reason:
        raise RuntimeError(reason)
    trial = CapacityTrial(args)
    if args.data_backend:
        trial.c.env['DATA_BACKEND'] = args.data_backend
        if args.data_backend == 'raw':
            trial.c.env['CONSUMER_RETENTION'] = '0'
    print('REPORT', trial.logs, flush=True)
    (trial.state_dir / '.capacity-owned').write_text(trial.run_id)
    trial.drivers = []
    guard = BenchGuard(trial, trial.logs)
    trial.guard = guard
    trial.report.update(kind='open-loop unique vector inserts', qps=args.qps, seconds=args.seconds,
                        write_gateways=args.write_gateways, write_routing=args.write_routing,
                        clients=args.clients, max_inflight_per_client=args.inflight, timeout_seconds=3,
                        service_gomaxprocs=os.environ.get('GOMAXPROCS', 'runtime default'), client_gomaxprocs=2,
                        background_services=args.background_services, target_met=False)
    trial.report.update(data_backend=trial.c.env.get('DATA_BACKEND', 'txn'),
                        consumer_retention=trial.c.env['CONSUMER_RETENTION'] == '1',
                        max_owned_rss_bytes=None, cpu_load_stop_enabled=guard.limit_busy_cpu)
    try:
        if sys.platform == 'darwin':
            trial.report['executables'] = {
                role: subprocess.check_output(['/usr/bin/file', '-b', str(binary)], text=True).strip()
                for role, binary in [('server', trial.c.server_binary), ('client', ROOT / '.tools/writebench')]
            }
        guard.sample()
        guard.thread.start()
        trial.storage.start()
        for slot in (0, 1, 2, 35, 3, 4, 5):
            trial.check_background()
            trial.launch(slot)
        if args.background_services:
            for process in guard.processes():
                subprocess.run(['/usr/sbin/taskpolicy', '-b', '-p', str(process.pid)], check=True)
        trial.wait('publishers', lambda: publishers_ready(trial.state()))
        trial.wait('data sources', lambda: all(d['owner_available'] for d in trial.state()['data_progress'].values()))
        with Client(f'127.0.0.1:{args.base_port + 180}', trial.c.env['API_TOKEN']) as reader, ThreadPoolExecutor(max_workers=1) as sampler:
            trial.wait('all follower read paths', lambda: ready_follower(reader, args.centers))
            endpoints = dict(zip(('gateway-0', 'gateway-1'), trial.urls))
            if args.write_stages:
                before = collect(endpoints, trial.c.env['API_TOKEN'])
            trial.measurements.phase = 'write'
            urls = {name: trial.c.urls[name] for slot, name in trial.slots.items() if slot in (3, 4, 5)}
            before_stop = None

            def start_stage_sample():
                nonlocal before_stop
                before_stop = sampler.submit(worker_stage_sample, urls, trial.c.env['API_TOKEN'])

            load = write_window(args, trial.logs, trial.c.env['API_TOKEN'], trial.drivers, trial.check_background,
                                before_end=start_stage_sample if args.drain_stages else None)
            trial.report['writes'] = load
            trial.report['target_met'] = load['full_window'] and load['durable_acks'] == load['planned'] and load['ack_after_window'] == 0
            trial.persist()
            print('ACKS', load['durable_acks'], 'INVOKED', load['invoked'], flush=True)
            guard.check()
            if args.write_stages:
                after = collect(endpoints, trial.c.env['API_TOKEN'])
                save(trial.logs / 'gateway-stages.json', interval(before, after))
            trial.measurements.phase = 'follower_read'
            stop = threading.Event()
            if args.drain_stages:
                stages = sampler.submit(capture_drain_stages, stop, urls, trial.c.env['API_TOKEN'],
                                        trial.logs, load['last_ack_ns'], before_stop)
            try:
                trial.report['catchup'] = catchup(reader, load, centers=trial.centers, read_state=trial.state,
                    check_background=trial.check_background, logs=trial.logs, drain_seconds=args.drain_seconds)
            finally:
                stop.set()
                if args.drain_stages:
                    stages.result()
            trial.report['status'] = 'measured'
    except BaseException as exc:
        trial.report.update(status='interrupted', error=repr(exc))
        raise
    finally:
        stop_drivers(trial.drivers)
        try:
            guard.close()
            trial.close()
            clean_owned_state(trial.state_dir, args.state_root, trial.run_id, trial.report['all_children_stopped'])
            trial.report['owned_data_removed'] = True
        finally:
            trial.report.update(all_drivers_stopped=all(p.poll() is not None for p in trial.drivers),
                                resources=guard.summary())
            trial.persist()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--qps', type=int, default=40000)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--drain-seconds', type=int, default=150)
    parser.add_argument('--clients', type=int, default=8)
    parser.add_argument('--data-backend', choices=('raw', 'txn'),
                        help='select the data WAL backend; raw disables consumer retention')
    parser.add_argument('--write-gateways', type=int, choices=(1, 2), default=2,
                        help='number of Gateways receiving inserts; both services stay active')
    parser.add_argument('--write-stages', action='store_true',
                        help='scrape Gateway histograms around the write window; delays first follower probe')
    parser.add_argument('--drain-stages', action='store_true',
                        help='sample local Worker stage histograms during catch-up')
    parser.add_argument('--write-routing', choices=('client', 'mixed', 'partition'), default='client',
                        help='assign a Gateway per client, mix each partition across both, or route by partition')
    parser.add_argument('--inflight', type=int, default=128)
    parser.add_argument('--base-port', type=int, default=28400)
    parser.add_argument('--background-services', action='store_true',
                        help='use macOS background priority for the ten owned service processes only')
    args = parser.parse_args()
    if min(args.qps, args.seconds, args.drain_seconds, args.clients, args.inflight) <= 0 or args.qps % args.clients:
        parser.error('positive values and evenly divided rate required')
    if args.write_routing != 'client' and args.write_gateways != 2:
        parser.error('mixed and partition routing require both write Gateways')

    def stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    run(args)
