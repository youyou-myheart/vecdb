"""Measure fixed-rate Go SDK writes and check visibility on every configured graph replica."""
from collections import Counter
from contextlib import nullcontext
import json
import math
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import uuid

ROOT = Path(__file__).resolve().parents[2]


def all_caught(state, expected, nindex, replicas):
    data = state.get('data_progress', {})
    heads = state.get('graph_heads', {})
    if not (state.get('ready') and state.get('fully_replicated') and set(data) == set(expected)
            and len(heads) == nindex):
        return False
    for key, floor in expected.items():
        p = data[key]
        if not p.get('owner_available') or p['completed_lsn'] != p['head'] or p['head'] < floor:
            return False
    for idx, h in heads.items():
        owners = state.get('replica_owners', {}).get(idx, {}).get('owners', [])
        publisher = state.get('replica_plans', {}).get(idx, {}).get('publisher')
        if len(owners) != replicas or h['visible'] != h['committed']:
            return False
        positions = state.get('graph_positions', {})
        leader = h.get('leader', {})
        if not any(o['partition'] == publisher and all(o.get(k) == leader.get(k)
                   for k in ('node', 'epoch', 'session', 'partition')) for o in owners):
            return False
        if any(positions.get(o['partition']) != h['committed'] for o in owners):
            return False
    return True


def wait_caught(c, expected, nindex, timeout):
    started = time.monotonic()
    observations = 0
    decode_failures = 0
    state = {}
    last_error = None
    while True:
        try:
            state = c.status()
            observations += 1
            if all_caught(state, expected, nindex, c.replicas):
                return state, True, observations, last_error
        except (URLError, TimeoutError) as exc:
            last_error = repr(exc)
            if isinstance(exc, HTTPError):
                with exc:
                    message = exc.read(1000).decode(errors='replace')
                last_error += ' ' + message
                if 'invalid marker byte' in message:
                    decode_failures += 1
                    if decode_failures >= 5 and time.monotonic() - started >= 20:
                        return state, False, observations, last_error
        if time.monotonic() - started >= timeout:
            return state, False, observations, last_error
        time.sleep(.25)


def execute_window(binary, endpoint, token, path, centers, partitions, key_prefix,
                   rate, seconds, clients, inflight, on_start, on_end):
    path.mkdir(parents=True)
    center_file = path / 'centers.json'
    center_file.write_text(json.dumps(centers))
    barrier = path / 'start-ns'
    children, outputs = [], []
    measuring = False
    client_env = {**os.environ, 'API_TOKEN': token, 'GOMAXPROCS': '2'}
    try:
        for i in range(clients):
            output = path / f'client-{i}.json'
            outputs.append(output)
            client_rate = rate // clients + int(i < rate % clients)
            argv = [
                str(binary), '-endpoint', endpoint, '-centers', str(center_file),
                '-shards', str(partitions), '-key-prefix', key_prefix, '-client', str(i),
                '-rate', str(client_rate), '-seconds', str(seconds), '-inflight', str(inflight),
                '-output', str(output), '-barrier', str(barrier),
            ]
            with output.with_suffix('.log').open('w') as log:
                children.append(subprocess.Popen(argv, cwd=ROOT, env=client_env,
                                                 stdout=log, stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 15
        while not all(Path(str(output) + '.ready').exists() for output in outputs):
            if any(child.poll() is not None for child in children) or time.monotonic() >= deadline:
                logs = [output.with_suffix('.log').read_text() for output in outputs]
                raise RuntimeError(f'Go driver readiness failed: {logs}')
            time.sleep(.02)
        if on_start:
            on_start(rate)
        measuring = True
        start_ns = time.time_ns() + 200_000_000
        pending_barrier = path / 'barrier.tmp'
        pending_barrier.write_text(str(start_ns))
        pending_barrier.replace(barrier)
        deadline = time.monotonic() + seconds + 15
        for child, output in zip(children, outputs):
            if child.wait(timeout=max(.01, deadline - time.monotonic())):
                raise RuntimeError('Go driver failed: ' + output.with_suffix('.log').read_text())
        return [json.loads(output.read_text()) for output in outputs], start_ns
    finally:
        for p in children:
            if p.poll() is None:
                p.terminate()
        for p in children:
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        if measuring and on_end:
            on_end(rate)


def summarize(reports, seconds, owners, gateway, start_ns):
    target = sum(x['Target'] for x in reports)
    sent = sum(x['Started'] for x in reports)
    ack = sum(x['Ack'] for x in reports)
    failed = Counter()
    peers = Counter()
    examples = {}
    latencies = []
    timelines = [dict(started=0, acked=0, failed=0) for _ in range(seconds + 5)]
    for x in reports:
        failed.update(x['Errors'])
        peers.update(x['RPCDestinations'])
        examples.update(x['ErrorExample'])
        latencies.extend(x['AckLatenciesUS'] or [])
        for out, row in zip(timelines, x['BySecond']):
            for k, v in [('started', 'Started'), ('acked', 'Ack'), ('failed', 'Failed')]:
                out[k] += row[v]
    latencies.sort()

    def latency(q):
        return latencies[math.ceil(q * len(latencies)) - 1] / 1000 if latencies else None

    drop = target - sent
    cap = sum(x['DroppedInflight'] for x in reports)
    if drop != sum(x['Unsent'] for x in reports) or not 0 <= cap <= drop:
        raise ValueError('driver arrival totals do not reconcile')
    if not (sent == sum(peers.values()) == ack + sum(failed.values()) and len(latencies) == ack):
        raise ValueError('driver RPC totals or latency samples do not reconcile')
    by_shard = [sum(x['AckShard'][i] for x in reports) for i in range(len(reports[0]['AckShard']))]
    if sum(by_shard) != ack or sum(t['acked'] for t in timelines) != ack:
        raise ValueError('driver shard totals or ACK timeline do not reconcile')
    if any(x['Interrupted'] for x in reports):
        raise RuntimeError('write collection was interrupted before its window ended')
    in_window = sum(x['acked'] for x in timelines[:seconds])
    last_ack_ns = max(x['LastAckNS'] for x in reports)
    tail_ms = max(0, (last_ack_ns - start_ns) / 1e6 - seconds * 1000) if ack else None
    return dict(
        target_qps=target / seconds, seconds=seconds, offered=target, invoked=sent,
        durable_acks=ack, durable_acks_in_window=in_window, after_window_acks=ack - in_window,
        invoked_qps=sent / seconds, ack_qps_in_window=in_window / seconds,
        after_window_ack_tail_ms=tail_ms, rejected_resource_exhausted=failed['ResourceExhausted'],
        rpc_failure_count=sum(failed.values()), rpc_failures=dict(failed), examples=examples,
        dropped_unsent=drop, dropped_inflight_cap=cap, dropped_scheduler_window=drop - cap,
        p50_ack_ms=latency(.5), p95_ack_ms=latency(.95), p99_ack_ms=latency(.99), max_ack_ms=latency(1),
        last_ack_ns=last_ack_ns, durable_acks_per_shard=by_shard,
        peak_inflight_per_client=[x['PeakInflight'] for x in reports],
        last_rpc_peer_counts=dict(peers), observed_owner_peer_calls=sum(peers[p] for p in owners),
        gateway_peer_calls=peers[gateway],
        other_or_unknown_peer_calls=sum(n for p, n in peers.items() if p not in owners and p != gateway),
        client_timeline=timelines,
    )


def run_sdk_writes(c, centers, after_phase=None, *, rates=(500, 1500, 4000, 8000), seconds=5,
                   clients=4, inflight=32, drain_seconds=240, key_prefix=None, artifact_dir=None, sustained=(1000, 60),
                   driver=None, on_start=None, on_end=None):
    """Return one JSON-ready dict per level. after_phase(row) is called after drain.
    on_start(rate) runs as clients reach the barrier, on_end(rate) after issued RPCs return.
    driver defaults to .tools/writebench, built from ./scripts/writebench.
    Use the same key_prefix for independent empty namespaces to match key-shard assignment.
    When recording raw artifacts, provide a directory with fresh level-* subdirectories.
    """
    binary = Path(driver).resolve() if driver else ROOT / '.tools' / 'writebench'
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise FileNotFoundError(f'{binary}: build ./scripts/writebench or pass driver= with an executable')
    levels = [(rate, seconds) for rate in rates]
    if sustained:
        levels.append(sustained)
    if c.grpc_port is None or not levels or min(clients, inflight, drain_seconds) <= 0:
        raise ValueError('need positive load parameters, at least one rate, and a gateway gRPC endpoint')
    if any(rate < clients or duration <= 0 for rate, duration in levels):
        raise ValueError('each rate must offer at least one arrival per client per second')
    if not centers or not all(len(row) == int(c.env['DIMENSION']) for row in centers):
        raise ValueError('centers do not match collection dimension')
    gateway = f'127.0.0.1:{c.grpc_port}'
    key_prefix = key_prefix or uuid.uuid4().hex[:12]
    minimum = {f'data-{i:04d}': 0 for i in range(c.partitions)}
    rows = []
    workspace = nullcontext(artifact_dir) if artifact_dir is not None else TemporaryDirectory(prefix='vecdb-go-writes-')
    with workspace as tmp:
        output = Path(tmp).resolve()
        for ordinal, (rate, duration) in enumerate(levels):
            state, ok, _, err = wait_caught(c, minimum, len(centers), drain_seconds)
            if not ok:
                raise TimeoutError(f'Index replicas failed to settle before rate={rate}, last error={err}')
            before = {k: v['head'] for k, v in state['data_progress'].items()}
            owners = {d['owner'].get('write_endpoint') for d in state['data_progress'].values()}
            owners.discard(None)
            owners.discard('')
            path = output / f'level-{ordinal}-{rate}'
            reports, start_ns = execute_window(
                binary, gateway, c.env['API_TOKEN'], path, centers, c.partitions,
                f'{key_prefix}-{ordinal}-{rate}', rate, duration, clients, inflight, on_start, on_end,
            )
            row = summarize(reports, duration, owners, gateway, start_ns)
            minimum = {key: before[key] + row['durable_acks_per_shard'][shard]
                       for shard, key in enumerate(sorted(before))}
            end, ok, samples, error = wait_caught(c, minimum, len(centers), drain_seconds)
            caught_at_ns = time.time_ns()
            progress = end.get('data_progress', {})
            heads = end.get('graph_heads', {})
            confirmed_ms = None
            if ok and row['last_ack_ns']:
                confirmed_ms = (caught_at_ns - row['last_ack_ns']) / 1e6
            row.update(
                clients=clients, inflight_per_client=inflight, timeout_seconds=3, rate=rate,
                client='Go SDK Put expected_revision=0, onebox localhost, 1ms scheduler ticks',
                latency='nearest rank from SDK Put invocation to durable ACK (successful calls only)',
                owner_write_endpoints=sorted(owners), validation_reads=samples,
                expected_data_heads=minimum, observed_data_heads={k: v['head'] for k, v in progress.items()},
                graph_committed={k: v.get('committed') for k, v in heads.items()},
                graph_visible={k: v.get('visible') for k, v in heads.items()},
                graph_replica_positions=end.get('graph_positions', {}),
                data_completed={k: v.get('completed_lsn') for k, v in progress.items()},
                all_replicas_caught=ok, last_validation_error=error, confirmed_after_last_ack_ms_upper=confirmed_ms,
            )
            if not ok:
                try:
                    pd_endpoint = c.env['PD_ENDPOINTS'].split(',')[0]
                    with urlopen(f'http://{pd_endpoint}/pd/api/v1/regions', timeout=2) as response:
                        regions = json.load(response)['regions']
                    fields = ('id', 'start_key', 'end_key', 'approximate_size')
                    row['region_boundaries'] = [{key: r.get(key) for key in fields} for r in regions]
                    with (c.directory.parent / 'tikv.log').open(errors='replace') as log:
                        row['hot_splits'] = [line.strip() for line in log if 'load base split region' in line]
                except (OSError, ValueError, KeyError) as exc:
                    row['diagnostic_error'] = str(exc)
            rows.append(row)
            (path / 'summary.json').write_text(json.dumps(row, indent=2))
            if after_phase:
                after_phase(row)
            if not ok:
                break
    return rows
