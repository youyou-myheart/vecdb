"""Gate steady write throughput, durable ACK latency and post-write follower visibility."""
from dataclasses import asdict, dataclass
import json
import os
import signal
import time
import uuid

from cluster_local import LocalCluster, ROOT
from onebox_freshness import hardware, publishers_ready
from onebox_writebench import catchup, ready_follower, stop_drivers, write_window
from quality import save
from vectorstore import Client


@dataclass(frozen=True)
class Profile:
    qps: int = 200
    seconds: int = 30
    clients: int = 4
    inflight: int = 32
    base_port: int = 19500
    write_gateways: int = 2
    write_routing: str = 'client'
    min_window_fraction: float = .95
    max_p99_ms: int = 200
    max_follower_ms: int = 1000


def checks(profile, writes, follower):
    planned = profile.qps * profile.seconds
    qps = writes['ack_qps_in_window']
    p99 = writes['p99_ack_ms_upper']
    confirmed = follower.get('first_confirmed_ms')
    return dict(
        full_window=writes['full_window'] and writes['planned'] == planned,
        every_write_sent=writes['invoked'] == planned and writes['not_sent'] == 0,
        every_write_durable=writes['durable_acks'] == planned and not writes['rpc_failures'],
        window_throughput=qps is not None and qps >= profile.qps * profile.min_window_fraction,
        write_p99=p99 is not None and 0 <= p99 <= profile.max_p99_ms,
        durable_frontier=follower.get('pinned_durable_events') == planned,
        follower_read=follower.get('status') == 'confirmed' and follower.get('probe_errors') == 0,
        follower_latency=confirmed is not None and 0 <= confirmed <= profile.max_follower_ms,
    )


def run():
    profile = Profile()
    run_id = uuid.uuid4().hex
    logs = ROOT / 'reports' / 'performance-smoke' / run_id
    logs.mkdir(parents=True)
    centers = [[i * 20.] + [0.] * 63 for i in range(8)]
    route = logs / 'routing.json'
    save(route, dict(version=1, centers=centers))
    env = dict(os.environ, API_TOKEN=uuid.uuid4().hex, CLUSTER_TOKEN=uuid.uuid4().hex,
               INDEX_REPLICATION='graph', CONSUMER_RETENTION='1', LEASE_TTL='6s', CLUSTER_ROUTING_FILE=str(route))
    cluster = LocalCluster('performance/'+run_id, partitions=16, dimension=64, replicas=2, scheduler='load',
                           base_port=profile.base_port, grpc_port=profile.base_port+180,
                           directory=logs/'services', environment=env)
    drivers = []
    report = dict(status='failed', namespace=cluster.namespace, profile=asdict(profile), host=hardware(),
                  logs=str(logs.relative_to(ROOT)), topology=dict(master=2, gateway=2, worker=3,
                  data_partitions=16, index_partitions=8, replicas=2, dimension=64))

    def healthy():
        for node, process in cluster.children.items():
            if process.poll() is not None:
                raise RuntimeError(f'{node} exited ({process.returncode})')

    try:
        cluster.start(3)
        cluster.launch('gateway', 34, grpc_port=profile.base_port+181)
        cluster.wait_ready()
        with Client(f'127.0.0.1:{profile.base_port+180}', env['API_TOKEN']) as reader:
            deadline = time.monotonic() + 60
            while True:
                healthy()
                state = cluster.status()
                data = state.get('data_progress', {})
                if (len(data) == 16 and all(d['owner_available'] for d in data.values())
                        and publishers_ready(state) and ready_follower(reader, centers)):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('waiting for data owners, graph publishers and follower reads')
                time.sleep(.1)
            writes = write_window(profile, logs, env['API_TOKEN'], drivers, healthy)
            report['writes'] = writes
            follower = catchup(reader, writes, centers=centers, read_state=cluster.status, check_background=healthy,
                               logs=logs, drain_seconds=20)
            report['catchup'] = follower
            report['checks'] = checks(profile, writes, follower)
            healthy()
    except Exception as exc:
        report['error'] = repr(exc)
    finally:
        try:
            stop_drivers(drivers)
        finally:
            cluster.close()
            report['all_children_stopped'] = all(p.poll() is not None for p in [*drivers, *cluster.children.values()])
            if report.get('checks') and all(report['checks'].values()) and report['all_children_stopped'] and 'error' not in report:
                report['status'] = 'passed'
            save(logs/'report.json', report)
            save(ROOT/'reports/performance-smoke.json', report)
    print(json.dumps({k: report[k] for k in ('status', 'logs', 'checks', 'error') if k in report}), flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    def interrupt(signum, frame):
        raise InterruptedError(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupt)
    raise SystemExit(run())
