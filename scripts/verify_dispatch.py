"""Real-process fault matrix: fixed clusters, repeated failures, exact state oracle."""
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import signal
import sys
import time
import urllib.error
import urllib.request
import uuid
from cluster_local import LocalCluster, ROOT
sys.path.insert(0, str(ROOT/'sdk/python'))
from vectorstore import Client, messages


def converged(state, data_count=4, index_count=3):
    """Completion is durable receipt, not proof that every query replica applied."""
    progress = state.get('data_progress', {})
    heads = state.get('graph_heads', {})
    if not state.get('ready') or not state.get('fully_replicated') or len(progress) != data_count or len(heads) != index_count:
        return False
    if any(not d.get('owner_available') or d['floor'] > d['completed_lsn'] or d['completed_lsn'] != d['head'] for d in progress.values()):
        return False
    for index, head in heads.items():
        owners = state.get('replica_owners', {}).get(index, {}).get('owners', [])
        if len(owners) != 2 or head['visible'] != head['committed']:
            return False
        if any(state.get('graph_positions', {}).get(o['partition']) != head['visible'] for o in owners):
            return False
    return True


def main():
    directory = ROOT/'reports/dispatch-processes'/uuid.uuid4().hex
    directory.mkdir(parents=True)
    centers = [[-10, 0], [0, 10], [10, 0]]
    route = directory/'routing.json'
    route.write_text(json.dumps(dict(version=1, centers=centers)))
    env = dict(os.environ, API_TOKEN='dispatch-public-token', CLUSTER_TOKEN='dispatch-private-token',
               INDEX_REPLICATION='graph', CONSUMER_RETENTION='1', LEASE_TTL='6s', CLUSTER_ROUTING_FILE=str(route))
    c = LocalCluster('dispatch/'+uuid.uuid4().hex, partitions=4, dimension=2, replicas=2,
                     base_port=19040, grpc_port=19080, environment=env, directory=directory)
    report = dict(status='running', checks=[], rounds=[], namespace=c.namespace)
    paused, expected, last_completed = set(), {}, {}

    def check(name):
        report['checks'].append(name)
        (ROOT/'reports/dispatch-processes.json').write_text(json.dumps(report, indent=2))
        print(name, flush=True)

    def state():
        s = c.status()
        for id, d in s.get('data_progress', {}).items():
            assert d['floor'] <= d['completed_lsn'] <= d['head'], d
            assert d['completed_lsn'] >= last_completed.get(id, 0), d
            last_completed[id] = d['completed_lsn']
        report['last_status'] = s
        return s

    def wait(name, fn):
        deadline = time.monotonic()+90
        while time.monotonic() < deadline:
            value = fn()
            if value:
                return value
            time.sleep(.2)
        raise TimeoutError(name+': '+json.dumps(state()))

    def caught():
        return converged(state())

    def put(id, vector, request=None):
        token = c.request('/v1/records/'+id, 'PUT', dict(request_id=request or uuid.uuid4().hex, vector=vector))['token']
        expected[id] = dict(vector=vector, revision=token['revision'])
        return token

    def delete(id, client):
        client.delete(id, request_id=uuid.uuid4().hex)
        expected.pop(id, None)

    def metrics(url=None):
        req = urllib.request.Request((url or c.gateway)+'/metrics', headers={'Authorization':'Bearer '+env['API_TOKEN']})
        with urllib.request.urlopen(req, timeout=15) as response:
            text = response.read().decode()
        return text

    def metric_sum(text, name):
        values = [float(line.split()[-1]) for line in text.splitlines() if line.startswith(name+'{') or line.startswith(name+' ')]
        assert values, 'missing metric: '+name
        return sum(values)

    def verify(client):
        wait('all data and query replicas caught up', caught)
        result = client.search([0, 0], k=1000, exact=True, timeout=60)
        assert len(result.hits) == len(expected), (len(result.hits), len(expected))
        seen = set()
        for h in result.hits:
            assert h.id in expected and h.id not in seen, h
            seen.add(h.id)
            record = expected[h.id]
            assert h.revision == record['revision'], h
            assert math.isclose(h.distance, sum(v*v for v in record['vector']), rel_tol=1e-9, abs_tol=1e-6), h
        text = metrics()
        assert metric_sum(text, 'vecdb_data_pending_events') == 0
        assert metric_sum(text, 'vecdb_graph_replica_lag_entries') == 0
        assert metric_sum(text, 'vecdb_data_owner_available') == 4
        assert metric_sum(text, 'vecdb_routing_version') == 1
        # Compare approximate retrieval with exact ground truth on structured data.
        exact = client.search([9.999, .02], k=10, exact=True).hits
        ann = client.search([9.999, .02], k=10).hits
        assert len({h.id for h in ann} & {h.id for h in exact}) >= 9

    def kill(node):
        slot = int(c.urls[node].rsplit(':', 1)[1])-c.base_port
        c.children[node].kill(); c.children[node].wait(timeout=10)
        if node in c.workers:
            c.workers.remove(node)
        return slot

    try:
        c.start(3); c.wait_ready()
        with Client('127.0.0.1:19080', env['API_TOKEN'], timeout=60) as client:
            layout = client.layout()
            assert layout.data_partitions == 4 and len(layout.bindings) == 3 and len(layout.routing.centers) == 3
            for node in c.workers:
                os.kill(c.children[node].pid, signal.SIGSTOP); paused.add(node)
            started = time.monotonic(); put('offline', [-10, 0])
            assert time.monotonic()-started < 3 and client.get('offline').revision == 1
            assert metric_sum(metrics(), 'vecdb_data_pending_events') == 1
            for node in paused:
                os.kill(c.children[node].pid, signal.SIGCONT)
            paused.clear()
            wait('dispatcher resumes from durable data', caught)
            check('business_writes_continue_without_workers_and_backlog_is_observable')
            for i in range(90):
                center = centers[i%3]
                put(str(i), [center[0]+(i//3)*.001, center[1]])
            verify(client)
            check('four_data_partitions_three_clustered_indexes_exact_and_ANN_oracles')
            for center in centers:
                put('2', center)
            delete('3', client)
            verify(client)
            check('cross_cluster_updates_and_broadcast_delete')
            lease = client.consumer('data-0000', consumer='sink', action='acquire', session=uuid.uuid4().hex, ttl_seconds=300)
            manifest = client.checkpoint(timeout=60)
            assert manifest.partitions['data-0000'].format == 'data-records-v1'
            client.prune(timeout=60)
            assert client.consumer('data-0000').floor == 0
            page = client.changes('data-0001')
            assert page.reset_required and page.checkpoint.format == 'data-records-v1'
            for i in range(3):
                assert client.changes(f'index-{i:04d}', stream='graph').reset_required
            check('independent_subscriptions_consumer_pin_snapshot_GC_reset')

            # A paused old owner resumes after replacement: its former epoch must
            # never overwrite newer graph or source progress.
            before = state()
            old = before['data_progress']['data-0000']['owner']
            victim = old['node']
            os.kill(c.children[victim].pid, signal.SIGSTOP); paused.add(victim)
            for i in range(12):
                put('stale-'+str(i), [10, i*.001])
            wait('expired source lease replaced', lambda: state()['data_progress']['data-0000']['owner']['epoch'] > old['epoch'])
            os.kill(c.children[victim].pid, signal.SIGCONT); paused.remove(victim)
            verify(client)
            check('SIGSTOP_lease_expiry_takeover_and_stale_owner_resume')

            for round in range(3):
                c.wait_ready(); verify(client)
                client.checkpoint(timeout=60); client.prune(timeout=60)
                before = state(); old_master = before['leader']
                victims = [before['data_progress']['data-0000']['owner']['node']]
                if round == 1:
                    victims.append(next(n for n in c.workers if n not in victims))
                slots = [kill(n) for n in victims]
                master_slot = kill(old_master['node'])
                # A deterministic burst creates backlog during overlapping control
                # and worker failures. Concurrent client requests exercise receipts.
                def write_new(i):
                    id = f'round-{round}-{i}'
                    center = centers[i%3]
                    return id, [center[0]+i*.001, center[1]], c.request('/v1/records/'+id, 'PUT',
                        dict(request_id='new-'+id, vector=[center[0]+i*.001, center[1]]))['token']
                with ThreadPoolExecutor(max_workers=4) as pool:
                    for id, vector, token in pool.map(write_new, range(48)):
                        expected[id] = dict(vector=vector, revision=token['revision'])
                for center in centers:
                    put('2', center)
                # Competing conditional updates to the same vector: exactly one
                # winner, one conflict, with no duplicate revision allocation.
                revision = expected['2']['revision']
                def compete(i):
                    try:
                        return c.request('/v1/records/2', 'PUT', dict(request_id=f'cas-{round}-{i}',
                            expected_revision=revision, vector=centers[i]))['token']['revision']
                    except urllib.error.HTTPError as exc:
                        if exc.code == 409:
                            return 'conflict'
                        raise
                with ThreadPoolExecutor(max_workers=2) as pool:
                    outcomes = list(pool.map(compete, range(2)))
                assert outcomes.count('conflict') == 1 and outcomes.count(revision+1) == 1, outcomes
                current = client.get('2')
                expected['2'] = dict(vector=list(current.vector), revision=current.revision)
                delete(f'round-{round}-1', client)
                put(f'round-{round}-1', [-10, .1])
                # Repeat an already successful request after failures: revision
                # and data must not change, even if delivery is repeated.
                id = f'round-{round}-0'
                retry = c.request('/v1/records/'+id, 'PUT', dict(request_id='new-'+id, vector=centers[0]))
                assert retry['token']['revision'] == 1 and client.get(id).revision == 1
                c.launch('master', master_slot)
                for slot in slots:
                    c.workers.append(c.launch('worker', slot))
                c.wait_ready(); verify(client)
                after = state()
                assert after['leader']['epoch'] > old_master['epoch']
                for id, d in before['data_progress'].items():
                    if d['owner']['node'] in victims:
                        assert after['data_progress'][id]['owner']['epoch'] > d['owner']['epoch']
                report['rounds'].append(dict(round=round+1, killed_workers=victims,
                    killed_master=old_master['node'], records=len(expected),
                    master_epoch=after['leader']['epoch'], data_progress=after['data_progress'],
                    graph_heads=after['graph_heads'], graph_positions=after['graph_positions']))
            check('three_rounds_overlapping_master_worker_SIGKILL_two_workers_GC_and_concurrent_writes')
            for node in c.workers:
                text = metrics(c.urls[node])
                assert metric_sum(text, 'vecdb_dispatch_ticks_total') >= 0
                assert 'vecdb_index_admissions_total' in text
                if c.request('/v1/cluster', url=c.urls[node])['local_partitions']:
                    assert 'vecdb_index_queue_bytes' in text
            check('worker_counters_queues_and_gateway_durable_gauges')
            report['live_records'] = len(expected)
        report['status'] = 'passed'
    except BaseException as exc:
        report.update(status='failed', error=repr(exc)); raise
    finally:
        for node in paused:
            if c.children[node].poll() is None:
                os.kill(c.children[node].pid, signal.SIGCONT)
        c.close()
        report['all_children_stopped'] = all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/dispatch-processes.json').write_text(json.dumps(report, indent=2))
    print(json.dumps({k:v for k,v in report.items() if k not in ('last_status', 'rounds')}, indent=2))


if __name__ == '__main__':
    main()
