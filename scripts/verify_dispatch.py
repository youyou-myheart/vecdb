"""Real-process fault matrix: fixed clusters, repeated failures, exact state oracle."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import grpc
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
from vectorstore.v1.vector_pb2_grpc import VectorStoreStub


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
        publisher = state.get('replica_plans', {}).get(index, {}).get('publisher')
        owner = next((o for o in owners if o['partition'] == publisher), None)
        leader = head.get('leader', {})
        if not owner or any(leader.get(k) != owner.get(k) for k in ('partition', 'node', 'session', 'epoch')):
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
    last_source_epochs = {}
    report['observed_source_epoch_advances'] = 0

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
        for index, head in s.get('graph_heads', {}).items():
            for source, epoch in head.get('source_epochs', {}).items():
                key = (index, source)
                previous = last_source_epochs.get(key, 0)
                assert epoch >= previous, (key, previous, epoch)
                if previous and epoch > previous:
                    report['observed_source_epoch_advances'] += 1
                last_source_epochs[key] = epoch
            for source, epoch in head.get('checkpoint_source_epochs', {}).items():
                assert epoch <= head.get('source_epochs', {}).get(source, 0), head
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

    def retry_put(id, vector, key):
        deadline = time.monotonic() + 60
        while True:
            try:
                return c.request('/v1/records/'+id, 'PUT', dict(request_id=key, vector=vector))['token']
            except urllib.error.HTTPError as exc:
                if exc.code != 503 or time.monotonic() >= deadline:
                    raise
                time.sleep(.1)

    def put(id, vector, request=None):
        token = retry_put(id, vector, request or uuid.uuid4().hex)
        expected[id] = dict(vector=vector, revision=token['revision'])
        return token

    def delete(id, client):
        request_id = uuid.uuid4().hex
        try:
            client.delete(id, request_id=request_id)
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAVAILABLE:
                raise
            client.delete(id, request_id=request_id)
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
        assert metric_sum(text, 'vecdb_index_source_epoch') > 0
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
            client.put('offline', [-10, 0], request_id='before-stop')
            expected['offline'] = dict(vector=[-10, 0], revision=1)
            for node in c.workers:
                os.kill(c.children[node].pid, signal.SIGSTOP); paused.add(node)
            try:
                client.put('offline', [-10, 1], request_id='during-stop', timeout=.4)
                raise AssertionError('write ACK with every data owner paused')
            except grpc.RpcError as exc:
                assert exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED), exc
            assert client.get('offline').revision == 1
            for node in paused:
                os.kill(c.children[node].pid, signal.SIGCONT)
            paused.clear()
            put('offline', [-10, 1], request='during-stop')
            wait('dispatcher resumes from durable data', caught)
            check('owner_unavailable_no_write_ack_raw_read_available_retry_once')
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
            stale_id = next(id for id in sorted(expected) if int.from_bytes(hashlib.sha256(id.encode()).digest()[:8], 'big') % 4 == 0)
            route = client.stub.ResolveWriteRoute(messages.GetRequest(id=stale_id), metadata=client.metadata, timeout=3)
            assert route.endpoint and route.partition == 0 and route.epoch == old['epoch']
            victim = old['node']
            os.kill(c.children[victim].pid, signal.SIGSTOP); paused.add(victim)
            wait('expired source lease replaced', lambda: state()['data_progress']['data-0000']['owner']['epoch'] > old['epoch'])
            for i in range(12):
                put('stale-'+str(i), [10, i*.001])
            os.kill(c.children[victim].pid, signal.SIGCONT); paused.remove(victim)
            with grpc.insecure_channel(route.endpoint) as channel:
                try:
                    VectorStoreStub(channel).Put(messages.PutRequest(id=stale_id, vector=[0, 0],
                        request_id='old-owner-write', route=route), metadata=client.metadata, timeout=5)
                    raise AssertionError('stale writer acknowledged')
                except grpc.RpcError as exc:
                    assert exc.code() == grpc.StatusCode.FAILED_PRECONDITION, exc
            assert client.get(stale_id).revision == expected[stale_id]['revision']
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
                    return id, [center[0]+i*.001, center[1]], retry_put(id, [center[0]+i*.001, center[1]], 'new-'+id)
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
            assert report['observed_source_epoch_advances'] > 0
            check('durable_Index_source_epochs_monotonic_across_repeated_failover_and_GC')
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
