"""Real-process acceptance for automatic checkpoints and reference-safe GC."""
import argparse
import hashlib
import json
import os
import time
import uuid
from cluster_local import LocalCluster, ROOT
import sys
sys.path.insert(0, str(ROOT/'sdk/python'))
from vectorstore import Client, messages
from verify_dispatch import converged


def run(mode):
    directory = ROOT/'reports/reference-checkpoint'/uuid.uuid4().hex
    directory.mkdir(parents=True)
    route = directory/'routing.json'
    route.write_text(json.dumps(dict(version=1, centers=[[-10, 0], [0, 10], [10, 0]])))
    settings = dict(GRAPH_CHECKPOINT_BYTES='1', GRAPH_CHECKPOINT_AGE='3m') if mode == 'bytes' else dict(GRAPH_CHECKPOINT_BYTES='30000000', GRAPH_CHECKPOINT_AGE='3s')
    env = dict(os.environ, API_TOKEN='reference-public-token', CLUSTER_TOKEN='reference-private-token',
               INDEX_REPLICATION='graph', CONSUMER_RETENTION='1', LEASE_TTL='6s',
               CLUSTER_ROUTING_FILE=str(route), **settings)
    c = LocalCluster('reference/'+uuid.uuid4().hex, partitions=4, dimension=2, replicas=2,
                     base_port=19240, grpc_port=19280, environment=env, directory=directory)
    report = dict(mode=mode, status='running', checks=[], settings=settings, namespace=c.namespace)
    expected = {}

    def wait(name, fn):
        deadline = time.monotonic()+60
        while time.monotonic() < deadline:
            value = fn()
            if value:
                report['checks'].append(name)
                return value
            time.sleep(.15)
        raise TimeoutError(name+': '+json.dumps(c.status()))

    def full_checkpoint():
        s = c.status()
        hs = s['graph_heads']
        return len(hs) == 3 and all(h['committed']['sequence'] > 0 and h['checkpoint_position'] == h['committed']
            and h.get('checkpoint_bytes', 0) == h.get('log_bytes', 0) and h.get('checkpoint_sources') == h.get('sources')
            and h.get('checkpoint_source_epochs') == h.get('source_epochs') for h in hs.values())

    def fully_pruned():
        s = c.status()
        return full_checkpoint() and all(h['floor'] == h['checkpoint_position'] for h in s['graph_heads'].values()) and all(
            d['head'] > 0 and d['head'] == d['completed_lsn'] == d['floor'] for d in s['data_progress'].values())

    def write(client, start, end):
        for n in range(start, end):
            center = [[-10, 0], [0, 10], [10, 0]][n % 3]
            v = [center[0]+(n % 11)/100, center[1]+(n % 7)/100]
            client.put(str(n), v, request_id=f'initial-{n}', payload={'cluster': str(n % 3)})
            expected[str(n)] = v

    try:
        c.start(3)
        c.launch('gateway', 34, grpc_port=19281)
        c.wait_ready()
        with Client('127.0.0.1:19280', env['API_TOKEN']) as client:
            session = uuid.uuid4().hex
            leases = {}
            for n in range(3):
                index = f'index-{n:04}'
                leases[index] = client.consumer(index, stream='graph', action='acquire', consumer='reference-test',
                    session=session, ack=0, ttl_seconds=120).leases['reference-test'].epoch
            write(client, 0, 120)
            client.search([0, 0], k=1, min_visible=messages.Token(id=str(max(map(int, expected))), revision=1), timeout=60)
            wait('automatic_checkpoint_covers_exact_sources_and_bytes', full_checkpoint)
            s = c.status()
            assert all(h['floor']['sequence'] == 0 for h in s['graph_heads'].values())
            assert all(d['floor'] == 0 for d in s['data_progress'].values())
            for index in leases:
                page = client.changes(index, stream='graph', limit=256)
                assert page.graph and page.graph[0].kind == 'source_epoch'
                for entry in page.graph:
                    if entry.kind == 'source_epoch':
                        assert entry.source_epochs and not entry.payload_data and not entry.object.key
                        assert entry.position.data_offset == entry.previous.data_offset
                    else:
                        assert entry.kind == 'delta'
                first = next(entry for entry in page.graph if entry.kind == 'delta')
                assert first.payload_data and not first.object.key
                assert hashlib.sha256(first.payload_data).hexdigest() == first.payload.sha256
                assert json.loads(first.payload_data)['format'] == 'hnsw-data-reference-v1'
            report['checks'].append('graph_lease_keeps_epoch_controls_TiKV_payloads_and_source_events_after_checkpoint')
            for index, epoch in leases.items():
                client.consumer(index, stream='graph', action='release', consumer='reference-test', session=session, epoch=epoch)
            wait('automatic_graph_then_data_GC_without_manual_checkpoint_or_prune', fully_pruned)
            before = {i: h['checkpoint']['generation'] for i, h in c.status()['graph_heads'].items()}
            time.sleep(4)
            after = {i: h['checkpoint']['generation'] for i, h in c.status()['graph_heads'].items()}
            assert before == after
            report['checks'].append('idle_graph_does_not_repeat_checkpoints')
            # Kill all workers after both streams have been compacted. TiKV
            # point reads/writes remain independent of worker availability.
            for node in list(c.workers):
                c.children[node].kill()
                c.children[node].wait(timeout=10)
            c.workers.clear()
            assert client.get('0').id == '0'
            write(client, 120, 135)
            c.scale(3)
            c.wait_ready()
            client.search([0, 0], k=1, min_visible=messages.Token(id=str(max(map(int, expected))), revision=1), timeout=60)
            wait('restored_query_replicas_reach_the_durable_graph_tail', lambda: converged(c.status(), 4, 3))
            result = client.search([0, 0], k=1000, exact=True, timeout=60)
            assert {h.id for h in result.hits} == set(expected), sorted(set(expected)-{h.id for h in result.hits})
            with Client('127.0.0.1:19281', env['API_TOKEN']) as second:
                assert {h.id for h in second.search([0, 0], k=1000, exact=True, timeout=60).hits} == set(expected)
            for hit in result.hits:
                assert dict(hit.payload)['cluster'] == str(int(hit.id) % 3)
            wait('all_workers_restore_self_contained_snapshots_and_replay_tail', full_checkpoint)
            report['last_status'] = c.status()
        report['status'] = 'passed'
    except BaseException as exc:
        report.update(status='failed', error=repr(exc))
        raise
    finally:
        c.close()
        report['all_children_stopped'] = all(p.poll() is not None for p in c.children.values())
        (directory/'report.json').write_text(json.dumps(report, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('bytes', 'age', 'both'), default='both')
    args = parser.parse_args()
    report = dict(status='running', runs=[])
    try:
        for mode in (['bytes', 'age'] if args.mode == 'both' else [args.mode]):
            report['runs'].append(run(mode))
        report.update(status='passed', all_children_stopped=all(r['all_children_stopped'] for r in report['runs']))
    except BaseException as exc:
        report.update(status='failed', error=repr(exc))
        raise
    finally:
        (ROOT/'reports/reference-checkpoint.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
