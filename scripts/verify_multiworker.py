"""Exercise dynamic Worker membership through the reusable local launcher."""
import hashlib
import json
import math
import os
from pathlib import Path
import time
import urllib.error
import uuid

from cluster_local import LocalCluster, ROOT


def main():
    path = ROOT / 'reports/multiworker-http.json'
    path.parent.mkdir(exist_ok=True)
    report = {'status': 'running', 'scope': 'same-host independent processes with real TiKV and SeaweedFS'}
    checks, timings = [], []
    cluster = LocalCluster('multiworker-test/' + uuid.uuid4().hex, partitions=8,
        base_port=18240, directory=ROOT / 'reports/multiworker-processes',
        environment=dict(os.environ, API_TOKEN='multiworker-public-token',
                         CLUSTER_TOKEN='multiworker-internal-token', LEASE_TTL='6s'))
    cluster.prefix = 'acceptance1'  # Deterministic rendezvous distribution; namespace is isolated.
    rows = {}
    def verify():
        # Force synchronization on every partition before comparing the complete truth set.
        cluster.request('/v1/checkpoint', 'POST', {})
        for group in sorted({r['payload']['group'] for r in rows.values()}):
            got = cluster.request('/v1/search', 'POST', {'vector': [0.] * 16,
                'k': 1000, 'exact': True, 'filter': {'group': group}})['hits']
            expected = sorted((key for key in rows if rows[key]['payload']['group'] == group),
                              key=lambda key: (sum(x*x for x in rows[key]['vector']), key))
            assert [h['id'] for h in got] == expected
            for hit in got:
                assert hit['revision'] == rows[hit['id']]['revision']
                assert hit['payload'] == rows[hit['id']]['payload']
        # Each shard exceeds the default exact fallback threshold (128).
        sizes = [0] * cluster.partitions
        for key in rows:
            sizes[int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % cluster.partitions] += 1
        assert min(sizes) > 128, sizes
        # Also exercise the actual distributed ANN path, not just exact fallback.
        query = rows['0']['vector']
        hits = cluster.request('/v1/search', 'POST', {'vector': query, 'k': 10})['hits']
        assert hits and hits[0]['id'] == '0'
        assert len({h['id'] for h in hits}) == len(hits) == 10
    def settle(label):
        began = time.monotonic()
        state = cluster.wait_ready()
        timings.append({'event': label, 'seconds': time.monotonic() - began})
        # Independently calculate master rendezvous targets for every fixed partition.
        for index, owner in state['owners'].items():
            wanted = max(cluster.workers, key=lambda node: hashlib.sha256((index+'\0'+node).encode()).digest())
            assert owner['node'] == wanted
        return state
    try:
        cluster.start(2)
        initial = settle('initial_two_workers')
        for j in range(1600):
            key = str(j)
            vector = [math.sin((j % 8 + 1) * (k+1) * .17) + .01*math.cos((j+1)*(k+1)) for k in range(16)]
            rows[key] = {'vector': vector, 'payload': {'group': str(j % 8)}, 'revision': 1}
            cluster.request('/v1/records/' + key, 'PUT', dict(request_id='create-'+key, vector=vector, payload=rows[key]['payload']))
        verify()
        cluster.request('/v1/log/prune', 'POST', {})
        checks.append('two_worker_distributed_exact_and_hnsw_snapshot_prune')
        cluster.scale(3)
        expanded = settle('expand_two_to_three')
        assert any(o['node'] == cluster.workers[-1] for o in expanded['owners'].values())
        for index, owner in initial['owners'].items():
            if owner['node'] == expanded['owners'][index]['node']:
                assert owner['epoch'] == expanded['owners'][index]['epoch']
        verify()
        checks.append('new_worker_receives_partitions_unchanged_owners_keep_epoch')
        # Tail changes after the last snapshot must survive removing two workers.
        rows['0'] = {'vector': [0.] * 16, 'payload': {'group': 'updated'}, 'revision': 2}
        cluster.request('/v1/records/0', 'PUT', dict(request_id='tail-update', vector=rows['0']['vector'], payload=rows['0']['payload']))
        cluster.request('/v1/records/1', 'DELETE', {'request_id': 'tail-delete'})
        del rows['1']
        cluster.scale(1)
        settle('shrink_three_to_one')
        verify()
        checks.append('scale_in_restores_snapshots_updates_deletes_without_duplicates')
        cluster.scale(0)
        deadline = time.monotonic() + 20
        while cluster.status()['ready']:
            assert time.monotonic() < deadline
            time.sleep(.2)
        try:
            cluster.request('/v1/search', 'POST', {'vector': [0.] * 16, 'k': 10})
            raise AssertionError('missing partitions returned success')
        except urllib.error.HTTPError as exc:
            assert exc.code == 503, exc.code
        # Durable writes remain possible while no index Worker is available.
        rows['offline'] = {'vector': [2.] * 16, 'payload': {'group': 'offline'}, 'revision': 1}
        cluster.request('/v1/records/offline', 'PUT', dict(request_id='offline-write', vector=rows['offline']['vector'], payload=rows['offline']['payload']))
        checks.append('zero_workers_query_fails_closed_durable_write_succeeds')
        cluster.scale(2)
        final = settle('recover_zero_to_two')
        verify()
        checks.append('all_worker_processes_replaced_data_recovers')
        report.update(status='passed', namespace=cluster.namespace, checks=checks,
                      timings=timings, live_records=len(rows), final_owners=final['owners'])
    except BaseException as exc:
        report.update(status='failed', error=repr(exc), checks=checks)
        raise
    finally:
        cluster.close()
        report['all_children_stopped'] = all(p.poll() is not None for p in cluster.children.values())
        path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
