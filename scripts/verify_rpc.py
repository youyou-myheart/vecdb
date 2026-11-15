"""Cross-language gRPC acceptance against real TiKV/SeaweedFS and multiple Workers."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'sdk/python'))
import grpc
from vectorstore import Client, messages
from cluster_local import LocalCluster


def main():
    report = {'status': 'running', 'checks': []}
    namespace = 'rpc-test/' + uuid.uuid4().hex
    env = dict(os.environ, API_TOKEN='rpc-acceptance-public', CLUSTER_TOKEN='rpc-acceptance-internal', LEASE_TTL='6s')
    cluster = LocalCluster(namespace, partitions=4, dimension=3, base_port=18400,
                           grpc_port=18500, environment=env, directory=ROOT / 'reports/rpc-processes')
    def expect(code, action):
        try:
            action()
        except grpc.RpcError as exc:
            assert exc.code() == code, (exc.code(), exc.details())
        else:
            raise AssertionError('expected ' + str(code))
    try:
        cluster.start(2)
        cluster.wait_ready()
        with Client('127.0.0.1:18500', env['API_TOKEN']) as client:
            assert client.health().namespace == namespace
            assert client.cluster().ready and client.layout().data_partitions == 4
            with Client('127.0.0.1:18500', 'wrong') as bad:
                expect(grpc.StatusCode.UNAUTHENTICATED, bad.health)
            expect(grpc.StatusCode.INVALID_ARGUMENT, lambda: client.insert('bad', [1], request_id='bad-dim'))
            expect(grpc.StatusCode.NOT_FOUND, lambda: client.get('absent'))
            report['checks'].append('grpc_auth_status_layout_validation_and_not_found')
            first = client.insert('python', [1,1,1], payload={'lang':'python'}, request_id='py-insert')
            assert first.durable and first.token.revision == 1
            assert client.insert('python', [1,1,1], payload={'lang':'python'}, request_id='py-insert') == first
            expect(grpc.StatusCode.ABORTED, lambda: client.insert('python', [1,1,1], request_id='duplicate'))
            updated = client.update('python', [0,0,0], revision=1, payload={'lang':'python'}, request_id='py-update')
            assert updated.token.revision == 2
            expect(grpc.StatusCode.ABORTED, lambda: client.update('python', [1,1,1], revision=1, request_id='stale'))
            assert client.search([0,0,0], k=1, min_visible=updated.token).hits[0].id == 'python'
            report['checks'].append('python_sdk_idempotency_CAS_and_visibility')
            run = subprocess.run(['bash','scripts/go.sh','test','-count=1','-run','TestSDKRealCluster','-v','./sdk/go'],
                cwd=ROOT, env=dict(env, RPC_ACCEPTANCE='1',RPC_TARGET='127.0.0.1:18500'),
                text=True, capture_output=True, timeout=90)
            (ROOT/'reports/go-sdk-integration.log').write_text(run.stdout + run.stderr)
            assert run.returncode == 0, run.stdout + run.stderr
            assert client.get('golang').revision == 2
            assert sorted(r.id for r in client.iter_records(limit=1)) == ['golang','python']
            expect(grpc.StatusCode.INVALID_ARGUMENT, lambda: client.list(cursor='bad!'))
            filtered = client.search([0,0,0], k=10, filter={'lang':'python'}, exact=True,
                min_visible=messages.Token(id='golang', revision=2))
            assert [h.id for h in filtered.hits] == ['python']
            report['checks'].append('go_python_cross_read_write_pagination_and_filter')
            assert len(client.checkpoint().partitions) == 4
            assert len(client.prune().partitions) == 4
            assert len(client.rebuild().partitions) == 4
            deleted = client.delete('golang', expected_revision=2, request_id='delete-go')
            assert client.get('golang').deleted
            assert [h.id for h in client.search([0,0,0], k=10, min_visible=deleted.token).hits] == ['python']
            assert [r.id for r in client.iter_records()] == ['python']
            assert len(list(client.iter_records(include_deleted=True))) == 2
            cluster.scale(0)
            assert [r.id for r in client.iter_records()] == ['python']
            report['checks'].append('grpc_maintenance_delete_tombstones_and_worker_independent_list')
            # HTTP remains available on the same Gateway, over the same records.
            assert cluster.request('/v1/records/python')['revision'] == 2
            report['checks'].append('http_and_grpc_share_durable_state')
        report.update(status='passed', namespace=namespace)
    except BaseException as exc:
        report.update(status='failed', error=repr(exc))
        raise
    finally:
        cluster.close()
        report['all_children_stopped'] = all(p.poll() is not None for p in cluster.children.values())
        (ROOT/'reports/rpc-http.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__ == '__main__':
    main()
