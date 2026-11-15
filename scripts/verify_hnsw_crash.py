"""Real SIGKILL recovery of a nontrivial HNSW graph, including native-byte identity."""
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parent.parent
URL = 'http://127.0.0.1:18092'
TOKEN = 'hnsw-crash-test-token'


def call(method, path, data=None):
    request = urllib.request.Request(URL + path, method=method,
        headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'},
        data=None if data is None else json.dumps(data).encode())
    with urllib.request.urlopen(request, timeout=35) as response:
        return json.load(response)


def main():
    report_path = ROOT / 'reports/hnsw-crash.json'
    report_path.parent.mkdir(exist_ok=True)
    report = {'status': 'running', 'scope': 'SIGKILL of test vector-server only; real TiKV + SeaweedFS remain running'}
    report_path.write_text(json.dumps(report, indent=2))
    namespace = 'hnsw-crash/' + uuid.uuid4().hex
    env = dict(os.environ, API_TOKEN=TOKEN, NAMESPACE=namespace, DIMENSION='32', LISTEN_ADDR='127.0.0.1:18092')
    # Fail before spawning if another service owns the test port.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 18092))
    s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:18333',
        aws_access_key_id='poc-access', aws_secret_access_key='poc-secret-local-only',
        region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
    checks, restarts = [], []
    proc = None
    started = time.monotonic()
    with (ROOT / 'reports/hnsw-crash-server.log').open('w') as log:
        def start():
            p = subprocess.Popen([str(ROOT / '.tools/vector-server')], cwd=ROOT, env=env, stdout=log, stderr=log)
            began = time.monotonic()
            try:
                for _ in range(300):
                    if p.poll() is not None:
                        raise RuntimeError('server exited; see hnsw-crash-server.log')
                    try:
                        health = call('GET', '/healthz')
                        assert health['namespace'] == namespace
                        return p, time.monotonic() - began
                    except (OSError, urllib.error.URLError):
                        time.sleep(.1)
                raise TimeoutError('readiness timeout')
            except BaseException:
                if p.poll() is None:
                    p.kill()
                p.wait(timeout=10)
                raise

        def kill(p):
            p.kill()
            assert p.wait(timeout=10) == -signal.SIGKILL

        def snapshot(manifest):
            body = s3.get_object(Bucket='vector-index-poc', Key=manifest['key'])['Body']
            try:
                raw = body.read()
            finally:
                body.close()
            assert len(raw) == manifest['size']
            assert hashlib.sha256(raw).hexdigest() == manifest['sha256']
            state = json.loads(raw)
            assert state['offset'] == manifest['offset']
            graph = base64.b64decode(state['graph'], validate=True)
            return state, hashlib.sha256(graph).hexdigest()

        try:
            proc, _ = start()
            records = {}
            for j in range(600):
                vector = [3 * math.sin((j % 8 + 1) * (k + 1) * .071) + .1 * math.cos((j + 1) * (k + 1)) for k in range(32)]
                records[str(j)] = {'vector': vector, 'payload': {'group': str(j % 4)}, 'revision': 1}
                token = call('PUT', '/v1/records/' + str(j), dict(request_id='create-' + str(j), vector=vector, payload=records[str(j)]['payload']))['token']
            query = {'vector': records['0']['vector'], 'k': 10, 'ef_search': 512}
            call('POST', '/v1/search', dict(query, min_visible=token))
            # Preserve a stale graph node deliberately: rebuilding only records cannot reproduce it.
            records['0'] = {'vector': [2.] * 32, 'payload': {'group': 'changed'}, 'revision': 2}
            token = call('PUT', '/v1/records/0', dict(request_id='update-before-snapshot', vector=records['0']['vector'], payload=records['0']['payload']))['token']
            before = call('POST', '/v1/search', dict(query, min_visible=token))
            manifest = call('POST', '/v1/checkpoint', {})
            assert manifest['offset'] == 601 and manifest['format'] == 'hnswlib-0.8.0-le64-v1'
            state, graph_sha = snapshot(manifest)
            assert len(state['nodes']) == 601 and len(state['records']) == 600
            # Remove all checkpoint-covered logs so recovery must use snapshot/records.
            floor = 0
            while floor < manifest['offset']:
                floor = call('POST', '/v1/log/prune', {})['retained_after']
            kill(proc)
            proc, elapsed = start()
            restarts.append(elapsed)
            assert call('POST', '/v1/search', query) == before
            recovered_manifest = call('POST', '/v1/checkpoint', {})
            recovered, recovered_sha = snapshot(recovered_manifest)
            assert recovered_sha == graph_sha, 'native graph bytes changed: not an identical graph restore'
            assert recovered['nodes'] == state['nodes'] and recovered['next'] == state['next']
            assert recovered['records'] == state['records']
            checks.append('SIGKILL_restore_native_graph_byte_identical_with_stale_node')
            checks.append('ANN_results_identical_after_restore_and_log_prune')
            # Mutations acknowledged after the snapshot, then kill without another checkpoint.
            records['1'] = {'vector': [0.] * 32, 'payload': {'group': 'moved'}, 'revision': 2}
            call('PUT', '/v1/records/1', dict(request_id='tail-update', vector=records['1']['vector'], payload=records['1']['payload']))
            deleted = call('DELETE', '/v1/records/2', {'request_id': 'tail-delete'})['token']
            del records['2']
            records['600'] = {'vector': [.001] * 32, 'payload': {'group': 'new'}, 'revision': 1}
            token = call('PUT', '/v1/records/600', dict(request_id='tail-insert', vector=records['600']['vector'], payload=records['600']['payload']))['token']
            kill(proc)
            proc, elapsed = start()
            restarts.append(elapsed)
            all_hits = call('POST', '/v1/search', {'vector': [0.] * 32, 'k': 1000, 'exact': True, 'min_visible': token})['hits']
            assert {h['id'] for h in all_hits} == set(records)
            for hit in all_hits:
                expected = records[hit['id']]
                assert hit['revision'] == expected['revision'] and hit['payload'] == expected['payload']
                assert math.isclose(hit['distance'], sum(x*x for x in expected['vector']), rel_tol=1e-12, abs_tol=1e-12)
            nearest = call('POST', '/v1/search', {'vector': [0.] * 32, 'k': 10, 'ef_search': 512})['hits']
            assert nearest[0]['id'] == '1' and nearest[1]['id'] == '600'
            assert call('GET', '/v1/records/2')['deleted']
            assert call('POST', '/v1/search', {'vector': [0.] * 32, 'k': 10, 'filter': {'group': 'moved'}})['hits'][0]['id'] == '1'
            assert call('PUT', '/v1/records/1', {'request_id': 'tail-update', 'vector': [0.] * 32, 'payload': {'group': 'moved'}})['token']['revision'] == 2
            final_manifest = call('POST', '/v1/checkpoint', {})
            final_state, _ = snapshot(final_manifest)
            assert final_manifest['offset'] == 604 and len(final_state['nodes']) == 603
            assert len(final_state['records']) == 601  # Includes the deleted record's tombstone.
            checks.extend(['SIGKILL_replay_uncheckpointed_insert_update_delete',
                           'all_600_live_records_versions_payloads_distances_verified',
                           'deleted_record_not_resurrected_and_idempotent_retry',
                           'original_graph_retained_and_two_tail_nodes_added'])
            report.update(status='passed', namespace=namespace, checks=checks,
                initial_graph_nodes=601, restored_graph_sha256=graph_sha,
                final_graph_nodes=603, snapshot_offset=601, recovered_offset=604,
                restart_ready_seconds=restarts, elapsed_seconds=time.monotonic()-started)
        except BaseException as exc:
            report.update(status='failed', namespace=namespace, checks=checks, error=repr(exc))
            raise
        finally:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
            report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
