"""End-to-end HTTP tests with a real process crash and restart.
Requires local PD/TiKV and SeaweedFS. Only its own child process is stopped.
"""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent
TOKEN = 'local-vector-test-token'
URL = 'http://127.0.0.1:18091'


def call(method, path, data=None, expected=200, auth=True):
    headers = {'Content-Type': 'application/json'}
    if auth:
        headers['Authorization'] = 'Bearer ' + TOKEN
    req = urllib.request.Request(URL + path, method=method, headers=headers,
                                 data=json.dumps(data).encode() if data is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=35) as r:
            status, value = r.status, json.load(r)
    except urllib.error.HTTPError as e:
        status, value = e.code, json.load(e)
    assert status == expected, (method, path, status, value)
    return value


def main():
    namespace = 'http-test/' + uuid.uuid4().hex
    env = dict(os.environ, API_TOKEN=TOKEN, NAMESPACE=namespace,
               DIMENSION='3', LISTEN_ADDR='127.0.0.1:18091')
    (ROOT / 'reports').mkdir(exist_ok=True)
    checks = []
    started = time.monotonic()
    with (ROOT / 'reports/http-server.log').open('w') as log:
        def start():
            proc = subprocess.Popen([str(ROOT / '.tools/vector-server')], cwd=ROOT,
                                    env=env, stdout=log, stderr=log)
            for _ in range(120):
                if proc.poll() is not None:
                    raise RuntimeError('server failed; see reports/http-server.log')
                try:
                    call('GET', '/healthz')
                    return proc
                except (OSError, AssertionError):
                    time.sleep(.25)
            proc.kill()
            proc.wait()
            raise TimeoutError('server readiness timeout')

        proc = start()
        try:
            call('GET', '/healthz', expected=401, auth=False)
            call('PUT', '/v1/records/bad', {'request_id': 'bad-dimension', 'vector': [1]}, expected=400)
            checks.append('authentication_and_dimension_validation')
            call('POST', '/v1/search', {'vector': [0, 0, 0], 'k': 1,
                 'min_visible': {'id': 'missing', 'revision': 1}}, expected=503)
            a = {'request_id': 'a-create', 'vector': [0, 0, 0], 'payload': {'tag': 'red'}}
            token = call('PUT', '/v1/records/a', a)['token']
            assert call('PUT', '/v1/records/a', a)['token'] == token
            call('PUT', '/v1/records/a', dict(a, vector=[1, 1, 1]), expected=409)
            b = call('PUT', '/v1/records/b', {'request_id': 'b-create', 'vector': [1, 0, 0], 'payload': {'tag': 'blue'}})['token']
            result = call('POST', '/v1/search', {'vector': [0, 0, 0], 'k': 1, 'min_visible': b})
            assert result['hits'][0]['id'] == 'a'
            result = call('POST', '/v1/search', {'vector': [0, 0, 0], 'k': 1, 'filter': {'tag': 'blue'}})
            assert result['hits'][0]['id'] == 'b'
            checks.append('durable_write_idempotency_filtered_exact_topk')
            token = call('PUT', '/v1/records/a', {'request_id': 'a-update', 'expected_revision': 1,
                                                 'vector': [10, 0, 0], 'payload': {'tag': 'red'}})['token']
            call('PUT', '/v1/records/a', {'request_id': 'a-stale', 'expected_revision': 1, 'vector': [0, 0, 0]}, expected=409)
            result = call('POST', '/v1/search', {'vector': [0, 0, 0], 'k': 1, 'min_visible': token})
            assert result['hits'][0]['id'] == 'b'
            checks.append('update_supersedes_old_vector')
            checkpoint = call('POST', '/v1/checkpoint', {})
            assert checkpoint['format'] == 'hnswlib-0.8.0-le64-v1'
            rebuilt = call('POST', '/v1/rebuild', {})
            assert rebuilt['generation'] != checkpoint['generation']
            assert rebuilt['log_version'] == 1 and rebuilt['offset'] == 3
            pruned = call('POST', '/v1/log/prune', {})
            assert pruned['retained_after'] == rebuilt['offset']
            checks.append('graph_snapshot_and_rebuild_publish')
            checks.append('checkpoint_cursor_bound_log_prune')
            deleted = call('DELETE', '/v1/records/b', {'request_id': 'b-delete'})['token']
            proc.kill()
            proc.wait(timeout=10)
            proc = start()
            result = call('POST', '/v1/search', {'vector': [0, 0, 0], 'k': 10, 'min_visible': deleted})
            assert [h['id'] for h in result['hits']] == ['a']
            assert call('GET', '/v1/records/b')['deleted'] is True
            assert call('PUT', '/v1/records/a', a)['token']['revision'] == 1
            assert call('GET', '/v1/records/a')['revision'] == 2
            checks.append('process_crash_snapshot_restore_later_delete_replay')
            checks.append('idempotent_retry_after_restart_does_not_overwrite_new_version')
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
    report = {'status': 'passed', 'checks': checks, 'namespace': namespace,
              'elapsed_seconds': round(time.monotonic() - started, 3),
              'scope': 'real TiKV + SeaweedFS; process crash, not machine or quorum failure'}
    (ROOT / 'reports/http-integration.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
