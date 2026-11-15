"""End-to-end vectorctl commands against independent processes and real storage."""
import json
import os
from pathlib import Path
import signal
import socket as sockets
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent


def main():
    report = {'status': 'running', 'checks': []}
    env = dict(os.environ, API_TOKEN='cli-acceptance-public',
               CLUSTER_TOKEN='cli-acceptance-internal', LEASE_TTL='6s')
    directory = tempfile.TemporaryDirectory(prefix='vctl-', dir='/tmp')
    socket = str(Path(directory.name) / 'control.sock')
    base = [str(ROOT / '.tools/vectorctl'), '--endpoint', 'http://127.0.0.1:18302', '--socket', socket]
    namespace = 'cli-test/' + uuid.uuid4().hex
    report['namespace'] = namespace
    log = (ROOT / 'reports/cli-process.log').open('w')
    proc = subprocess.Popen(base + ['cluster', 'start', '--namespace', namespace,
        '--workers', '2', '--partitions', '4', '--dimension', '3', '--base-port', '18300', '--grpc-port', '18350'],
        cwd=ROOT, env=env, stdout=log, stderr=log)
    def call(*args, failure=None):
        result = subprocess.run(base + list(args), cwd=ROOT, env=env,
                                text=True, capture_output=True, timeout=100)
        if failure:
            assert result.returncode != 0 and failure in result.stderr, (args, result.stdout, result.stderr)
            assert result.stdout == '', result.stdout
            return
        assert result.returncode == 0, (args, result.stderr)
        return json.loads(result.stdout)
    try:
        deadline = time.monotonic() + 40
        while not Path(socket).exists():
            assert proc.poll() is None
            assert time.monotonic() < deadline
            time.sleep(.1)
        state = call('cluster', 'local-status')  # queued while supervisor finishes startup
        assert state['ready'] and len(state['desired_workers']) == 2
        with sockets.create_connection(('127.0.0.1',18350),timeout=5):
            pass
        assert Path(socket).stat().st_mode & 0o777 == 0o600
        for command in ('status', 'nodes', 'leader', 'partitions', 'layout', 'health'):
            assert call('cluster', command)
        report['checks'].append('start_and_all_status_commands_owner_only_control_socket')
        first = call('record', 'insert', '--id', 'a ?#', '--vector', '[1,2,3]',
                     '--payload', '{"group":"one"}', '--request-id', 'insert-a')
        assert first['token']['revision'] == 1
        assert call('record', 'insert', '--id', 'a ?#', '--vector', '[1,2,3]',
                    '--payload', '{"group":"one"}', '--request-id', 'insert-a') == first
        call('record', 'insert', '--id', 'a ?#', '--vector', '[1,2,3]', '--request-id', 'duplicate', failure='HTTP 409')
        assert call('record', 'get', '--id', 'a ?#')['revision'] == 1
        call('record', 'update', '--id', 'a ?#', '--vector', '[0,0,0]', '--payload', '{"group":"two"}',
             '--revision', '1', '--request-id', 'update-a')
        call('record', 'update', '--id', 'a ?#', '--vector', '[3,3,3]', '--revision', '1',
             '--request-id', 'stale-update', failure='HTTP 409')
        call('record', 'get', '--id', 'absent', failure='HTTP 404')
        report['checks'].append('insert_idempotency_duplicate_conflict_get_update_CAS_encoded_ID')
        vector_file = Path(directory.name) / 'vector.json'
        vector_file.write_text('[1,1,1]')
        call('record', 'upsert', '--id', 'd', '--vector', '@' + str(vector_file), '--request-id', 'upsert-b')
        hits = call('search', '--vector', '[0,0,0]', '--k', '2', '--exact',
                    '--visible-id', 'd', '--visible-revision', '1')['hits']
        assert [h['id'] for h in hits] == ['a ?#', 'd']
        filtered = call('search', '--vector', '[0,0,0]', '--k', '2', '--filter', '{"group":"two"}')['hits']
        assert [h['id'] for h in filtered] == ['a ?#']
        report['checks'].append('vector_file_top_N_filter_and_read_your_write_token')
        listed = []
        cursor = ''
        for _ in range(10):
            options = ['--limit', '1'] + (['--cursor', cursor] if cursor else [])
            page = call('record', 'list', *options)
            listed.extend(page['records'])
            cursor = page.get('next_cursor', '')
            if not cursor:
                break
        assert not cursor and sorted(r['id'] for r in listed) == ['a ?#', 'd']
        assert all('vector' in r for r in listed)
        call('record', 'list', '--cursor', 'invalid!', failure='HTTP 400')
        report['checks'].append('list_cursor_pagination_across_data_partitions')

        for command in ('checkpoint', 'prune', 'rebuild'):
            call('cluster', command)
        call('cluster', 'scale', '--workers', '3')
        assert len(call('cluster', 'local-status')['desired_workers']) == 3
        call('cluster', 'scale', '--workers', '1')
        assert len(call('search', '--vector', '[0,0,0]', '--k', '2')['hits']) == 2
        call('record', 'delete', '--id', 'a ?#', '--revision', '2', '--request-id', 'delete-a')
        assert call('record', 'get', '--id', 'a ?#')['deleted'] is True
        hits = call('search', '--vector', '[0,0,0]', '--k', '2', '--visible-id', 'a ?#', '--visible-revision', '3')['hits']
        assert [h['id'] for h in hits] == ['d']
        report['checks'].append('maintenance_scale_and_versioned_delete')
        assert [r['id'] for r in call('record', 'list')['records']] == ['d']
        assert len(call('record', 'list', '--include-deleted')['records']) == 2
        call('cluster', 'scale', '--workers', '0')
        assert [r['id'] for r in call('record', 'list')['records']] == ['d']
        report['checks'].append('list_hides_tombstones_and_works_without_workers')

        children = call('cluster', 'local-status')['processes']
        assert call('cluster', 'stop')['stopping']
        assert proc.wait(timeout=20) == 0
        assert not Path(socket).exists()
        for child in children.values():
            try:
                os.kill(child['pid'], 0)
            except ProcessLookupError:
                continue
            raise AssertionError('child process still running: ' + str(child))
        report['checks'].append('stop_reaps_all_children_and_removes_control_socket')
        report['status'] = 'passed'
    except BaseException as exc:
        report.update(status='failed', error=repr(exc))
        raise
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=30)
        log.close()
        directory.cleanup()
        (ROOT / 'reports/cli-http.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
