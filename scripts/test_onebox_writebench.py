import os
from pathlib import Path
import subprocess
import sys
import unittest

from onebox_writebench import BenchGuard, aggregate
from resource_guard import GIB, limit_reason


def client_report(interrupted):
    return dict(
        Target=10, Started=4, Ack=3, Unsent=6, Interrupted=interrupted, PeakInflight=2,
        Errors={'Unavailable': 1}, ErrorExample={'Unavailable': 'write conflict'},
        ElapsedSeconds=.5 if interrupted else 1, LastAckNS=2, AckShard=[3] + [0]*15,
        LatencyHistogramUS={'1000': 2, '10000': 1},
        BySecond=[dict(Started=4, Ack=2, Failed=1), dict(Started=0, Ack=1, Failed=0)],
    )


class WriteBenchTest(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'darwin', 'macOS build target')
    def test_rosetta_launcher_builds_native_server_by_default(self):
        for arch in ('arm64', 'x86_64'):
            if subprocess.run(['/usr/bin/arch', '-'+arch, '/usr/bin/true'], capture_output=True).returncode:
                self.skipTest('requires Apple Silicon and Rosetta')
        env = {k: v for k, v in os.environ.items() if k not in ('GOARCH', 'GOOS')}
        cmd = ['/usr/bin/arch', '-x86_64', '/bin/bash', 'scripts/go.sh', 'env', 'GOARCH']
        root = Path(__file__).resolve().parents[1]
        for build_env, expected in ((env, 'arm64'), (dict(env, GOARCH='amd64'), 'amd64')):
            self.assertEqual(subprocess.check_output(cmd, cwd=root, env=build_env, text=True).strip(), expected)

    def test_writebench_disk_reserve_is_independent_of_run_size(self):
        sample = dict(memory_pressure=1, owned_bytes=3*GIB, disk_free_bytes=15*GIB+1)
        self.assertIsNotNone(limit_reason(sample))
        self.assertIsNone(limit_reason(sample, limit_owned_disk=BenchGuard.limit_owned_disk))
        for free in (15*GIB, 15*GIB-1):
            sample['disk_free_bytes'] = free
            self.assertEqual(limit_reason(sample, limit_owned_disk=BenchGuard.limit_owned_disk),
                             'disk free at or below 15 GiB')

    def test_counts_window_and_late_acks_separately(self):
        r = aggregate([client_report(False), client_report(False)], 1)
        self.assertEqual((r['planned'], r['invoked'], r['not_sent']), (20, 8, 12))
        self.assertEqual((r['durable_acks'], r['ack_qps_in_window'], r['ack_after_window']), (6, 4, 2))
        self.assertEqual(r['rpc_failures'], {'Unavailable': 2})
        self.assertEqual((r['p50_ack_ms_upper'], r['p99_ack_ms_upper']), (1, 10))

    def test_interruption_has_no_full_window_throughput(self):
        r = aggregate([client_report(True)], 1)
        self.assertFalse(r['full_window'])
        self.assertIsNone(r['sent_qps'])
        self.assertIsNone(r['ack_qps_in_window'])
        self.assertEqual(r['invoked_per_generation_second'], 8)

    def test_inconsistent_client_accounting_fails(self):
        raw = client_report(False)
        raw['AckShard'][0] = 2
        with self.assertRaises(AssertionError):
            aggregate([raw], 1)

    def test_worker_histograms_bracket_last_ack(self):
        from concurrent.futures import Future
        import json
        import tempfile
        import threading
        from unittest.mock import patch
        from onebox_writebench import capture_drain_stages

        last_ack = 5_000_000_000
        before = Future()
        before.set_result(dict(started_ns=3_900_000_000, captured_ns=4_000_000_000, nodes={}))
        stop = threading.Event()
        stop.set()
        after = dict(started_ns=5_100_000_000, captured_ns=5_200_000_000, nodes={})
        with tempfile.TemporaryDirectory() as logs, patch('onebox_writebench.worker_stage_sample', return_value=after):
            capture_drain_stages(stop, {}, '', Path(logs), last_ack, before)
            rows = (Path(logs) / 'worker-stages.jsonl').read_text().splitlines()
        self.assertEqual([json.loads(row)['since_last_ack_s'] for row in rows], [-1, .2])

    def test_slow_frontier_collection_does_not_block_follower_samples(self):
        import grpc
        import json
        import tempfile
        import threading
        import time
        from types import SimpleNamespace
        from onebox_writebench import catchup

        second_read = threading.Event()
        position = dict(sequence=1, data_offset=1, hash='one')
        state_calls = []
        def state():
            self.assertTrue(second_read.wait(2), 'status read blocked the follower probe')
            state_calls.append(None)
            if len(state_calls) == 1:
                raise TimeoutError('control sample timed out')
            return dict(data_progress={'data-0000': dict(head=1, completed_lsn=1)},
                        graph_heads={'index-0000': dict(committed=position)})

        attempts = []
        class ProbeTimeout(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.DEADLINE_EXCEEDED

        def search(vector, k, timeout):
            attempts.append(time.monotonic())
            if len(attempts) == 1:
                raise ProbeTimeout()
            second_read.set()
            pos = SimpleNamespace(**position)
            read = SimpleNamespace(index_partition='index-0000', replica='follower', session='test', owner_epoch=1,
                                   node='b', target=pos, applied=pos, query_ms=0, queue_ms=0, leader_capture_ms=0,
                                   wait_apply_ms=0)
            return SimpleNamespace(hits=[SimpleNamespace(id='written')], follower_reads=[read])

        with tempfile.TemporaryDirectory() as logs:
            result = catchup(SimpleNamespace(follower_search=search), dict(last_ack_ns=time.time_ns(), ack_shards=[1]),
                             centers=[[1]], read_state=state, check_background=lambda: None, logs=Path(logs), drain_seconds=3)
            trace = json.loads((Path(logs) / 'drain.json').read_text())
        self.assertEqual(result['first_probe_ms'], trace['observations'][0]['sent_ms'])
        self.assertLess(result['first_probe_ms'], trace['samples'][0]['sent_ms'])
        self.assertEqual(result['status'], 'confirmed')
        self.assertEqual((result['confirming_attempt'], result['probe_errors']), (2, 1))
        self.assertEqual(result['pinned_durable_events'], 1)
        self.assertGreaterEqual(len(state_calls), 2)
