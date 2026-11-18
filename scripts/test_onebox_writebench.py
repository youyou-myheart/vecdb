import unittest

from onebox_writebench import aggregate


def client_report(interrupted):
    return dict(
        Target=10, Started=4, Ack=3, Unsent=6, Interrupted=interrupted, PeakInflight=2,
        Errors={'Unavailable': 1}, ErrorExample={'Unavailable': 'write conflict'},
        ElapsedSeconds=.5 if interrupted else 1, LastAckNS=2, AckShard=[3] + [0]*15,
        LatencyHistogramUS={'1000': 2, '10000': 1},
        BySecond=[dict(Started=4, Ack=2, Failed=1), dict(Started=0, Ack=1, Failed=0)],
    )


class WriteBenchTest(unittest.TestCase):
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

    def test_slow_frontier_collection_does_not_block_follower_samples(self):
        import tempfile
        import threading
        import time
        from pathlib import Path
        from types import SimpleNamespace
        from onebox_writebench import catchup

        second_read = threading.Event()
        position = dict(sequence=1, data_offset=1, hash='one')
        def state():
            self.assertTrue(second_read.wait(2), 'status read blocked the follower probe')
            return dict(data_progress={'data-0000': dict(head=1, completed_lsn=1)},
                        graph_heads={'index-0000': dict(committed=position)})

        attempts = []
        def search(vector, k, timeout):
            attempts.append(time.monotonic())
            if len(attempts) >= 2:
                second_read.set()
            pos = SimpleNamespace(**position)
            read = SimpleNamespace(index_partition='index-0000', replica='follower', session='test', owner_epoch=1,
                                   node='b', target=pos, applied=pos, query_ms=0, queue_ms=0, leader_capture_ms=0,
                                   wait_apply_ms=0)
            return SimpleNamespace(hits=[SimpleNamespace(id='written')], follower_reads=[read])

        with tempfile.TemporaryDirectory() as logs:
            result = catchup(SimpleNamespace(follower_search=search), dict(last_ack_ns=time.time_ns(), ack_shards=[1]),
                             centers=[[1]], read_state=state, check_background=lambda: None, logs=Path(logs), drain_seconds=3)
        self.assertEqual(result['status'], 'confirmed')
        self.assertEqual(result['confirming_attempt'], 1)
        self.assertEqual(result['pinned_durable_events'], 1)
