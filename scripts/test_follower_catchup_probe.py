import copy
import hashlib
import unittest

from follower_catchup_probe import final_frontier, summarize_capture


def position(n):
    return dict(sequence=n, data_offset=10*n, hash=f'hash-{n}')


def read(index, target, applied, query_ms):
    return dict(index=index, replica='replica-b', session='s', epoch=1,
                target=position(target), applied=position(applied), query_ms=query_ms,
                queue_ms=.25, capture_ms=2, wait_apply_ms=4)


class FollowerCatchupProbeTest(unittest.TestCase):
    def test_final_targets_require_coverage_of_every_ack_and_source(self):
        key = 'v-0001'
        shard = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % 2
        progress = {f'data-{i:04d}': dict(head=0, completed_lsn=0) for i in range(2)}
        source = progress[f'data-{shard:04d}']
        source.update(head=5, completed_lsn=4)
        state = dict(data_progress=progress, graph_heads={'index': dict(committed=position(3))})
        records = {key: dict(revision=5, deleted=True)}
        self.assertIsNone(final_frontier(state, records, {'index'}, 2))
        source['completed_lsn'] = 5
        self.assertEqual(final_frontier(state, records, {'index'}, 2), {'index': position(3)})
        source['head'] = 4
        with self.assertRaises(AssertionError):
            final_frontier(state, records, {'index'}, 2)

    def test_an_early_successful_stale_read_does_not_certify_the_last_writes(self):
        targets = {'a': position(11), 'b': position(21)}
        early = dict(attempt=1, sent_ms=4, received_ms=30, hit=[],
                     reads=[read('a', 11, 11, 2), read('b', 20, 20, 17)])
        good = dict(attempt=2, sent_ms=34, received_ms=110, hit=[],
                    reads=[read('a', 12, 12, 12), read('b', 21, 22, 3)])
        self.assertIsNone(summarize_capture([early], targets))
        row = summarize_capture([early, good, dict(good, received_ms=1200)], targets)
        self.assertEqual(row['first_confirmed_ms'], 110)
        self.assertEqual(row['applied_upper_ms'], 107)
        self.assertEqual(row['applied_lower_ms'], 10.25)
        good['reads'][1]['session'] = 'replacement'
        self.assertEqual(summarize_capture([early, good], targets)['applied_lower_ms'], 0)
        self.assertEqual(row['confirming_attempt'], 2)
        self.assertEqual(row['first_probe_ms'], 4)

    def test_partial_duplicate_or_regressed_reads_fail_closed(self):
        targets = {'a': position(3), 'b': position(2)}
        rows = [read('a', 3, 3, .1), read('b', 2, 2, .2)]
        invalid = copy.deepcopy(rows)
        invalid[0]['applied']['hash'] = 'other'
        for reads in (rows[1:], [rows[0], rows[0]], invalid, [rows[0],read('b',2,1,.2)]):
            with self.assertRaises(AssertionError):
                summarize_capture([dict(attempt=1, sent_ms=1, received_ms=5, reads=reads)], targets)
