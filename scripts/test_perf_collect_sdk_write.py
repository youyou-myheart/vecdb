import unittest

from perf_collect.sdk_write import all_caught, summarize


class SDKWriteCollectionTest(unittest.TestCase):
    def test_raw_accounting_includes_ack_tail_and_final_peer(self):
        client = {
            'Target': 10, 'Started': 4, 'Unsent': 6, 'DroppedInflight': 5, 'PeakInflight': 3,
            'Ack': 3, 'AckShard': [1, 2], 'AckLatenciesUS': [221, 9999, 100], 'LastAckNS': 2_002_000_000,
            'Errors': {'Unavailable': 1}, 'ErrorExample': {}, 'Interrupted': False,
            'RPCDestinations': {'gateway': 1, 'owner': 2, 'unknown': 1},
            'BySecond': [{'Started': 4, 'Ack': 2, 'Failed': 1}, {'Started': 0, 'Ack': 1, 'Failed': 0}],
        }
        row = summarize([client], 1, {'owner'}, 'gateway', 1_000_000_000)
        self.assertEqual([row[k] for k in ('durable_acks', 'durable_acks_in_window', 'after_window_acks')], [3, 2, 1])
        self.assertEqual(row['after_window_ack_tail_ms'], 2)
        self.assertEqual([row[k] for k in ('p50_ack_ms', 'p99_ack_ms')], [.221, 9.999])
        self.assertEqual([row[k] for k in ('dropped_inflight_cap', 'dropped_scheduler_window')], [5, 1])
        self.assertEqual([row[k] for k in ('observed_owner_peer_calls', 'gateway_peer_calls',
                                         'other_or_unknown_peer_calls')], [2, 1, 1])

    def test_convergence_requires_each_graph_replica_at_commit(self):
        leader = dict(partition='index-0/a', node='a', session='a-session', epoch=1)
        follower = dict(partition='index-0/b', node='b', session='b-session', epoch=2)
        state = dict(
            ready=True, fully_replicated=True,
            data_progress={'data-0': dict(owner_available=True, completed_lsn=2, head=2)},
            graph_heads={'index-0': dict(committed=2, visible=2, leader=leader)},
            replica_plans={'index-0': dict(publisher=leader['partition'])},
            replica_owners={'index-0': dict(owners=[leader, follower])},
            graph_positions={leader['partition']: 2, follower['partition']: 1},
        )
        self.assertFalse(all_caught(state, {'data-0': 2}, 1, 2))
        state['graph_positions'][follower['partition']] = 2
        self.assertTrue(all_caught(state, {'data-0': 2}, 1, 2))


if __name__ == '__main__':
    unittest.main()
