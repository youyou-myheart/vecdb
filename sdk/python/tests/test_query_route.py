import threading
import time
import unittest
import grpc
from unittest.mock import Mock

from vectorstore import Client, messages
from vectorstore.query_route import ranked_partitions, initial_fanout


def cluster():
    state = messages.ClusterStatus(routing=messages.ClusterRouting(version=1,
        centers=[messages.Center(vector=[x]) for x in (0, 10, 20)]))
    for i, count in enumerate((3, 5, 20)):
        id = f'index-{i:04d}'
        for replica in range(2):
            name = f'w-{i}-{replica}'
            node = state.nodes.add(id=name, session='s')
            node.load[id].CopyFrom(messages.ShardLoad(live_records=count, records_known=True))
            state.replica_owners[id].owners.add(node=name, session='s', ready=True)
    return state


class QueryRoutingTests(unittest.TestCase):
    def test_invalid_refresh_interval(self):
        for value in (0, -1, float('inf'), float('nan')):
            with self.assertRaises(ValueError):
                Client('localhost:1', 'secret', search_refresh_interval=value)

    def client(self, **options):
        c = Client('localhost:1', 'secret', **options)
        self.addCleanup(c.close)
        c.stub = Mock()
        c.stub.GetSearchRoutes.return_value = cluster()
        return c

    def test_counts_do_not_sum_replicas(self):
        ranked = ranked_partitions(cluster(), [0])
        self.assertEqual(initial_fanout(ranked, 4), 2)
        self.assertEqual(initial_fanout(ranked, 2), 1)
        self.assertEqual(ranked_partitions(cluster(), [19])[0][1], 'index-0002')

    def test_expand_when_filter_or_lag_reduces_hits(self):
        c = self.client()
        calls = []
        def search(q, **kwargs):
            calls.append(list(q.index_partitions))
            n = 4 if len(q.index_partitions) == 3 else 1
            return messages.SearchResponse(hits=[messages.Hit(id=str(i)) for i in range(n)])
        c.stub.Search.side_effect = search
        self.assertEqual(len(c.search([0], k=4, filter={'tag': 'x'}).hits), 4)
        self.assertEqual(calls, [['index-0000', 'index-0001'], ['index-0000', 'index-0001', 'index-0002']])
        self.assertEqual(c.stub.GetSearchRoutes.call_count, 1)

    def test_global_exact_and_explicit_all_do_not_fetch_metadata(self):
        c = self.client()
        c.search([0], exact=True)
        c.search([0], all_partitions=True)
        c.stub.GetSearchRoutes.assert_not_called()
        self.assertEqual(list(c.stub.Search.call_args.args[0].index_partitions), [])

    def test_unknown_counts_search_all_and_expired_cache_is_not_used(self):
        c = self.client()
        state = cluster()
        state.nodes[0].load['index-0000'].records_known = False
        state.nodes[1].load['index-0000'].records_known = False
        c.stub.GetSearchRoutes.return_value = state
        c.stub.Search.return_value = messages.SearchResponse(hits=[messages.Hit(id='a')])
        c.search([0], k=1)
        self.assertEqual(len(c.stub.Search.call_args.args[0].index_partitions), 3)
        c._search_snapshot = (cluster(), time.monotonic()-60)
        c.search([0], k=1)
        self.assertEqual(list(c.stub.Search.call_args.args[0].index_partitions), [])

    def test_periodic_refresh_and_shutdown(self):
        c = self.client(search_refresh_interval=.02)
        refreshed = threading.Event()
        def lookup(*args, **kwargs):
            if c.stub.GetSearchRoutes.call_count >= 2:
                refreshed.set()
            return cluster()
        c.stub.GetSearchRoutes.side_effect = lookup
        c.stub.Search.return_value = messages.SearchResponse(hits=[messages.Hit(id='a')])
        c.search([0], k=1)
        self.assertTrue(refreshed.wait(1))
        c.close()
        self.assertFalse(c._search_thread.is_alive())

    def test_search_by_id_reads_vector_once(self):
        c = self.client()
        c.stub.Get.return_value = messages.Record(id='A', vector=[19])
        c.stub.Search.return_value = messages.SearchResponse(hits=[messages.Hit(id='a')])
        c.search_by_id('A', k=1)
        c.stub.Get.assert_called_once()
        self.assertEqual(list(c.stub.Search.call_args.args[0].index_partitions), ['index-0002'])

    def test_lightweight_route_falls_back_only_for_old_servers(self):
        class OldServer(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNIMPLEMENTED
        c = self.client()
        c.stub.GetSearchRoutes.side_effect = OldServer()
        c.stub.GetCluster.return_value = cluster()
        c.stub.Search.return_value = messages.SearchResponse(hits=[messages.Hit(id='a')])
        c.search([19], k=1)
        c.stub.GetCluster.assert_called_once()
        self.assertEqual(list(c.stub.Search.call_args.args[0].index_partitions), ['index-0002'])
