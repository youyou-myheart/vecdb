import unittest
from unittest.mock import Mock, patch
import grpc
from vectorstore import Client, messages

class ClientTests(unittest.TestCase):
    def test_follower_search_is_one_rpc_with_deadline(self):
        with Client('localhost:1', 'secret') as client:
            client.stub = Mock()
            client.follower_search([1, 2], k=3, filter={'tag': 'x'}, timeout=.25)
            call = client.stub.FollowerSearch.call_args
            self.assertEqual(call.args[0].k, 3)
            self.assertEqual(dict(call.args[0].filter), {'tag': 'x'})
            self.assertEqual(call.kwargs['timeout'], .25)
            self.assertEqual(call.kwargs['metadata'], (('authorization', 'Bearer secret'),))
            client.stub.Search.assert_not_called()
            client.stub.GetCluster.assert_not_called()
            with self.assertRaises(ValueError):
                client.follower_search([1, 2], timeout=0)

    def test_request_presence_and_token(self):
        with Client('localhost:1', 'secret', timeout=4) as client:
            client.stub = Mock()
            client.stub.ResolveWriteRoute.return_value = messages.WriteRoute()
            client.insert('a', [1,2], request_id='insert')
            request = client.stub.Put.call_args.args[0]
            self.assertTrue(request.HasField('expected_revision'))
            self.assertEqual(request.expected_revision, 0)
            self.assertEqual(client.stub.Put.call_args.kwargs['metadata'], (('authorization','Bearer secret'),))
            client.put('a', [1,2], request_id='upsert')
            self.assertFalse(client.stub.Put.call_args.args[0].HasField('expected_revision'))
            with self.assertRaises(ValueError):
                client.update('a', [1], revision=0, request_id='bad')
            with self.assertRaises(ValueError):
                client.get('a', timeout=0)

    def test_direct_writes_share_route_and_refresh_a_stale_epoch(self):
        class Stale(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.FAILED_PRECONDITION

        with Client('localhost:1', 'secret') as client:
            client.stub = Mock()
            first = messages.WriteRoute(partitions=1, endpoint='worker:1000', epoch=1, session='a')
            second = messages.WriteRoute(partitions=1, endpoint='worker:2000', epoch=2, session='b')
            client.stub.ResolveWriteRoute.side_effect = [first, second]
            old, new = Mock(), Mock()
            old.Delete.side_effect = Stale()
            new.Delete.return_value = messages.MutationResponse(durable=True)
            with patch('vectorstore.VectorStoreStub', side_effect=[old, new]):
                client.put('a', [1], request_id='r1')
                response = client.delete('b', request_id='r2')
            self.assertTrue(response.durable)
            self.assertEqual(old.Put.call_args.args[0].route.epoch, 1)
            self.assertEqual(new.Delete.call_args.args[0].route.epoch, 2)
            self.assertEqual(new.Delete.call_args.args[0].request_id, 'r2')
            self.assertEqual(new.Delete.call_args.kwargs['metadata'], client.metadata)
            self.assertEqual(client.stub.ResolveWriteRoute.call_count, 2)
            client.stub.Put.assert_not_called()
            client.stub.Delete.assert_not_called()

    def test_iteration_continues_through_empty_page(self):
        with Client('localhost:1','secret') as client:
            client.stub = Mock()
            client.stub.List.side_effect = [messages.ListResponse(next_cursor='next'),
                messages.ListResponse(records=[messages.Record(id='a')])]
            self.assertEqual([r.id for r in client.iter_records()], ['a'])
            self.assertEqual(client.stub.List.call_args.args[0].cursor, 'next')

    def test_invalid_configuration(self):
        with self.assertRaises(ValueError):
            Client('localhost:1','',timeout=1)
        with self.assertRaises(ValueError):
            Client('localhost:1','token',root_certificates=b'cert')

    def test_changes_cursor_and_auth(self):
        with Client('localhost:1','secret',timeout=4) as client:
            client.stub = Mock()
            client.changes('index-0000', stream='graph', after=9007199254740993, limit=7)
            call = client.stub.ReadChanges.call_args
            self.assertEqual(call.args[0].after, 9007199254740993)
            self.assertEqual(call.args[0].stream, 'graph')
            self.assertEqual(call.kwargs['metadata'], (('authorization','Bearer secret'),))

    def test_consumer_epoch_and_ack(self):
        with Client('localhost:1','secret') as client:
            client.stub = Mock()
            client.consumer('index-0000', stream='graph', action='ack', consumer='sink',
                            session='session-123456789', epoch=7, ack=9007199254740993)
            request = client.stub.ManageConsumer.call_args.args[0]
            self.assertEqual(request.epoch, 7)
            self.assertEqual(request.ack, 9007199254740993)
            self.assertEqual(request.action, 'ack')
