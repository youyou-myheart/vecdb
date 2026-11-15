import unittest
from unittest.mock import Mock
from vectorstore import Client, messages

class ClientTests(unittest.TestCase):
    def test_request_presence_and_token(self):
        with Client('localhost:1', 'secret', timeout=4) as client:
            client.stub = Mock()
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
