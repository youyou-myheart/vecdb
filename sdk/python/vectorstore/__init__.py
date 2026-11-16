"""Typed gRPC SDK. Responses are generated Protobuf messages; errors are grpc.RpcError."""
import grpc
from .v1 import vector_pb2 as messages
from .v1.vector_pb2_grpc import VectorStoreStub


class Client:
    def __init__(self, target, token, *, timeout=30.0, tls=False, root_certificates=None):
        if not target or not token or timeout <= 0:
            raise ValueError('target, token and positive timeout required')
        if root_certificates is not None and not tls:
            raise ValueError('root_certificates requires tls=True')
        options = [('grpc.max_receive_message_length', 64 << 20),
                   ('grpc.max_send_message_length', 1 << 20),
                   ('grpc.enable_retries', 0)]
        self.channel = (grpc.secure_channel(target, grpc.ssl_channel_credentials(root_certificates), options=options)
                        if tls else grpc.insecure_channel(target, options=options))
        self.stub = VectorStoreStub(self.channel)
        self.timeout = timeout
        self.metadata = (('authorization', 'Bearer ' + token),)

    def close(self):
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _call(self, method, request, timeout=None):
        duration = self.timeout if timeout is None else timeout
        if duration <= 0:
            raise ValueError('timeout must be positive')
        return method(request, timeout=duration, metadata=self.metadata)

    def put(self, id, vector, *, request_id, payload=None, expected_revision=None, timeout=None):
        request = messages.PutRequest(id=id, vector=vector, payload=payload or {}, request_id=request_id)
        if expected_revision is not None:
            request.expected_revision = expected_revision
        return self._call(self.stub.Put, request, timeout)

    def insert(self, id, vector, *, request_id, payload=None, timeout=None):
        return self.put(id, vector, request_id=request_id, payload=payload, expected_revision=0, timeout=timeout)

    def update(self, id, vector, *, revision, request_id, payload=None, timeout=None):
        if revision <= 0:
            raise ValueError('update revision must be positive')
        return self.put(id, vector, request_id=request_id, payload=payload, expected_revision=revision, timeout=timeout)

    def get(self, id, *, timeout=None):
        return self._call(self.stub.Get, messages.GetRequest(id=id), timeout)

    def delete(self, id, *, request_id, expected_revision=None, timeout=None):
        request = messages.DeleteRequest(id=id, request_id=request_id)
        if expected_revision is not None:
            request.expected_revision = expected_revision
        return self._call(self.stub.Delete, request, timeout)

    def list(self, *, limit=100, cursor='', include_deleted=False, timeout=None):
        return self._call(self.stub.List, messages.ListRequest(limit=limit, cursor=cursor, include_deleted=include_deleted), timeout)

    def iter_records(self, *, limit=100, include_deleted=False):
        cursor = ''
        while True:
            page = self.list(limit=limit, cursor=cursor, include_deleted=include_deleted)
            yield from page.records
            cursor = page.next_cursor
            if not cursor:
                return

    def search(self, vector, *, k=10, filter=None, exact=False, ef_search=0, min_visible=None, timeout=None):
        return self._call(self.stub.Search, messages.SearchRequest(vector=vector, k=k, filter=filter or {},
            exact=exact, ef_search=ef_search, min_visible=min_visible), timeout)

    def health(self, *, timeout=None):
        return self._call(self.stub.Health, messages.Empty(), timeout)

    def cluster(self, *, timeout=None):
        return self._call(self.stub.GetCluster, messages.Empty(), timeout)

    def layout(self, *, timeout=None):
        return self._call(self.stub.GetLayout, messages.Empty(), timeout)

    def checkpoint(self, *, timeout=None):
        return self._call(self.stub.Checkpoint, messages.Empty(), timeout)

    def rebuild(self, *, timeout=None):
        return self._call(self.stub.Rebuild, messages.Empty(), timeout)

    def prune(self, *, timeout=None):
        return self._call(self.stub.Prune, messages.Empty(), timeout)

    def changes(self, partition, *, stream='data', after=0, limit=100, timeout=None):
        """One bounded page. Persist next after processing; handle reset_required explicitly."""
        return self._call(self.stub.ReadChanges, messages.ChangeRequest(
            partition=partition, stream=stream, after=after, limit=limit), timeout)

    def consumer(self, partition, *, stream='data', action='list', consumer='', session='',
                 epoch=0, ack=0, ttl_seconds=60, timeout=None):
        """Acquire/renew/ACK/release retention. Use a fresh session after expiry."""
        return self._call(self.stub.ManageConsumer, messages.ConsumerRequest(
            partition=partition, stream=stream, action=action, consumer=consumer,
            session=session, epoch=epoch, ack=ack, ttl_seconds=ttl_seconds), timeout)
