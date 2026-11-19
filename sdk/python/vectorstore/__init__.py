"""Typed gRPC SDK. Responses are generated Protobuf messages; errors are grpc.RpcError."""
import hashlib
import math
import threading
import time

import grpc
from .v1 import vector_pb2 as messages
from .v1.vector_pb2_grpc import VectorStoreStub
from .query_route import ranked_partitions, initial_fanout


class Client:
    def __init__(self, target, token, *, timeout=30.0, tls=False, root_certificates=None, search_refresh_interval=5.0):
        if not target or not token or timeout <= 0:
            raise ValueError('target, token and positive timeout required')
        if root_certificates is not None and not tls:
            raise ValueError('root_certificates requires tls=True')
        if not math.isfinite(search_refresh_interval) or search_refresh_interval <= 0:
            raise ValueError('search_refresh_interval must be positive')
        options = [('grpc.max_receive_message_length', 64 << 20),
                   ('grpc.max_send_message_length', 1 << 20),
                   ('grpc.enable_retries', 0)]
        credentials = grpc.ssl_channel_credentials(root_certificates) if tls else None
        self._channel = lambda address: (grpc.secure_channel(address, credentials, options=options)
                                         if tls else grpc.insecure_channel(address, options=options))
        self.channel = self._channel(target)
        self._lock = threading.Lock()
        self._routes = {}
        self._partitions = 0
        self._proxy_only = False
        self._closed = False
        self.stub = VectorStoreStub(self.channel)
        self.timeout = timeout
        self.metadata = (('authorization', 'Bearer ' + token),)
        self._search_refresh_interval = search_refresh_interval
        self._search_snapshot = None
        self._search_refresh_lock = threading.Lock()
        self._search_stop = threading.Event()
        self._search_thread = None

    def close(self):
        self._search_stop.set()
        with self._lock:
            self._closed = True
            thread = self._search_thread
        if thread is not None:
            thread.join()
        with self._lock:
            self._closed = True
            for entry in self._routes.values():
                entry.channel.close()
            self._routes.clear()
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

    def _acquire_route(self, id, timeout):
        number = int.from_bytes(hashlib.sha256(id.encode()).digest()[:8], 'big')
        with self._lock:
            if self._closed:
                raise ValueError('client is closed')
            if self._proxy_only:
                return None
            entry = self._routes.get(number % self._partitions) if self._partitions else None
            if entry:
                entry.users += 1
                return entry
        try:
            hint = self._call(self.stub.ResolveWriteRoute, messages.GetRequest(id=id), timeout)
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.UNIMPLEMENTED:
                raise
            hint = messages.WriteRoute()
        with self._lock:
            if self._closed:
                raise ValueError('client is closed')
            if not hint.endpoint:
                self._proxy_only = hint.partitions == 0
                return None
            if not 0 < hint.partitions <= 64 or hint.partition != number % hint.partitions:
                raise ValueError('invalid data write route')
            self._partitions = hint.partitions
            entry = self._routes.get(hint.partition)
            if entry is None:
                entry = _Route(hint, self._channel(hint.endpoint))
                self._routes[hint.partition] = entry
            entry.users += 1
            return entry

    def _write(self, method, request, timeout):
        duration = self.timeout if timeout is None else timeout
        if duration <= 0:
            raise ValueError('timeout must be positive')
        deadline = time.monotonic() + duration
        for attempt in range(2):
            entry = self._acquire_route(request.id, max(deadline - time.monotonic(), 1e-9))
            remaining = max(deadline - time.monotonic(), 1e-9)
            if entry is None:
                request.ClearField('route')
                return self._call(getattr(self.stub, method), request, remaining)
            request.route.CopyFrom(entry.hint)
            try:
                return self._call(getattr(entry.stub, method), request, remaining)
            except grpc.RpcError as error:
                if error.code() in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    with self._lock:
                        if self._routes.get(entry.hint.partition) is entry:
                            del self._routes[entry.hint.partition]
                        entry.retired = True
                if attempt or error.code() != grpc.StatusCode.FAILED_PRECONDITION:
                    raise
            finally:
                with self._lock:
                    entry.users -= 1
                    if entry.retired and not entry.users:
                        entry.channel.close()

    def put(self, id, vector, *, request_id, payload=None, expected_revision=None, timeout=None):
        request = messages.PutRequest(id=id, vector=vector, payload=payload or {}, request_id=request_id)
        if expected_revision is not None:
            request.expected_revision = expected_revision
        return self._write('Put', request, timeout)

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
        return self._write('Delete', request, timeout)

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

    def _refresh_search_routes(self, timeout):
        try:
            state = self._call(self.stub.GetSearchRoutes, messages.Empty(), min(timeout, 2.0))
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.UNIMPLEMENTED:
                return
            try:
                state = self._call(self.stub.GetCluster, messages.Empty(), min(timeout, 2.0))
            except grpc.RpcError:
                return
        if isinstance(state, messages.ClusterStatus):
            with self._lock:
                self._search_snapshot = (state, time.monotonic())

    def _search_refresh_loop(self):
        while not self._search_stop.wait(self._search_refresh_interval):
            self._refresh_search_routes(self.timeout)

    def _search_routes(self, vector, timeout):
        with self._search_refresh_lock:
            with self._lock:
                if self._closed:
                    raise ValueError('client is closed')
                first = self._search_thread is None
            if first:
                self._refresh_search_routes(timeout)
                with self._lock:
                    if not self._closed:
                        self._search_thread = threading.Thread(target=self._search_refresh_loop, daemon=True)
                        self._search_thread.start()
            with self._lock:
                snapshot = self._search_snapshot
        if snapshot is None or time.monotonic()-snapshot[1] > max(30, 3*self._search_refresh_interval):
            return []
        return ranked_partitions(snapshot[0], vector)

    def search(self, vector, *, k=10, filter=None, exact=False, ef_search=0, min_visible=None,
               timeout=None, all_partitions=False, index_partitions=None):
        """Search an eventual index view; nearest clusters are approximate.

        exact=True/all_partitions=True scans all shards. No candidate TiKV reads.
        Explicit index_partitions restricts exactness to those partitions.
        """
        duration = self.timeout if timeout is None else timeout
        if duration <= 0:
            raise ValueError('timeout must be positive')
        deadline = time.monotonic()+duration
        request = messages.SearchRequest(vector=vector, k=k, filter=filter or {},
            exact=exact, ef_search=ef_search, min_visible=min_visible,
            index_partitions=index_partitions or [])
        ranked = []
        if not (exact or all_partitions or min_visible or index_partitions):
            ranked = self._search_routes(vector, duration)
        count = initial_fanout(ranked, k)
        while True:
            if ranked:
                del request.index_partitions[:]
                request.index_partitions.extend(row[1] for row in ranked[:count])
            result = self._call(self.stub.Search, request, max(deadline-time.monotonic(), 1e-9))
            if not ranked or len(result.hits) >= k or count == len(ranked):
                return result
            count += 1  # Filters, moves and stale counts can reduce actual hits.

    def search_by_id(self, id, *, timeout=None, **options):
        """Read A's vector through Get (TiKV), then route a similarity search."""
        duration = self.timeout if timeout is None else timeout
        deadline = time.monotonic()+duration
        record = self.get(id, timeout=duration)
        if record.deleted:
            raise ValueError('query record is deleted')
        return self.search(record.vector, timeout=max(deadline-time.monotonic(), 1e-9), **options)

    def health(self, *, timeout=None):
        return self._call(self.stub.Health, messages.Empty(), timeout)

    def follower_search(self, vector, *, k=10, filter=None, exact=False, ef_search=0, timeout=None):
        """Wait for a fresh leader applied position on a follower, then search.

        No leader fallback. follower_reads reports per-partition positions and
        queue/capture/apply-wait/query timings. The server caps calls at 5 seconds.
        This does not wait for writes still pending on the leader's input queue.
        """
        return self._call(self.stub.FollowerSearch, messages.SearchRequest(
            vector=vector, k=k, filter=filter or {}, exact=exact, ef_search=ef_search), timeout)

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

class _Route:
    def __init__(self, hint, channel):
        self.hint = hint
        self.channel = channel
        self.stub = VectorStoreStub(channel)
        self.users = 0
        self.retired = False
