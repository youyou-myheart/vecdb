package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRawData stores each RawKV key independently and clones data across the
// client boundary. Hooks run outside mu so tests can hold an old RPC while a
// successor publishes, or leave a prefix written when a batch fails.
type fakeRawData struct {
	mu   sync.Mutex
	data map[string][]byte

	beforeBatch func(context.Context, [][]byte, [][]byte) (int, error)
	afterBatch  func([][]byte)
	beforeCAS   func(context.Context, []byte, []byte, []byte) error
}

func newFakeRawData() *fakeRawData { return &fakeRawData{data: make(map[string][]byte)} }

func (f *fakeRawData) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return bytes.Clone(f.data[string(key)]), nil
}

func (f *fakeRawData) BatchPut(ctx context.Context, keys, values [][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) != len(values) {
		return errors.New("mismatched raw keys and values")
	}
	n := len(keys)
	var err error
	if f.beforeBatch != nil {
		n, err = f.beforeBatch(ctx, keys, values)
	}
	f.mu.Lock()
	for i := range n {
		f.data[string(keys[i])] = bytes.Clone(values[i])
	}
	f.mu.Unlock()
	if f.afterBatch != nil {
		f.afterBatch(keys[:n])
	}
	return err
}

func (f *fakeRawData) CompareAndSwap(ctx context.Context, key, expected, next []byte) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if f.beforeCAS != nil {
		if err := f.beforeCAS(ctx, key, expected, next); err != nil {
			return nil, false, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	prev, exists := f.data[string(key)]
	if exists != (expected != nil) || !bytes.Equal(prev, expected) {
		return bytes.Clone(prev), false, nil
	}
	f.data[string(key)] = bytes.Clone(next)
	return bytes.Clone(prev), true, nil
}

func (f *fakeRawData) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = bytes.Clone(f.data[string(key)])
	}
	return values, nil
}

func (f *fakeRawData) Close() error { return nil }

const rawTestNamespace = "raw-test/"

func rawTestRepo(shard int) *Repository {
	return &Repository{Prefix: fmt.Sprintf("%sdata/%04d/", rawTestNamespace, shard), Dimension: 2}
}

func rawTestOwner(shard DataPartitionID, epoch uint64) Ownership {
	return Ownership{Partition: sourceID(shard), Node: "node", Session: fmt.Sprintf("session-%d", epoch), Epoch: epoch}
}

func rawTestRecover(t *testing.T, ctx context.Context, store *rawDataStore, repo *Repository, owner Ownership) *rawShardWriter {
	t.Helper()
	writer, err := store.recover(ctx, repo.Prefix, owner)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func rawTestRequest(t *testing.T, repo *Repository, id, request string) *mutationRequest {
	t.Helper()
	return preparedRequest(t, repo, id, Mutation{RequestID: request, Vector: []float64{1, 2}}, false)
}

func rawTestAccept(t *testing.T, ctx context.Context, writer *rawShardWriter, req *mutationRequest) {
	t.Helper()
	results, batch, err := writer.prepare([]*mutationRequest{req})
	if err != nil || len(results) != 1 || len(batch.Entries) != 1 || results[0].err != nil || results[0].record.Revision != 1 {
		t.Fatal("bad raw prepare", results, batch, err)
	}
	if _, err := writer.write(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := writer.accept(batch); err != nil {
		t.Fatal(err)
	}
}

func TestRawShardReceiptsReplayAndConflict(t *testing.T) {
	ctx := context.Background()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	a, b := rawTestRepo(0), rawTestRepo(1)
	wa := rawTestRecover(t, ctx, store, a, rawTestOwner(0, 1))
	wb := rawTestRecover(t, ctx, store, b, rawTestOwner(1, 1))
	first := rawTestRequest(t, a, "alice", "shared")
	rawTestAccept(t, ctx, wa, first)
	rawTestAccept(t, ctx, wb, rawTestRequest(t, b, "bob", "shared"))
	results, batch, err := wa.prepare([]*mutationRequest{first})
	if err != nil || len(results) != 1 || len(batch.Entries) != 0 || results[0].err != nil || results[0].record.Revision != 1 {
		t.Fatal("replay allocated a sequence or changed the original receipt", results, batch, err)
	}
	conflict := rawTestRequest(t, a, "alex", "shared")
	results, batch, err = wa.prepare([]*mutationRequest{conflict})
	if err != nil || len(results) != 1 || len(batch.Entries) != 0 || !errors.Is(results[0].err, ErrConflict) {
		t.Fatal("same-shard request ID could be used for a different mutation", results, batch, err)
	}
}

func TestRawWriteFailureDoesNotPublishWithoutAnEnvelope(t *testing.T) {
	ctx := context.Background()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo := rawTestRepo(0)
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	_, batch, err := writer.prepare([]*mutationRequest{
		rawTestRequest(t, repo, "alice", "one"),
		rawTestRequest(t, repo, "bob", "two"),
	})
	if err != nil || len(batch.Entries) != 2 {
		t.Fatal(batch, err)
	}
	client.beforeBatch = func(_ context.Context, keys, _ [][]byte) (int, error) {
		if len(keys) != 1 {
			t.Fatalf("one logical batch must be one immutable envelope, got %d keys", len(keys))
		}
		return 0, injected
	}
	if _, err := writer.write(ctx, batch); !errors.Is(err, injected) {
		t.Fatal("failed BatchPut was acknowledged", err)
	}
	successorStore := newRawDataStore(client, rawTestNamespace)
	rawTestRecover(t, ctx, successorStore, repo, rawTestOwner(0, 2))
	snap, err := successorStore.snapshot(ctx, repo.Prefix)
	if err != nil || snap.end != 0 {
		t.Fatal("unwritten batch survived recovery", snap, err)
	}
}

func TestRawBatchAndReplayNeedNoPublicationIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := newFakeRawData()
	probe := &rawSnapshotProbe{rawDataClient: client}
	store := newRawDataStore(probe, rawTestNamespace)
	repo, owner := rawTestRepo(0), rawTestOwner(0, 1)
	probe.headKey = string(store.shard(repo.Prefix).headKey())
	group := newMutationGroup(ctx, repo, &pipelineMetrics{}, string(owner.Partition))
	group.lease = &appendLease{owner: owner}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.rawWriter = rawTestRecover(t, ctx, store, repo, owner)
	t.Cleanup(func() { cancel(); group.close() })
	store.local.Store(repo.Prefix, group)
	probe.heads, probe.batches = 0, 0
	var calls, writes atomic.Int64
	client.beforeBatch = func(_ context.Context, keys, values [][]byte) (int, error) {
		writes.Add(1)
		return len(keys), nil
	}
	client.beforeCAS = func(context.Context, []byte, []byte, []byte) error {
		calls.Add(1)
		return injected
	}
	mutation := Mutation{RequestID: "one", Vector: []float64{1, 2}}
	record, err := group.mutate(ctx, "alice", mutation, false)
	if err != nil || record.Revision != 1 || group.lease.head.Load() != 1 {
		t.Fatal(record, err)
	}
	if replay, err := group.mutate(ctx, "alice", mutation, false); err != nil || replay.Revision != record.Revision {
		t.Fatal(replay, err)
	}
	if snapshot, ok := store.localSnapshot(repo.Prefix); !ok || snapshot.end != 1 {
		t.Fatal("missing confirmed owner view")
	}
	if calls.Load() != 0 || writes.Load() != 1 || probe.heads != 0 || probe.batches != 0 {
		t.Fatalf("one write and replay: %d BatchPut, %d CAS, %d head reads, %d WAL reads", writes.Load(), calls.Load(), probe.heads, probe.batches)
	}
	group.close()
	client.beforeCAS = nil
	client.beforeBatch = nil
	if _, ok := store.localSnapshot(repo.Prefix); ok {
		t.Fatal("retired owner served a read")
	}

	successorStore := newRawDataStore(client, rawTestNamespace)
	successor := rawTestRecover(t, ctx, successorStore, repo, rawTestOwner(0, 2))
	snap, err := successorStore.snapshot(ctx, repo.Prefix)
	if err != nil || snap.end != 1 {
		t.Fatal("accepted prefix was lost during takeover", snap, err)
	}
	req := preparedRequest(t, repo, "alice", mutation, false)
	results, batch, err := successor.prepare([]*mutationRequest{req})
	if err != nil || len(results) != 1 || len(batch.Entries) != 0 || results[0].err != nil || results[0].record.Revision != 1 {
		t.Fatal("successor did not recover the accepted receipt", results, batch, err)
	}
}

func TestRawLateOldBatchCannotChangeSealedPrefix(t *testing.T) {
	ctx := context.Background()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo := rawTestRepo(0)
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	rawTestAccept(t, ctx, writer, rawTestRequest(t, repo, "alice", "one"))
	_, late, err := writer.prepare([]*mutationRequest{rawTestRequest(t, repo, "late", "late")})
	if err != nil {
		t.Fatal(err)
	}
	next := newRawDataStore(client, rawTestNamespace)
	successor := rawTestRecover(t, ctx, next, repo, rawTestOwner(0, 2))
	if _, err := writer.write(ctx, late); err != nil {
		t.Fatal(err)
	}
	rawTestAccept(t, ctx, successor, rawTestRequest(t, repo, "bob", "two"))
	snapshot, err := next.snapshot(ctx, repo.Prefix)
	if err != nil || snapshot.end != 2 {
		t.Fatal(snapshot, err)
	}
	if _, found, err := snapshot.record("late"); found || err != nil {
		t.Fatal("late old write entered successor history")
	}
}

func TestRawGroupRejectsDurableSuffixAfterFailedPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo, owner := rawTestRepo(0), rawTestOwner(0, 1)
	first := store.shard(repo.Prefix).batchKey(owner.Epoch, 1)
	second := store.shard(repo.Prefix).batchKey(owner.Epoch, 2)
	entered, fail, suffixStored := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	client.beforeBatch = func(ctx context.Context, keys, _ [][]byte) (int, error) {
		if len(keys) == 1 && bytes.Equal(keys[0], first) {
			close(entered)
			select {
			case <-fail:
				return 0, injected
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		return len(keys), nil
	}
	client.afterBatch = func(keys [][]byte) {
		if len(keys) == 1 && bytes.Equal(keys[0], second) {
			suffixStored <- struct{}{}
		}
	}
	group := newMutationGroup(ctx, repo, &pipelineMetrics{}, string(owner.Partition))
	group.lease = &appendLease{owner: owner}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.rawWriter = rawTestRecover(t, ctx, store, repo, owner)
	t.Cleanup(func() { cancel(); group.close() })
	submit := func(id string) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := group.mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)
			result <- err
		}()
		return result
	}
	prefixResult := submit("first")
	receiveConsumer(t, entered)
	suffixResult := submit("second")
	receiveConsumer(t, suffixStored)
	select {
	case err := <-suffixResult:
		t.Fatal("published a later batch before the prefix was durable", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(fail)
	if err := receiveConsumer(t, prefixResult); !errors.Is(err, injected) {
		t.Fatal("missing BatchPut failure for the prefix", err)
	}
	if err := receiveConsumer(t, suffixResult); !errors.Is(err, injected) {
		t.Fatal("accepted a durable suffix after the prefix failed", err)
	}
	next := newRawDataStore(client, rawTestNamespace)
	rawTestRecover(t, ctx, next, repo, rawTestOwner(0, 2))
	if snapshot, err := next.snapshot(ctx, repo.Prefix); err != nil || snapshot.end != 0 {
		t.Fatal("recovered the suffix despite the failed prefix", snapshot, err)
	}
}

func TestRawGroupAcknowledgesConfirmedPrefixOfPartialBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo, owner := rawTestRepo(0), rawTestOwner(0, 1)
	group := newMutationGroup(ctx, repo, &pipelineMetrics{}, string(owner.Partition))
	group.lease = &appendLease{owner: owner}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.rawWriter = rawTestRecover(t, ctx, store, repo, owner)
	group.rawCtx, group.rawStop = context.WithCancel(ctx)
	defer group.close()

	requests := []*mutationRequest{
		rawTestRequest(t, repo, "first", "one"),
		rawTestRequest(t, repo, "second", "two"),
		rawTestRequest(t, repo, "third", "three"),
	}
	for _, req := range requests {
		req.ctx, req.result = ctx, make(chan mutationResult, 1)
	}
	work, err := group.prepareRawWrite(requests)
	if err != nil {
		t.Fatal(err)
	}
	client.beforeBatch = func(_ context.Context, keys, values [][]byte) (int, error) {
		if len(keys) != 3 {
			t.Fatalf("expected three independent keys, got %d", len(keys))
		}
		client.mu.Lock()
		client.data[string(keys[2])] = bytes.Clone(values[2])
		client.mu.Unlock()
		return 1, injected // The first and third keys reached storage.
	}
	group.writeRawBatch(ctx, work)
	err = group.confirmRawWrite(ctx, owner.Epoch, work)
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	group.failRawWrites(err, nil, []*rawPendingWrite{work})
	for i, req := range requests {
		result := receiveConsumer(t, req.result)
		if i == 0 && (result.err != nil || result.record.Revision != 1) {
			t.Fatal("durable prefix was not acknowledged", result)
		}
		if i != 0 && !errors.Is(result.err, injected) {
			t.Fatal("request beyond the gap was acknowledged", result)
		}
	}
	client.beforeBatch = nil
	next := newRawDataStore(client, rawTestNamespace)
	rawTestRecover(t, ctx, next, repo, rawTestOwner(0, 2))
	if snap, err := next.snapshot(ctx, repo.Prefix); err != nil || snap.end != 1 {
		t.Fatal("takeover did not recover exactly the contiguous prefix", snap, err)
	}
}

func TestRawGroupLimitsConcurrentWritesAndBatchSize(t *testing.T) {
	// Requests beyond the write window stay queued until a slot frees up.
	const slots, requests = rawWritesInFlight, 1 + (rawWritesInFlight+1)*mutationGroupRecords
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := newFakeRawData()
	enter := make(chan int, requests)
	release := make(chan struct{})
	var active, peak atomic.Int32
	client.beforeBatch = func(ctx context.Context, keys, values [][]byte) (int, error) {
		if len(keys) < 1 || len(keys) > mutationGroupRecords || len(keys) != len(values) {
			return 0, fmt.Errorf("raw batch had %d keys and %d values", len(keys), len(values))
		}
		for _, value := range values {
			var slot mutationLogBatch
			if err := json.Unmarshal(value, &slot); err != nil {
				return 0, err
			}
			if len(slot.Entries) != 1 {
				return 0, fmt.Errorf("raw slot had %d entries", len(slot.Entries))
			}
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); old < n && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		enter <- len(keys)
		select {
		case <-release:
			return len(keys), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	store := newRawDataStore(client, rawTestNamespace)
	repo, owner := rawTestRepo(0), rawTestOwner(0, 1)
	group := newMutationGroup(ctx, repo, &pipelineMetrics{}, string(owner.Partition))
	group.lease = &appendLease{owner: owner}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.rawWriter = rawTestRecover(t, ctx, store, repo, owner)
	t.Cleanup(func() { cancel(); group.close() })
	results := make(chan error, requests)
	submit := func(i int) {
		go func() {
			id := fmt.Sprint(i)
			_, err := group.mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)
			results <- err
		}()
	}
	submit(0)
	if size := receiveConsumer(t, enter); size != 1 {
		t.Fatal("first raw write was not immediately submitted on its own", size)
	}
	for i := 1; i < requests; i++ {
		submit(i)
	}
	for i := 1; i < slots; i++ {
		receiveConsumer(t, enter)
	}
	select {
	case <-enter:
		t.Fatal("exceeded the concurrent RawKV write limit")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	for range requests {
		if err := receiveConsumer(t, results); err != nil {
			t.Fatal(err)
		}
	}
	if got := peak.Load(); got != slots {
		t.Fatal("unexpected RawKV write concurrency", got)
	}
	snapshot, err := store.snapshot(ctx, repo.Prefix)
	if err != nil || snapshot.end != requests {
		t.Fatal("raw writes did not leave a contiguous accepted prefix", snapshot, err)
	}
}
