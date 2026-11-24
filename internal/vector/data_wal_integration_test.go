package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"example.com/vector-storage/internal/shardlog"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

// Observe the real RPC reply, then model process death after the immutable
// slot was committed, before any physical records or receipts were written.
func TestDataWALTiKVRecoveryAndDispatch(t *testing.T) {
	f := newEpochTiKVFixture(t, 2)
	repo := *f.r
	repo.packed = &packedStore{namespace: f.r.Prefix, count: 2, client: f.r.Client}
	repo.Prefix += "data/0000/"
	repo.collectionPrefix = f.r.Prefix
	f.c.data[0] = &repo
	f.c.root.packed = repo.packed
	if ids, err := repo.Client.SplitRegions(f.ctx, [][]byte{[]byte(repo.Prefix + "record/")}, false, nil); err != nil || len(ids) == 0 {
		t.Fatal("split record/log regions", ids, err)
	}
	var wal, commits atomic.Int32
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			resp, err := send(target, req)
			if req.Type == tikvrpc.CmdCommit {
				commits.Add(1)
			}
			if req.Type == tikvrpc.CmdPrewrite && err == nil && resp != nil {
				p := req.Prewrite()
				if bytes.HasPrefix(p.PrimaryLock, []byte(repo.Prefix+"wal/")) && len(p.Mutations) == 1 && p.Mutations[0].Op == kvrpcpb.Op_Insert {
					if reply, ok := resp.Resp.(*kvrpcpb.PrewriteResponse); ok && reply.OnePcCommitTs > p.StartVersion {
						wal.Add(1)
					}
				}
			}
			return resp, err
		}
	})
	state := func(epoch uint64) *appendLease {
		s := &appendLease{owner: Ownership{Epoch: epoch}}
		// Deliberately separate the local grant from the storage clock: a
		// deadline leaking into the WAL prewrite would make 1PC impossible.
		confirmAppendGrant(t, s, epoch, 1, time.Minute)
		return s
	}
	first := state(1)
	a := preparedRequest(t, &repo, "west", Mutation{RequestID: "one", Vector: []float64{0, 0}}, false)
	b := preparedRequest(t, &repo, "east", Mutation{RequestID: "two", Vector: []float64{10, 0}}, false)
	batch := mutationLogBatch{Number: 1, First: 1}
	for _, q := range []*mutationRequest{a, b} {
		rec, err := q.nextRecord(0)
		if err != nil {
			t.Fatal(err)
		}
		batch.Entries = append(batch.Entries, mutationLogEntry{q.mutation.RequestID, q.digest, rec})
	}
	value, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	native, err := repo.Client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer native.Rollback()
	if err := repo.dataLog(&first.Node, tikvTransaction{native}, nil, false).Propose(ctx, 1, shardlog.Slot{Sequence: 1, Value: value}); err != nil {
		t.Fatal(err)
	}
	first.StepDown()
	// A reader with no local owner sees the whole batch at one timestamp.
	pinned, err := repo.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Rollback()
	if rec, err := repo.Get(ctx, "west"); err != nil || rec.Revision != 1 {
		t.Fatal(rec, err)
	}
	if rows, head, err := repo.Bootstrap(ctx); err != nil || head != 2 || len(rows) != 2 {
		t.Fatal(rows, head, err)
	}
	if v, err := batchGet(ctx, pinned, [][]byte{repo.key("request", "two"), repo.logKey(2)}); err != nil || len(v) != 2 {
		t.Fatal(v, err)
	}
	d := &dataDispatcher{owner: f.source}
	if err := f.c.loadDispatchWindow(ctx, 0, d); err != nil {
		t.Fatal(err)
	}
	d.projection.update(f.c, d.window)
	if len(d.projection.targets) != 2 || len(d.projection.byIndex["index-0000"]) != 1 || len(d.projection.byIndex["index-0001"]) != 1 {
		t.Fatal("wrong projected targets", d.projection.byIndex)
	}
	// Deliver through the normal Worker entry point. Lose the volatile
	// Dispatcher before recording its ACKs, then recover from both graph logs.
	f.c.node, f.c.cachedRouting = f.node, true
	f.c.locals = map[IndexPartitionID]*localPartition{}
	f.c.coordinator.data[0] = &repo
	for _, svc := range f.services {
		g := svc.graphReplica
		g.repo.packed = repo.packed
		f.c.locals[g.journal.owner.Partition] = &localPartition{owner: g.journal.owner, service: svc}
	}
	if err := f.c.dispatchSource(f.ctx, 0, d); err != nil {
		t.Fatal("dispatch committed overlay", err)
	}
	for n, svc := range f.services {
		if err := svc.Sync(f.ctx); err != nil {
			t.Fatal(err)
		}
		h, err := svc.graphReplica.journal.Head(f.ctx)
		if err != nil || h.Sources["0"] != uint64(n+1) {
			t.Fatal("index source ACK", n, h, err)
		}
	}
	recovered := &dataDispatcher{owner: f.source}
	if err := f.c.dispatchSource(f.ctx, 0, recovered); err != nil || recovered.completed != 2 {
		t.Fatal("recover target ACKs", recovered.completed, err)
	}
	if _, err := f.c.coordinator.update(ctx, f.source, true, false, func(o *Ownership) {
		o.Offset = recovered.completed
	}); err != nil {
		t.Fatal("report confirmed prefix with lease renewal", err)
	}
	next := state(2)
	if err := repo.recoverDataWAL(ctx, next); err != nil || next.head.Load() != 2 {
		t.Fatal("recover WAL", next.head.Load(), err)
	}
	results, err := repo.proposeDataBatch(ctx, []*mutationRequest{a, b, rawTestRequest(t, &repo, "same-shard", "three")}, next)
	if err != nil || results[0].record.Revision != 1 || results[1].record.Revision != 1 || next.head.Load() != 3 {
		t.Fatal(results, next.head.Load(), err)
	}
	resumed := &dataDispatcher{owner: f.source}
	if err := f.c.loadDispatchWindow(ctx, 0, resumed); err != nil || resumed.completed != 2 || len(resumed.window) != 1 || resumed.window[0].Sequence != 3 {
		t.Fatal("cursor on restart", resumed.completed, resumed.window, err)
	}
	if wal.Load() != 2 || commits.Load() != 0 {
		t.Fatal("WAL one-phase commits / separate commits", wal.Load(), commits.Load())
	}
	view := repo.dataLog(nil, pinned, nil, true)
	if page, err := view.ReadCommitted(ctx, shardlog.Window{After: 1, Limit: 8}); err != nil || page.Head != 2 || len(page.Events) != 1 || page.Events[0].Record.ID != "east" {
		t.Fatal(page, err)
	}
	// The durable WAL is the only write. Older batches remain available
	// even after several subsequent appends and with no physical projection.
	if floor, err := repo.PruneEvents(f.ctx, 64); err != nil || floor != 0 {
		t.Fatal(floor, err)
	}
	probe, err := repo.Client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Rollback()
	for _, key := range [][]byte{repo.logKey(1), repo.logKey(3), repo.key("record", "west"), repo.key("request", "three")} {
		if _, err := probe.Get(ctx, key); !tikverr.IsErrNotFound(err) {
			t.Fatalf("unexpected physical projection %q: %v", key, err)
		}
	}
	if rows, head, err := repo.Bootstrap(ctx); err != nil || head != 3 || len(rows) != 3 {
		t.Fatal(rows, head, err)
	}

	// Same shard rejects conflicting reuse. A second shard may use the same id.
	results, err = repo.proposeDataBatch(ctx, []*mutationRequest{rawTestRequest(t, &repo, "different", "one")}, next)
	if err != nil || !errors.Is(results[0].err, ErrConflict) {
		t.Fatal(results, err)
	}
	other := repo
	other.Prefix = f.r.Prefix + "data/0001/"
	otherWriter := state(1)
	if err := other.recoverDataWAL(ctx, otherWriter); err != nil {
		t.Fatal(err)
	}
	results, err = other.proposeDataBatch(ctx, []*mutationRequest{rawTestRequest(t, &other, "new", "one")}, otherWriter)
	if err != nil || results[0].record.Revision != 1 {
		t.Fatal(results, err)
	}
	if _, err := repo.proposeDataBatch(ctx, []*mutationRequest{a}, first); !errors.Is(err, ErrFenced) {
		t.Fatal("old owner wrote", err)
	}
}

// An offline protocol upgrade retains the old event tail and receipts. A
// request id previously used on one shard remains available on another.
func TestDataWALTiKVUpgradeWithLegacyReceipt(t *testing.T) {
	f := newEpochTiKVFixture(t, 0)
	old := *f.r
	old.collectionPrefix = f.r.Prefix
	old.Prefix += "data/0000/"
	request := Mutation{RequestID: "legacy", Vector: []float64{1, 2}}
	if rec, err := old.Mutate(f.ctx, "west", request, false); err != nil || rec.Revision != 1 {
		t.Fatal(rec, err)
	}
	r := old
	r.packed = &packedStore{namespace: f.r.Prefix, count: 2, client: r.Client}
	open := func(repo *Repository) *appendLease {
		state := &appendLease{owner: Ownership{Epoch: 2}}
		if err := repo.recoverDataWAL(f.ctx, state); err != nil {
			t.Fatal(err)
		}
		confirmAppendGrant(t, state, 2, time.Now().Add(time.Minute).UnixMilli(), time.Minute)
		return state
	}
	state := open(&r)
	if state.head.Load() != 1 || state.batch != 0 {
		t.Fatal("lost legacy high watermark", state.head.Load(), state.batch)
	}
	a := preparedRequest(t, &r, "west", request, false)
	conflict := preparedRequest(t, &r, "different", request, false)
	results, err := r.proposeDataBatch(f.ctx, []*mutationRequest{a, conflict}, state)
	if err != nil || results[0].record.Revision != 1 || !errors.Is(results[1].err, ErrConflict) || state.batch != 0 {
		t.Fatal(results, state.batch, err)
	}
	// Both writes to the same record are checked in admission order.
	results, err = r.proposeDataBatch(f.ctx, []*mutationRequest{rawTestRequest(t, &r, "west", "r2"), rawTestRequest(t, &r, "west", "r3")}, state)
	if err != nil || results[0].record.Revision != 2 || results[1].record.Revision != 3 || state.head.Load() != 3 || state.batch != 1 {
		t.Fatal(results, state.head.Load(), state.batch, err)
	}
	other := r
	other.Prefix = f.r.Prefix + "data/0001/"
	results, err = other.proposeDataBatch(f.ctx, []*mutationRequest{preparedRequest(t, &other, "new", request, false)}, open(&other))
	if err != nil || results[0].record.Revision != 1 {
		t.Fatal(results, err)
	}
}

func TestDataWALTiKVAmbiguousAppend(t *testing.T) {
	f := newEpochTiKVFixture(t, 0)
	r := *f.r
	r.Prefix += "data/0000/"
	r.packed = &packedStore{namespace: f.r.Prefix, count: 1, client: r.Client}
	acquire := func(epoch uint64) *appendLease {
		s := &appendLease{owner: Ownership{Epoch: epoch}}
		if err := r.recoverDataWAL(f.ctx, s); err != nil {
			t.Fatal(err)
		}
		confirmAppendGrant(t, s, epoch, time.Now().Add(time.Minute).UnixMilli(), time.Minute)
		return s
	}
	first := acquire(1)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	var dropped atomic.Bool
	ctx = interceptor.WithRPCInterceptor(ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			resp, err := send(target, req)
			if req.Type == tikvrpc.CmdPrewrite && bytes.Equal(req.Prewrite().PrimaryLock, dataWAL(r.Prefix).Key(1)) &&
				err == nil && resp != nil && resp.Resp.(*kvrpcpb.PrewriteResponse).OnePcCommitTs != 0 && dropped.CompareAndSwap(false, true) {
				cancel()
				return nil, context.Canceled
			}
			return resp, err
		}
	})
	batch := []*mutationRequest{rawTestRequest(t, &r, "v", "req")}
	if results, err := r.proposeDataBatch(ctx, batch, first); err == nil || !dropped.Load() || results[0].err == nil || !first.uncertain.Load() || first.InLeaderLease(nil) {
		t.Fatal("uncertain append acknowledged", results, err)
	}
	late, err := r.Client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer late.Rollback()
	next := acquire(2)
	// A request with a timestamp from before recovery cannot fill the
	// successor's vacant slot, even if it passed its local lease check earlier.
	if err := dataWAL(r.Prefix).Append(f.ctx, late, 2, []byte("stale")); err == nil {
		t.Fatal("old in-flight write bypassed takeover fence")
	}
	// A stale local cursor can encounter an occupied slot. This is a
	// route/recovery retry, never a business-level revision conflict.
	stale := &appendLease{owner: Ownership{Epoch: 1}}
	stale.wal = &dataWALMemory{state: mutationBatchState{records: map[string]uint64{}, receipts: map[string]receipt{}}}
	confirmAppendGrant(t, stale, 1, time.Now().Add(time.Minute).UnixMilli(), time.Minute)
	if _, err := r.proposeDataBatch(f.ctx, []*mutationRequest{rawTestRequest(t, &r, "x", "other")}, stale); !errors.Is(err, ErrFenced) || errors.Is(err, ErrConflict) {
		t.Fatal("occupied WAL slot reported as a semantic conflict", err)
	}
	results, err := r.proposeDataBatch(f.ctx, batch, next)
	if err != nil || results[0].record.Revision != 1 || next.batch != 1 || next.head.Load() != 1 {
		t.Fatal("lost reply produced another event", results, next.batch, err)
	}
}

// Exercise the first delivery to an index, including the source epoch
// transition, through the ordinary data-owner and dispatcher entry points.
func TestDataWALTiKVTwoAppendPipeline(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	c := f.c
	f.r.packed = &packedStore{namespace: f.r.Prefix, count: 1, client: f.r.Client}
	repo := *f.r
	repo.Prefix += "data/0000/"
	c.data[0] = &repo
	c.coordinator.holdReleasedLeases = true
	c.node, c.replicas, c.cachedRouting = f.node, 2, true
	c.token = "integration-delivery-token"
	c.outboundSignals = newSourceSignals()
	g := f.services[0].graphReplica
	g.repo.packed = repo.packed
	c.indexes = map[IndexPartitionID]PartitionIndex{g.journal.id: &remotePartition{cluster: c, id: g.journal.id}}
	c.locals = map[IndexPartitionID]*localPartition{g.journal.owner.Partition: {owner: g.journal.owner, service: f.services[0]}}
	enableNativeLease(t, f)
	if _, err := c.cachedSearchRoutes(f.ctx, true); err != nil {
		t.Fatal(err)
	}
	var data, graph, other atomic.Int32
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			resp, err := send(target, req)
			if req.Type != tikvrpc.CmdPrewrite {
				other.Add(1)
				return resp, err
			}
			p := req.Prewrite()
			if err != nil || resp == nil || !p.TryOnePc || len(p.Mutations) != 1 || p.Mutations[0].Op != kvrpcpb.Op_Insert ||
				resp.Resp.(*kvrpcpb.PrewriteResponse).OnePcCommitTs == 0 {
				other.Add(1)
			} else if bytes.HasPrefix(p.PrimaryLock, []byte(repo.Prefix+"wal/")) {
				data.Add(1)
			} else if bytes.HasPrefix(p.PrimaryLock, []byte(g.journal.stream())) {
				graph.Add(1)
			} else {
				other.Add(1)
			}
			return resp, err
		}
	})
	c.runCtx = ctx
	group, err := c.recoverWriter(f.ctx, 0, f.source)
	if err != nil {
		t.Fatal(err)
	}
	defer group.close()
	confirmAppendGrant(t, group.lease, f.source.Epoch, f.source.Expires, time.Minute)
	c.writers = map[DataPartitionID]*mutationGroup{0: group}
	dispatcher := &dataDispatcher{owner: f.source, writer: group}

	record, err := c.MutateOwned(ctx, c.root.Prefix, 0, f.source.Epoch, f.source.Session, "point", Mutation{RequestID: "request", Vector: []float64{1, 2}}, false)
	if err != nil || record.Revision != 1 || data.Load() != 1 || graph.Load() != 0 {
		t.Fatal(record, err, data.Load(), graph.Load())
	}
	if got, err := c.Get(ctx, "point"); err != nil || got.Revision != record.Revision {
		t.Fatal(got, err)
	}
	if err := c.dispatchSource(ctx, 0, dispatcher); err != nil {
		t.Fatal(err)
	}
	if err := f.services[0].Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Lose the first graph ACK and allow a retry of the admitted batch.
	group.proofs.mu.Lock()
	proof := group.proofs.byIndex[g.journal.id]
	proof.through, proof.admittedAt = 0, time.Time{}
	group.proofs.byIndex[g.journal.id] = proof
	group.proofs.mu.Unlock()
	for range 2 {
		if err := c.dispatchSource(ctx, 0, dispatcher); err != nil {
			t.Fatal(err)
		}
	}
	if data.Load() != 1 || graph.Load() != 1 || other.Load() != 0 || dispatcher.completed != 1 {
		t.Fatal("data / graph / other / completed", data.Load(), graph.Load(), other.Load(), dispatcher.completed)
	}
	h, err := g.journal.Head(f.ctx)
	if err != nil || h.Committed.Sequence != 1 || h.SourceEpochs["0"] != f.source.Epoch || h.Sources["0"] != 1 || g.position != h.Committed {
		t.Fatal(h, g.position, err)
	}
}
