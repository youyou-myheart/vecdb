package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	next := state(2)
	if err := repo.recoverDataWAL(ctx, next); err != nil || next.head.Load() != 2 {
		t.Fatal("recover WAL", next.head.Load(), err)
	}
	results, err := repo.proposeDataBatch(ctx, []*mutationRequest{a, b, rawTestRequest(t, &repo, "same-shard", "three")}, next)
	if err != nil || results[0].record.Revision != 1 || results[1].record.Revision != 1 || next.head.Load() != 3 {
		t.Fatal(results, next.head.Load(), err)
	}
	resumed := &dataDispatcher{owner: f.source}
	if err := f.c.loadDispatchWindow(ctx, 0, resumed); err != nil || resumed.persisted != 2 || len(resumed.window) != 1 || resumed.window[0].Sequence != 3 {
		t.Fatal("cursor on restart", resumed.persisted, resumed.window, err)
	}
	if wal.Load() != 2 || commits.Load() != 0 {
		t.Fatal("WAL one-phase commits / separate commits", wal.Load(), commits.Load())
	}
	view := repo.dataLog(nil, pinned, nil, true)
	if page, err := view.ReadCommitted(ctx, shardlog.Window{After: 1, Limit: 8}); err != nil || page.Head != 2 || len(page.Events) != 1 || page.Events[0].Record.ID != "east" {
		t.Fatal(page, err)
	}
	// Simulate a certified prefix. Keep the tail anchor and records/receipts.
	if err := func() error {
		gc, err := repo.begin()
		if err != nil {
			return err
		}
		defer gc.Rollback()
		end, err := repo.pruneDataBatches(f.ctx, gc, 3, 64)
		if err != nil || end != 3 {
			return fmt.Errorf("prune end %d: %v", end, err)
		}
		return gc.Commit(f.ctx)
	}(); err != nil {
		t.Fatal(err)
	}
	probe, err := repo.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Rollback()
	if _, err := probe.Get(ctx, dataWAL(repo.Prefix).Key(1)); !tikverr.IsErrNotFound(err) {
		t.Fatal("old batch retained", err)
	}
	if _, err := probe.Get(ctx, dataWAL(repo.Prefix).Key(2)); err != nil {
		t.Fatal("tail lost", err)
	}
	// Same shard rejects conflicting reuse. A second shard may use the same id.
	results, err = repo.proposeDataBatch(ctx, []*mutationRequest{rawTestRequest(t, &repo, "different", "one")}, next)
	if err != nil || !errors.Is(results[0].err, ErrConflict) {
		t.Fatal(results, err)
	}
	other := repo
	other.Prefix = f.r.Prefix + "data/0001/"
	results, err = other.proposeDataBatch(ctx, []*mutationRequest{rawTestRequest(t, &other, "new", "one")}, state(1))
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
	confirmAppendGrant(t, stale, 1, time.Now().Add(time.Minute).UnixMilli(), time.Minute)
	if _, err := r.proposeDataBatch(f.ctx, []*mutationRequest{rawTestRequest(t, &r, "x", "other")}, stale); !errors.Is(err, ErrFenced) || errors.Is(err, ErrConflict) {
		t.Fatal("occupied WAL slot reported as a semantic conflict", err)
	}
	results, err := r.proposeDataBatch(f.ctx, batch, next)
	if err != nil || results[0].record.Revision != 1 || next.batch != 1 || next.head.Load() != 1 {
		t.Fatal("lost reply produced another event", results, next.batch, err)
	}
}
