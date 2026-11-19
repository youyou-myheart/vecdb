package vector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestTiKVMutationGroupsCompetingGateways(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV")
	}
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "group-test/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := newMutationGroup(ctx, r, &pipelineMetrics{}, "data-0000")
	b := newMutationGroup(ctx, r, &pipelineMetrics{}, "data-0000")
	defer a.close()
	defer b.close()
	groups := []*mutationGroup{a, b}
	run := func(mutation func(int) (string, Mutation)) []mutationResult {
		start := make(chan struct{})
		results := make([]mutationResult, 32)
		var wg sync.WaitGroup
		for n := range results {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				<-start
				id, m := mutation(n)
				results[n].record, results[n].err = groups[n%2].mutate(ctx, id, m, false)
			}(n)
		}
		close(start)
		wg.Wait()
		return results
	}
	zero := uint64(0)
	results := run(func(n int) (string, Mutation) {
		return "cas", Mutation{RequestID: fmt.Sprint(n), Vector: []float64{1, 2}, ExpectedRevision: &zero}
	})
	success := 0
	for _, result := range results {
		if result.err == nil {
			success++
		} else if !errors.Is(result.err, ErrConflict) {
			t.Fatal(result.err)
		}
	}
	if success != 1 {
		t.Fatal("multiple gateways passed one CAS", success)
	}
	results = run(func(int) (string, Mutation) {
		return "duplicate", Mutation{RequestID: "same-receipt", Vector: []float64{2, 3}}
	})
	for _, result := range results {
		if result.err != nil || result.record.Revision != 1 {
			t.Fatal(result)
		}
	}
	results = run(func(n int) (string, Mutation) {
		return fmt.Sprint(n), Mutation{RequestID: fmt.Sprintf("distinct-%d", n), Vector: []float64{3, 4}}
	})
	for _, result := range results {
		if result.err != nil || result.record.Revision != 1 {
			t.Fatal(result)
		}
	}
	log, err := r.ReadEvents(ctx, 0, 0, 256)
	if err != nil || log.Head != 34 || len(log.Events) != 34 {
		t.Fatal(log, err)
	}
	for n, event := range log.Events {
		if event.Sequence != uint64(n+1) {
			t.Fatal("noncontiguous LSN", event)
		}
	}
}

func TestTiKVDataAppendCommitRetryCannotCrossLease(t *testing.T) {
	f := newEpochTiKVFixture(t, 0)
	// Put the log and record/receipt on different Regions.
	if ids, err := f.r.Client.SplitRegions(f.ctx, [][]byte{[]byte(f.r.Prefix + "record/")}, false, nil); err != nil || len(ids) == 0 {
		t.Fatal("split write regions", ids, err)
	}
	f.c.runCtx = f.ctx
	coord := f.c.coordinator
	coord.holdReleasedLeases = true
	coord.ttl = 1500
	old, err := coord.Update(f.ctx, f.source, true, false)
	if err != nil {
		t.Fatal(err)
	}
	coord.ttl = 60000
	writer, err := f.c.recoverWriter(f.ctx, 0, old)
	if err != nil {
		t.Fatal(err)
	}
	writer.lease.grant.Store(&appendGrant{deadline: time.Now().Add(time.Second), expires: old.Expires})
	expiryTS := uint64(old.Expires) << 18
	q := preparedRequest(t, f.r, "delayed", Mutation{RequestID: "delayed", Vector: []float64{1, 2}}, false)
	var successor *mutationGroup
	commits := 0
	var prewrites sync.Map
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			switch req.Type {
			case tikvrpc.CmdPrewrite:
				p := req.Prewrite()
				prewrites.Store(req.Context.RegionId, struct{}{})
				if p.TryOnePc || p.UseAsyncCommit {
					t.Fatal("short commit path skipped the lease bound")
				}
				for _, mutation := range p.Mutations {
					if string(mutation.Key) == string(coord.writeGuardKey(old.Partition)) {
						t.Fatal("data batch still wrote the guard")
					}
				}
			case tikvrpc.CmdCommit:
				commits++
				commit := req.Commit()
				if commits == 2 {
					if commit.CommitVersion < expiryTS {
						t.Fatal("retry stayed below the expiry after takeover")
					}
					break // The actual lease interceptor must reject this RPC.
				}
				if commits != 1 || commit.CommitVersion >= expiryTS {
					t.Fatal("expected an initial commit timestamp inside the lease")
				}
				// The commit is delayed after all data keys were prewritten.
				// Let a new owner acquire and recover while those locks exist.
				var owner Ownership
				for {
					owner, err = coord.Acquire(f.ctx, old.Partition, f.node)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrConflict) || !waitIndexRetry(f.ctx, 25*time.Millisecond) {
						t.Fatal(err)
					}
				}
				successor, err = f.c.recoverWriter(f.ctx, 0, owner)
				if err != nil || successor.lease.head.Load() != 0 {
					t.Fatal("successor recovered an uncommitted append", err)
				}
				successor.lease.grant.Store(&appendGrant{deadline: time.Now().Add(time.Second), expires: owner.Expires})
				resp, err := next(target, req)
				if err != nil {
					t.Fatal(err)
				}
				actual, ok := resp.Resp.(*kvrpcpb.CommitResponse)
				if !ok || actual.GetError() == nil {
					t.Fatal("TiKV accepted a late commit after the successor read the tail")
				}
				// Exercise the retry even when recovery rolled back the lock
				// and TiKV returned a terminal error instead of CommitTsExpired.
				return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{CommitTsExpired: &kvrpcpb.CommitTsExpired{
						StartTs: commit.StartVersion, AttemptedCommitTs: commit.CommitVersion, MinCommitTs: expiryTS,
					}},
				}}, nil
			}
			return next(target, req)
		}
	})
	if _, err = f.r.appendGroup(ctx, []*mutationRequest{q}, writer.lease); err == nil ||
		!strings.Contains(err.Error(), "data writer lease expired before commit") || commits != 2 {
		t.Fatal("expected the commit RPC limit to reject the retry", err, commits)
	}
	regions := 0
	prewrites.Range(func(_, _ any) bool { regions++; return true })
	if regions < 2 {
		t.Fatal("expected a transaction spanning at least two Regions", regions)
	}
	if _, err = f.r.Get(f.ctx, "delayed"); !errors.Is(err, ErrNotFound) {
		t.Fatal("late append became visible", err)
	}
	if _, err = f.r.appendGroup(f.ctx, []*mutationRequest{q}, successor.lease); err != nil || successor.lease.head.Load() != 1 {
		t.Fatal(err, successor.lease.head.Load())
	}
}

// Both optimistic appenders choose LSN 1. Even after GC deletes the winning
// row, the loser must conflict, then restart at floor+1 in a fresh snapshot.
func TestTiKVDataLogAppendRacingCompaction(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV")
	}
	ctx := context.Background()
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "log-tail/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Client.Close()
	stale, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Rollback()
	m := Mutation{RequestID: "retry", Vector: []float64{2, 3}}
	q, err := r.prepareMutation("second", m, false)
	if err != nil {
		t.Fatal(err)
	}
	var tail appendCursor
	if _, changed, err := r.mutateIn(ctx, stale, q, &tail); err != nil || !changed || tail.lsn != 1 {
		t.Fatal(tail, err)
	}
	if _, err = r.Mutate(ctx, "first", Mutation{RequestID: "first", Vector: []float64{0, 1}}, false); err != nil {
		t.Fatal(err)
	}
	// Supply the durable checkpoint metadata consumed by PruneEvents.
	tx, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = putJSON(tx, []byte(r.Prefix+"manifest"), Manifest{LogVersion: LogVersion, Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if floor, err := r.PruneEvents(ctx, 1); floor != 1 || err != nil {
		t.Fatal(floor, err)
	}
	if err = stale.Commit(ctx); !tikverr.IsErrWriteConflict(err) {
		t.Fatal("stale append should conflict with compacted LSN", err)
	}
	if _, err = r.Mutate(ctx, "second", m, false); err != nil {
		t.Fatal(err)
	}
	b, err := r.ReadEvents(ctx, 1, 0, 1)
	if err != nil || b.Floor != 1 || b.Head != 2 || len(b.Events) != 1 || b.Events[0].Record.ID != "second" {
		t.Fatal(b, err)
	}
	tx, err = r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Get(ctx, []byte(r.Prefix+"log-head")); !tikverr.IsErrNotFound(err) {
		t.Fatal("unexpected stored data log head", err)
	}
}

// The old writer already staged the next log event when the new epoch was
// granted. Even with no writes by the successor, TiKV must reject its commit.
func TestTiKVInflightAppendFencedAtTakeover(t *testing.T) {
	f := newEpochTiKVFixture(t, 0)
	tx, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	guard := f.c.coordinator.writeGuardKey(f.source.Partition)
	cursor := appendCursor{known: true}
	q := preparedRequest(t, f.r, "delayed", Mutation{RequestID: "delayed", Vector: []float64{1, 2}}, false)
	if _, wrote, err := f.r.mutateIn(f.ctx, tx, q.preparedMutation, &cursor); err != nil || !wrote {
		t.Fatal(err)
	}
	if err = tx.Set(guard, epochToken(f.source.Epoch)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.coordinator.Update(f.ctx, f.source, false, true); err != nil {
		t.Fatal(err)
	}
	successor, err := f.c.coordinator.Acquire(f.ctx, f.source.Partition, f.node)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); !tikverr.IsErrWriteConflict(err) {
		t.Fatal("late commit crossed takeover", err)
	}
	g, err := f.c.recoverWriter(f.ctx, 0, successor)
	if err != nil {
		t.Fatal(err)
	}
	g.lease.grant.Store(&appendGrant{deadline: time.Now().Add(time.Second), expires: successor.Expires})
	if _, err = f.r.appendGroup(f.ctx, []*mutationRequest{q}, g.lease); err != nil || g.lease.head.Load() != 1 {
		t.Fatal(err, g.lease.head.Load())
	}
}
