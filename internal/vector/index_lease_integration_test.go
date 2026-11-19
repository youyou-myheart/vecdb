package vector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func enableNativeLease(t *testing.T, f *epochTiKVFixture) {
	t.Helper()
	f.c.coordinator.leaseReads = true
	for _, s := range f.services {
		g := s.graphReplica
		g.writer = testIndexWriterWithReplicaLease(g)
		plan, e := f.c.coordinator.Plan(f.ctx, g.journal.id)
		if e != nil {
			t.Fatal(e)
		}
		if e = g.refreshLease(f.ctx, plan); e != nil {
			t.Fatal(e)
		}
		if e = s.Sync(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
}

// Pause AFTER the local lease check, then transfer leadership and advance the
// storage guard. Both native 1PC and 2PC must reject the delayed old append.
func TestIndexLeaseTiKVLateAppendAfterTakeover(t *testing.T) {
	for _, twoPhase := range []bool{false, true} {
		t.Run(fmt.Sprint(twoPhase), func(t *testing.T) {
			f := newEpochTiKVFixture(t, 1)
			f.c.coordinator.ttl = 1000
			enableNativeLease(t, f)
			a := f.services[0]
			g := a.graphReplica
			q := f.delivery(t, false)
			if e := g.enqueue(f.ctx, q); e != nil {
				t.Fatal(e)
			}
			// Apply the epoch entry without consuming the batch under test.
			g.input.takeBatch()
			if e := a.Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			h, token, e := g.writer.snapshot()
			if e != nil {
				t.Fatal(e)
			}
			delta, e := a.Index.GenerateDelta([]Record{q.Events[0].Record})
			if e != nil {
				t.Fatal(e)
			}
			payload, e := referenceDelta(delta, []Delivery{q})
			if e != nil {
				t.Fatal(e)
			}
			repo := *f.r
			coord := *f.c.coordinator
			coord.root = &repo
			j := g.journal
			j.c = &coord
			var next *Service
			fired := false
			repo.beginTxn = func() (transactionAPI, error) {
				tx, err := f.r.begin()
				return dispatchHookTx{transactionAPI: tx, forceTwoPhase: twoPhase, beforeCommit: func() error {
					if fired {
						return errors.New("unexpected second old commit")
					}
					fired = true
					// Wait for the actual persisted lease to expire, not an artificial release.
					wait := time.Until(time.UnixMilli(g.writer.lease.Expires).Add(30 * time.Millisecond))
					if wait > 0 {
						timer := time.NewTimer(wait)
						defer timer.Stop()
						select {
						case <-timer.C:
						case <-f.ctx.Done():
							return f.ctx.Err()
						}
					}
					node := ClusterNode{ID: "replacement", Session: "new", URL: "http://localhost:2", Roles: []string{RoleWorker}}
					if err := f.c.coordinator.Heartbeat(f.ctx, node); err != nil {
						return err
					}
					id := replicaID(g.journal.id, node)
					testAssignment(t, f.c.coordinator, node, id)
					owner, err := f.c.coordinator.Acquire(f.ctx, id, node)
					if err != nil {
						return err
					}
					owner, err = f.c.coordinator.Update(f.ctx, owner, true, false)
					if err != nil {
						return err
					}
					old, err := f.c.coordinator.Plan(f.ctx, g.journal.id)
					if err != nil {
						return err
					}
					planned := ReplicaPlan{Publisher: id, Serving: []Assignment{{Partition: id, Node: node.ID, Session: node.Session}}}
					switchTx, err := f.r.begin()
					if err != nil {
						return err
					}
					defer switchTx.Rollback()
					if _, err = f.c.coordinator.protectPublisherLease(f.ctx, switchTx, g.journal.id, old, &planned); err != nil {
						return err
					}
					if err = putJSON(switchTx, f.c.coordinator.planKey(g.journal.id), planned); err != nil {
						return err
					}
					if err = switchTx.Commit(f.ctx); err != nil {
						return err
					}
					copyRepo := *f.r
					next, err = newGraphService(f.ctx, &copyRepo, f.c.objects, graphJournal{f.c.coordinator, g.journal.id, owner})
					if err != nil {
						return err
					}
					t.Cleanup(next.Index.graph.Close)
					next.graphReplica.input = newRoutedInput(f.c)
					next.graphReplica.writer = testIndexWriterWithReplicaLease(next.graphReplica)
					if err = next.graphReplica.refreshLease(f.ctx, planned); err != nil {
						return err
					}
					return next.Sync(f.ctx)
				}}, err
			}
			_, e = j.commitWithAuthority(f.ctx, h, 1, "delta", Manifest{}, []Delivery{q}, f.c, payload, token, g.writer)
			if !fired || !tikverr.IsErrWriteConflict(e) {
				t.Fatal("late transaction escaped storage fencing", e)
			}
			if e = next.graphReplica.enqueue(f.ctx, q); e != nil {
				t.Fatal(e)
			}
			if e = next.Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			durable, e := next.graphReplica.journal.Head(f.ctx)
			if e != nil || durable.Sources["0"] != 1 || durable.Committed.Sequence != 2 {
				t.Fatal(durable, e)
			}
			if e = g.writer.authority(token); !errors.Is(e, ErrFenced) {
				t.Fatal("expired old publisher still active", e)
			}
		})
	}
}

func TestIndexLeaseTiKVRenewalSerializesDrain(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	enableNativeLease(t, f)
	g := f.services[0].graphReplica
	oldGrant, ok := g.writer.leaderLease.Snapshot()
	if !ok {
		t.Fatal("publisher lease is not live")
	}
	repo := *f.r
	coord := *f.c.coordinator
	coord.root = &repo
	repo.beginTxn = func() (transactionAPI, error) {
		tx, e := f.r.begin()
		return dispatchHookTx{transactionAPI: tx, beforeCommit: func() error {
			other, e := f.r.begin()
			if e != nil {
				return e
			}
			defer other.Rollback()
			p, e := f.c.coordinator.planIn(f.ctx, other, g.journal.id)
			if e != nil {
				return e
			}
			p.LeaseDraining = true
			if e = putJSON(other, f.c.coordinator.planKey(g.journal.id), p); e != nil {
				return e
			}
			return other.Commit(f.ctx)
		}}, e
	}
	if _, e := coord.grantIndexLease(f.ctx, g.journal.id, g.journal.owner); !tikverr.IsErrWriteConflict(e) {
		t.Fatal("renewal survived concurrent drain", e)
	}
	if grant, ok := g.writer.leaderLease.Snapshot(); !ok || grant != oldGrant {
		t.Fatal("failed renewal extended local authority")
	}
	// Conversely, a switch based on an expired snapshot cannot pass a newer grant.
	tx, e := f.r.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	var expired indexLease
	if e = readJSON(f.ctx, tx, f.c.coordinator.indexLeaseKey(g.journal.id), &expired); e != nil {
		t.Fatal(e)
	}
	expired.Expires = 0
	if e = putJSON(tx, f.c.coordinator.indexLeaseKey(g.journal.id), expired); e != nil {
		t.Fatal(e)
	}
	p, e := f.c.coordinator.planIn(f.ctx, tx, g.journal.id)
	if e != nil {
		t.Fatal(e)
	}
	p.LeaseDraining = false
	if e = putJSON(tx, f.c.coordinator.planKey(g.journal.id), p); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(f.ctx); e != nil {
		t.Fatal(e)
	}
	stale, e := f.r.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer stale.Rollback()
	next := ReplicaPlan{Publisher: "replacement"}
	if _, e = f.c.coordinator.protectPublisherLease(f.ctx, stale, g.journal.id, p, &next); e != nil {
		t.Fatal(e)
	}
	if e = putJSON(stale, f.c.coordinator.planKey(g.journal.id), next); e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.coordinator.grantIndexLease(f.ctx, g.journal.id, g.journal.owner); e != nil {
		t.Fatal(e)
	}
	if e = stale.Commit(context.Background()); !tikverr.IsErrWriteConflict(e) {
		t.Fatal("switch passed a newer grant", e)
	}
}

func TestIndexLeaseTiKVRecoveryFencesUnknownAppend(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	enableNativeLease(t, f)
	a := f.services[0]
	g := a.graphReplica
	q := f.delivery(t, false)
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	g.input.takeBatch()
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	h, token, e := g.writer.snapshot()
	if e != nil {
		t.Fatal(e)
	}
	delta, e := a.Index.GenerateDelta([]Record{q.Events[0].Record})
	if e != nil {
		t.Fatal(e)
	}
	payload, e := referenceDelta(delta, []Delivery{q})
	if e != nil {
		t.Fatal(e)
	}
	repo := *f.r
	coord := *f.c.coordinator
	coord.root = &repo
	j := g.journal
	j.c = &coord
	fired := false
	repo.beginTxn = func() (transactionAPI, error) {
		tx, e := f.r.begin()
		return dispatchHookTx{transactionAPI: tx, beforeCommit: func() error {
			fired = true
			g.writer.invalidate()
			return a.Sync(f.ctx)
		}}, e
	}
	_, e = j.commitWithAuthority(f.ctx, h, 1, "delta", Manifest{}, []Delivery{q}, f.c, payload, token, g.writer)
	if !fired || !tikverr.IsErrWriteConflict(e) {
		t.Fatal("recovery did not fence in-flight unknown append", e)
	}
	if e = g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e = a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	durable, e := g.journal.Head(f.ctx)
	if e != nil || durable.Committed != g.position || durable.Sources["0"] != 1 {
		t.Fatal(durable, e)
	}
}

// Recovery and a delayed append in the same lease generation must conflict
// even though a normal delta no longer writes the index guard.
func TestNativeGraphInsertAndRecoveryFence(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	f.c.coordinator.holdReleasedLeases = true
	enableNativeLease(t, f)
	s := f.services[0]
	g := s.graphReplica
	h, token, err := g.writer.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	native, ok := g.journal.nativeAppend(tx)
	if !ok {
		t.Fatal("native insert disabled")
	}
	key := g.journal.key(fmt.Sprintf("entries/%020d", h.Committed.Sequence+1))
	if err = native.insert(key, []byte("late")); err != nil {
		t.Fatal(err)
	}
	if _, err = g.journal.claimLeased(f.ctx, token); err != nil {
		t.Fatal(err)
	}
	if err = native.commitBeforeLeaseExpiry(f.ctx, g.writer.lease.Expires); err == nil {
		t.Fatal("recovery failed to fence delayed insert")
	}
	q := f.delivery(t, false)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	var guard, insert atomic.Bool
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if req.Type == tikvrpc.CmdPrewrite {
				for _, m := range req.Prewrite().Mutations {
					if bytes.Equal(m.Key, g.journal.c.indexGuardKey(g.journal.id)) {
						guard.Store(true)
					}
					if m.Op == kvrpcpb.Op_Insert && bytes.HasPrefix(m.Key, g.journal.key("entries/")) {
						insert.Store(true)
					}
				}
			}
			return next(target, req)
		}
	})
	if err = s.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if guard.Load() || !insert.Load() {
		t.Fatal("expected immutable graph Insert without guard", guard.Load(), insert.Load())
	}
	h, err = g.journal.Head(f.ctx)
	if err != nil || h.Sources["0"] != 1 {
		t.Fatal(h, err)
	}
	replay, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Rollback()
	native, _ = g.journal.nativeAppend(replay)
	key = g.journal.key(fmt.Sprintf("entries/%020d", h.Committed.Sequence))
	if err = native.insert(key, []byte("overwrite")); err != nil {
		t.Fatal(err)
	}
	if err = native.commitBeforeLeaseExpiry(f.ctx, g.writer.lease.Expires); err == nil {
		t.Fatal("duplicate graph slot overwritten")
	}
}
