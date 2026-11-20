package vector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.com/vector-storage/internal/lease"
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
	for _, mode := range []string{"guard-1pc", "guard-2pc", "single-slot-1pc"} {
		t.Run(mode, func(t *testing.T) {
			f := newEpochTiKVFixture(t, 1)
			f.c.coordinator.ttl = 1000
			f.c.coordinator.holdReleasedLeases = mode == "single-slot-1pc"
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
			takeover := func() error {
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
			}
			ctx := f.ctx
			if mode == "single-slot-1pc" {
				ctx = interceptor.WithRPCInterceptor(ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
					return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
						if req.Type == tikvrpc.CmdPrewrite && req.Prewrite().TryOnePc && !fired {
							if err := takeover(); err != nil {
								return nil, err
							}
						}
						return send(target, req)
					}
				})
			} else {
				repo.beginTxn = func() (transactionAPI, error) {
					tx, err := f.r.begin()
					return dispatchHookTx{transactionAPI: tx, forceTwoPhase: mode == "guard-2pc", beforeCommit: takeover}, err
				}
			}
			_, e = j.commitWithAuthority(ctx, h, 1, "delta", Manifest{}, []Delivery{q}, f.c, payload, token, g.writer)
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
	if _, err = g.journal.claimLeased(f.ctx, token); err != nil {
		t.Fatal(err)
	}
	if err = native.commitGraphSlot(f.ctx, key, []byte("late")); err == nil {
		t.Fatal("recovery failed to fence delayed insert")
	}
	// Separate the confirmed local authority from the store-clock field. A
	// lease-derived MaxCommitTs (or StartTS expiry check) would reject both slots.
	g.writer.replicaLease = &lease.Handle{}
	if !g.writer.replicaLease.Confirm(time.Now(), lease.Grant{Epoch: g.journal.owner.Epoch, Expires: 1, Remaining: time.Minute}) {
		t.Fatal("failed to publish test replica grant")
	}
	defer g.writer.replicaLease.Retire()
	q := f.delivery(t, false)
	// Exercise both the first source epoch barrier and its data delta. Inspect
	// the store reply: merely enabling TryOnePc does not prove it was used.
	var onePhase, separateCommits, extraKeys atomic.Int32
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if req.Type == tikvrpc.CmdCommit {
				for _, k := range req.Commit().Keys {
					if bytes.HasPrefix(k, g.journal.key("entries/")) {
						separateCommits.Add(1)
					}
				}
			}
			resp, err := next(target, req)
			if req.Type == tikvrpc.CmdPrewrite {
				p := req.Prewrite()
				for _, m := range p.Mutations {
					if bytes.HasPrefix(m.Key, g.journal.key("entries/")) {
						if len(p.Mutations) != 1 || m.Op != kvrpcpb.Op_Insert || !p.TryOnePc {
							extraKeys.Add(1)
						}
						if err == nil && resp != nil {
							reply := resp.Resp.(*kvrpcpb.PrewriteResponse)
							if ts := reply.OnePcCommitTs; ts > p.StartVersion && (p.MaxCommitTs == 0 || ts <= p.MaxCommitTs) {
								onePhase.Add(1)
							}
						}
					}
				}
			}
			return resp, err
		}
	})
	if err = g.enqueue(ctx, q); err != nil {
		t.Fatal(err)
	}
	if err = s.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if onePhase.Load() != 2 || separateCommits.Load() != 0 || extraKeys.Load() != 0 {
		t.Fatal("expected two single-slot 1PC replies", onePhase.Load(), separateCommits.Load(), extraKeys.Load())
	}
	m := &metricText{}
	if err = f.c.observations.pipeline.appendTo(m); err != nil {
		t.Fatal(err)
	}
	metrics := m.String()
	for _, stage := range []string{"graph_tikv_one_phase", "graph_tikv_latest_ts", "graph_tikv_prewrite", "graph_tikv_commit_ts", "graph_tikv_commit_primary", "graph_tikv_backoff"} {
		if !strings.Contains(metrics, `stage="`+stage+`"`) {
			t.Fatalf("missing native TiKV commit detail %s", stage)
		}
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
	if err = native.commitGraphSlot(f.ctx, key, []byte("overwrite")); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate graph slot overwritten")
	}
}

func TestNativeGraphSlotRefusesTwoPhaseFallback(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	f.c.coordinator.holdReleasedLeases = true
	j := f.services[0].graphReplica.journal
	key := j.key("test/graph-slot")
	tx, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	native, ok := j.nativeAppend(tx)
	if !ok {
		t.Fatal("missing native transaction")
	}
	var fallback, commits atomic.Int32
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if req.Type == tikvrpc.CmdCommit {
				commits.Add(1)
			}
			if req.Type != tikvrpc.CmdPrewrite {
				return send(target, req)
			}
			// Force the store to return a real uncommitted 2PC lock.
			req.Prewrite().MaxCommitTs = native.StartTS()
			resp, err := send(target, req)
			if err == nil && resp != nil {
				if p := resp.Resp.(*kvrpcpb.PrewriteResponse); p.RegionError == nil && len(p.Errors) == 0 && p.OnePcCommitTs == 0 {
					fallback.Add(1)
				}
			}
			return resp, err
		}
	})
	if err = native.commitGraphSlot(ctx, key, []byte("declined")); err == nil || fallback.Load() != 1 || commits.Load() != 0 {
		t.Fatal("unexpected 1PC fallback outcome", err, fallback.Load(), commits.Load())
	}
	retry, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Rollback()
	if _, err = retry.Get(f.ctx, key); !tikverr.IsErrNotFound(err) {
		t.Fatal("unconfirmed graph slot visible", err)
	}
	newTxn, _ := j.nativeAppend(retry)
	if err = newTxn.commitGraphSlot(f.ctx, key, []byte("recovered")); err != nil {
		t.Fatal("slot unavailable after declined 1PC", err)
	}
}

func TestNativeGraphSlotLostReplyRecoversOnce(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	f.c.coordinator.holdReleasedLeases = true
	enableNativeLease(t, f)
	s := f.services[0]
	g := s.graphReplica
	q := f.delivery(t, false)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	var dropped atomic.Bool
	ctx = interceptor.WithRPCInterceptor(ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			resp, err := send(target, req)
			if req.Type == tikvrpc.CmdPrewrite && err == nil && resp != nil &&
				resp.Resp.(*kvrpcpb.PrewriteResponse).OnePcCommitTs > 0 && dropped.CompareAndSwap(false, true) {
				cancel()
				return nil, context.Canceled
			}
			return resp, err
		}
	})
	if err := s.Sync(ctx); err == nil || !dropped.Load() {
		t.Fatal("lost confirmed 1PC reply reported as success", err)
	}
	durable, err := g.journal.Head(f.ctx)
	if err != nil || durable.Committed.Sequence != 2 || durable.Sources["0"] != 1 {
		t.Fatal("missing durable delta", durable, err)
	}
	if g.position == durable.Committed {
		t.Fatal("uncertain delta already applied")
	}
	if err = s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err = g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err = s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	head, err := g.journal.Head(f.ctx)
	if err != nil || head.Committed != durable.Committed || g.position != head.Committed {
		t.Fatal("redelivery appended or applied a different position", head, g.position, err)
	}
}
