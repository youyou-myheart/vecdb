package vector

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func leasedFixture(t *testing.T) (*graphFixture, *ClusterService, Ownership) {
	t.Helper()
	f, c, o := routedFixture(t)
	enableReferences(f)
	f.c.leaseReads = true
	for _, s := range f.services {
		g := s.graphReplica
		g.writer = &indexWriter{}
		p, e := f.c.Plan(f.ctx, g.journal.id)
		if e != nil {
			t.Fatal(e)
		}
		if e = g.refreshLease(f.ctx, p, time.Now()); e != nil {
			t.Fatal(e)
		}
		if e = s.Sync(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
	return f, c, o
}

// Count actual transaction operations, not just calls to the public Head API.
// KV reads for immutable source data and the storage write guard remain allowed.
type noControlReadsTx struct {
	transactionAPI
	t *testing.T
}

func (x noControlReadsTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	k := string(key)
	if strings.Contains(k, "cluster/owners/") || strings.Contains(k, "cluster/replica-plans/") || strings.HasSuffix(k, "/head") || strings.Contains(k, "cluster/fences/") {
		x.t.Fatalf("hot path control read: %s", k)
	}
	return x.transactionAPI.Get(ctx, key)
}
func (x noControlReadsTx) Last(ctx context.Context, a, b []byte) ([]byte, []byte, error) {
	x.t.Fatalf("hot path reverse scan: %s", a)
	return nil, nil, injected
}

func TestIndexLeaseHotPathAndFollowerEquality(t *testing.T) {
	f, c, o := leasedFixture(t)
	a, b := f.services[0], f.services[1]
	f.write(t, "first", false)
	q := deliveryFor(t, f, o, 0)
	if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	original := f.r.beginTxn
	f.write(t, "second", false)
	q = deliveryFor(t, f, o, 1)
	f.r.beginTxn = func() (transactionAPI, error) {
		tx, e := original()
		if e != nil {
			return nil, e
		}
		return noControlReadsTx{tx, t}, nil
	}
	// Each index repository is a value copy of the root; instrument it as well.
	a.graphReplica.repo.beginTxn = f.r.beginTxn
	entry := &localPartition{owner: a.graphReplica.journal.owner, service: a}
	c.locals = map[IndexPartitionID]*localPartition{entry.owner.Partition: entry}
	if _, e := c.execute(f.ctx, partitionRPC{Namespace: f.r.Prefix, Partition: entry.owner.Partition, Epoch: entry.owner.Epoch, Operation: "deliver", Delivery: &q}); e != nil {
		t.Fatal(e)
	}
	if _, _, e := c.stepRoutedIndex(f.ctx, entry); e != nil {
		t.Fatal(e)
	}
	f.r.beginTxn = original
	a.graphReplica.repo.beginTxn = original
	if e := b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	h, e := a.graphReplica.journal.Head(f.ctx)
	if e != nil || h.Sources["0"] != 2 || h.Committed != a.graphReplica.position {
		t.Fatal(h, e)
	}
}

func TestIndexLeaseUnknownAppendRecoversDurableState(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "unknown_committed"}[committed], func(t *testing.T) {
			f, _, o := leasedFixture(t)
			a, b := f.services[0], f.services[1]
			f.write(t, "v", false)
			q := deliveryFor(t, f, o, 0)
			if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
				t.Fatal(e)
			}
			f.db.commitErr = injected
			f.db.committedDespiteError = committed
			if e := a.Sync(f.ctx); !errors.Is(e, injected) {
				t.Fatal(e)
			}
			if _, _, e := a.graphReplica.writer.snapshot(); !errors.Is(e, ErrFenced) {
				t.Fatal("uncertain head remained usable", e)
			}
			f.db.commitErr = nil
			f.db.committedDespiteError = false
			if e := a.Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
				t.Fatal(e)
			}
			if e := a.Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			if e := b.Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			equalGraph(t, a.Index, b.Index)
			h, e := a.graphReplica.journal.Head(f.ctx)
			if e != nil || h.Sources["0"] != 1 || h.Committed.Sequence != 2 {
				t.Fatal(h, e)
			}
		})
	}
}

func TestIndexLeaseExpiryFailsClosedAndDelayedGrant(t *testing.T) {
	f, _, o := leasedFixture(t)
	g := f.services[0].graphReplica
	w := g.writer
	w.mu.Lock()
	w.deadline = time.Now().Add(-time.Millisecond)
	w.mu.Unlock()
	f.write(t, "v", false)
	if e := g.enqueue(f.ctx, deliveryFor(t, f, o, 0)); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	if e := f.services[0].Sync(f.ctx); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	lease := w.lease
	lease.remaining = 100 * time.Millisecond
	w.install(time.Now().Add(-time.Second), lease)
	if e := w.authority(lease.token()); !errors.Is(e, ErrFenced) {
		t.Fatal("late reply extended lease", e)
	}
	w.retire()
	w.install(time.Now(), lease)
	if e := w.authority(lease.token()); !errors.Is(e, ErrFenced) {
		t.Fatal("renewal resurrected retired replica", e)
	}
}

func TestIndexLeasePinsPublisherUntilExpiry(t *testing.T) {
	f, _, _ := leasedFixture(t)
	g := f.services[0].graphReplica
	old, e := f.c.Plan(f.ctx, g.journal.id)
	if e != nil {
		t.Fatal(e)
	}
	follower := f.services[1].graphReplica.journal.owner.Partition
	// Explicit release cannot cancel an already issued lease.
	if _, e = f.c.Update(f.ctx, g.journal.owner, false, true); e != nil {
		t.Fatal(e)
	}
	tx, _ := f.r.begin()
	next := ReplicaPlan{Publisher: follower, Serving: append([]Assignment(nil), old.Serving...)}
	drain, e := f.c.protectPublisherLease(f.ctx, tx, g.journal.id, old, &next)
	if e != nil || !drain || next.Publisher != old.Publisher || !next.LeaseDraining {
		t.Fatal(next, drain, e)
	}
	if e = putJSON(tx, f.c.planKey(g.journal.id), next); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(f.ctx); e != nil {
		t.Fatal(e)
	}
	_ = tx.Rollback()
	if _, e = f.c.grantIndexLease(f.ctx, g.journal.id, g.journal.owner); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	f.db.nowMillis = g.writer.lease.Expires + 1
	tx, _ = f.r.begin()
	defer tx.Rollback()
	target := ReplicaPlan{Publisher: follower, Serving: next.Serving}
	if _, e = f.c.protectPublisherLease(f.ctx, tx, g.journal.id, next, &target); e != nil {
		t.Fatal(e)
	}
	if e = putJSON(tx, f.c.planKey(g.journal.id), target); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(f.ctx); e != nil {
		t.Fatal(e)
	}
	check, _ := f.r.begin()
	defer check.Rollback()
	if e = f.c.fenceIndexGuard(f.ctx, check, g.journal.id, g.writer.lease.token()); !errors.Is(e, ErrFenced) {
		t.Fatal("old storage guard accepted", e)
	}
}

func TestIndexLeaseEpochAndCheckpointRebuild(t *testing.T) {
	f, _, o := leasedFixture(t)
	a, b := f.services[0], f.services[1]
	f.write(t, "v", false)
	q := deliveryFor(t, f, o, 0)
	if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	newer := nextSourceOwner(t, f, o)
	q.Owner = newer
	if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	if _, e := a.Checkpoint(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := a.Rebuild(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	f.write(t, "v2", false)
	q = deliveryFor(t, f, newer, 1)
	if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
}

func TestIndexLeaseScheduledMigrationDrains(t *testing.T) {
	f := newMigrationFixture(t)
	f.c.leaseReads = true
	p := f.tick()
	f.ready(p, 0)
	p = f.tick()
	owner := f.owners[p.Publisher]
	f.c.ttl = 1000
	grant, e := f.c.grantIndexLease(f.ctx, "index-0000", owner)
	f.c.ttl = 60000
	if e != nil {
		t.Fatal(e)
	}
	replacement := ClusterNode{ID: "replacement", Session: "r", URL: "http://replacement", Roles: []string{RoleWorker}}
	if e = f.c.Heartbeat(f.ctx, replacement); e != nil {
		t.Fatal(e)
	}
	f.nodes = append(f.nodes, replacement)
	for n := range f.nodes {
		if f.nodes[n].ID == owner.Node {
			f.nodes[n].Draining = true
		}
	}
	p = f.tick()
	f.ready(p, 0)
	p = f.tick()
	if p.Publisher != owner.Partition || !p.LeaseDraining || !containsReplica(p.Serving, owner.Partition) {
		t.Fatal("migration revoked active lease", p)
	}
	if _, e = f.c.grantIndexLease(f.ctx, "index-0000", owner); !errors.Is(e, ErrFenced) {
		t.Fatal("draining publisher renewed", e)
	}
	f.db.nowMillis = grant.Expires + 1
	p = f.tick()
	if p.Publisher == owner.Partition || p.LeaseDraining || containsReplica(p.Serving, owner.Partition) {
		t.Fatal("migration did not finish after expiry", p)
	}
}

func TestIndexLeaseRenewalDoesNotWaitForConstruction(t *testing.T) {
	f, _, _ := leasedFixture(t)
	g := f.services[0].graphReplica
	w := g.writer
	plan, e := f.c.Plan(f.ctx, g.journal.id)
	if e != nil {
		t.Fatal(e)
	}
	w.op.Lock()
	defer w.op.Unlock()
	// The caller deliberately owns the construction lock. A regular renewal
	// must update the deadline without needing that lock (and cannot deadlock).
	if e = g.refreshLease(f.ctx, plan, time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, _, e = w.snapshot(); e != nil {
		t.Fatal(e)
	}
}

func TestIndexLeaseLocalMetricsSurviveKVFailure(t *testing.T) {
	f, c, _ := leasedFixture(t)
	a := f.services[0]
	g := a.graphReplica
	c.locals = map[IndexPartitionID]*localPartition{g.journal.owner.Partition: {owner: g.journal.owner, service: a}}
	f.db.beginErr = injected
	metrics, e := c.LocalMetrics()
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"vecdb_index_local_lease_valid", "vecdb_index_local_head_ready", "vecdb_index_local_lease_remaining_seconds"} {
		if !strings.Contains(metrics, name) {
			t.Fatal("lease diagnostics missing during KV outage", name)
		}
	}
	g.writer.invalidate()
	metrics, e = c.LocalMetrics()
	if e != nil {
		t.Fatal(e)
	}
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "vecdb_index_local_head_ready{") && !strings.HasSuffix(line, " 0") {
			t.Fatal("unknown head reported ready", line)
		}
	}
}

func TestIndexLeaseRejectsV13Namespace(t *testing.T) {
	r, db := newFakeRepo()
	old := []byte("v13-cluster-routing-source-epoch-hash-node-retention=true")
	db.data[r.Prefix+"cluster-mode"] = old
	if _, e := r.ensureRoutedLayout(context.Background(), 1, &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}, "hash", "node", true); e == nil {
		t.Fatal("accepted an incompatible writer protocol")
	}
	if string(db.data[r.Prefix+"cluster-mode"]) != string(old) {
		t.Fatal("rewrote an existing namespace")
	}
}
