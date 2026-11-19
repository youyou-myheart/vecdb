package vector

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testReadEntry() (*localPartition, partitionRPC) {
	o := Ownership{Partition: "index-0000~a", Node: "a", Session: "s", Epoch: 3, Ready: true, Expires: time.Now().Add(time.Second).UnixMilli()}
	e := &localPartition{owner: o}
	e.grantReadLease(time.Now(), o, time.Second)
	return e, partitionRPC{Partition: o.Partition, Session: o.Session, Epoch: o.Epoch, Operation: "search"}
}

func TestReadLeaseExpiresAndRejectsOldIncarnation(t *testing.T) {
	e, q := testReadEntry()
	if err := e.checkReadLease(q); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []partitionRPC{{Partition: q.Partition, Epoch: q.Epoch - 1, Session: q.Session}, {Partition: q.Partition, Epoch: q.Epoch, Session: "old"}} {
		if !errors.Is(e.checkReadLease(bad), ErrFenced) {
			t.Fatal("old incarnation accepted")
		}
	}
	delayed := &localPartition{owner: e.owner}
	delayed.grantReadLease(time.Now().Add(-2*time.Second), e.owner, time.Second)
	if !errors.Is(delayed.checkReadLease(q), ErrFenced) {
		t.Fatal("late renewal created authority")
	}
	e.retireReads()
	e.grantReadLease(time.Now(), e.owner, time.Hour)
	if !errors.Is(e.checkReadLease(q), ErrFenced) {
		t.Fatal("late grant resurrected retired entry")
	}
}

func TestSearchUsesLocalLeaseAndRechecksAfterQuery(t *testing.T) {
	e, q := testReadEntry()
	r, _ := newFakeRepo()
	q.Namespace = r.Prefix
	r.beginTxn = func() (transactionAPI, error) { panic("search performed storage IO") }
	index := mustGraph(t, 2)
	if err := index.Apply([]Record{{ID: "a", Revision: 1, Vector: []float64{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	e.service = &Service{Index: index, graphReplica: &graphReplica{}}
	c := &ClusterService{root: r, graphReplication: true, coordinator: &Coordinator{leaseReads: true}, locals: map[IndexPartitionID]*localPartition{q.Partition: e}}
	q.Query = Search{Vector: []float64{1, 0}, K: 1}
	if reply, err := c.execute(context.Background(), q); err != nil || len(reply.Hits) != 1 {
		t.Fatal(reply, err)
	}
	index.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := c.execute(context.Background(), q); done <- err }()
	until := time.Now().Add(time.Second)
	for e.mu.TryLock() {
		e.mu.Unlock()
		if time.Now().After(until) {
			index.mu.Unlock()
			t.Fatal("query did not enter")
		}
		time.Sleep(time.Millisecond)
	}
	e.retireReads()
	index.mu.Unlock()
	if err := <-done; !errors.Is(err, ErrFenced) {
		t.Fatal("query crossed retirement", err)
	}
}

func TestFollowerSearchRechecksLocalReadLease(t *testing.T) {
	f, c, _, follower, _ := followerFixture(t)
	o := follower.graphReplica.journal.owner
	o.Ready = true
	entry := c.locals[o.Partition]
	c.localOwnerLeases = true
	entry.grantReadLease(time.Now(), o, time.Minute)
	req := partitionRPC{
		Namespace: f.r.Prefix, Partition: o.Partition, Epoch: o.Epoch, Session: o.Session,
		Operation: "follower_search", Query: Search{Vector: []float64{1, 2}, K: 1},
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.execute(f.ctx, req)
		done <- err
	}()
	queue := follower.graphReplica.readQueue()
	waitFollowerState(t, queue, func(q *followerReadQueue) bool {
		for waiter := range q.pending {
			if waiter.target != nil {
				return true
			}
		}
		return false
	})
	entry.retireReads()
	if err := follower.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrFenced) {
		t.Fatal("follower query crossed retirement", err)
	}
}

func TestFailedRenewalDoesNotExtendReadLease(t *testing.T) {
	f := newGraphFixture(t)
	s := f.services[0]
	o := s.graphReplica.journal.owner
	o.Ready = true
	e := &localPartition{owner: o, service: s}
	e.grantReadLease(time.Now(), o, time.Hour)
	old, ok := e.readLease.Snapshot()
	if !ok {
		t.Fatal("missing confirmed authority")
	}
	var node ClusterNode
	for _, n := range f.nodes {
		if n.ID == o.Node && n.Session == o.Session {
			node = n
		}
	}
	c := &ClusterService{root: f.r, coordinator: f.c, node: node, replicas: 2, graphReplication: true, locals: map[IndexPartitionID]*localPartition{o.Partition: e}, PartitionedService: &PartitionedService{layout: f.layout}}
	f.db.getFail = string(f.c.ownerKey(o.Partition))
	c.replicaTick(f.ctx)
	if now, ok := e.readLease.Snapshot(); !ok || now != old {
		t.Fatal("failed renewal changed local authority", now)
	}
}

func TestUnknownPlanDoesNotRetireOrExtendConfirmedReadLease(t *testing.T) {
	f := newGraphFixture(t)
	f.c.leaseReads = true
	s := f.services[0]
	o := s.graphReplica.journal.owner
	o.Ready = true
	e := &localPartition{owner: o, service: s}
	e.grantReadLease(time.Now(), o, time.Hour)
	old, ok := e.readLease.Snapshot()
	if !ok {
		t.Fatal("missing confirmed authority")
	}
	c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, graphReplication: true, locals: map[IndexPartitionID]*localPartition{o.Partition: e}, PartitionedService: &PartitionedService{layout: f.layout}}
	f.db.getFail = string(f.c.planKey("index-0000"))
	c.replicaTick(f.ctx)
	if now, ok := e.readLease.Snapshot(); !ok || now != old {
		t.Fatal("unknown plan retired/extended confirmed authority")
	}
}
