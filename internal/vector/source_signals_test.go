package vector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSourceSignalsCoalesceWithoutLosingPartitions(t *testing.T) {
	s := newSourceSignals()
	var wg sync.WaitGroup
	for id := DataPartitionID(0); id < 16; id++ {
		wg.Add(1)
		go func(id DataPartitionID) {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				s.add(id)
			}
		}(id)
	}
	wg.Wait()
	if len(s.wake) != 1 || len(s.take()) != 16 {
		t.Fatal("signals were lost or unbounded")
	}
	<-s.wake
	s.add(3)
	<-s.wake
	if ids := s.take(); len(ids) != 1 || ids[0] != 3 {
		t.Fatal(ids)
	}
	if len(s.take()) != 0 {
		t.Fatal("drained signal set was not empty")
	}
}

func TestCachedSourceNotificationDoesNotResolveOwnerAgain(t *testing.T) {
	f, c, source := routedFixture(t)
	c.cachedRouting = true
	c.dispatchSignals, c.outboundSignals = newSourceSignals(), newSourceSignals()
	c.worker = &Worker{cluster: c}
	c.token = "cached-notification-token"
	server := httptest.NewServer(c.InternalHandler())
	defer server.Close()
	source.URL = server.URL
	raw, _ := json.Marshal(source)
	f.db.data[string(c.coordinator.ownerKey(source.Partition))] = raw
	c.client = server.Client()
	if _, err := c.cachedSearchRoutes(f.ctx, false); err != nil {
		t.Fatal(err)
	}
	f.r.beginTxn = func() (transactionAPI, error) { panic("cached source hint read metadata") }
	if err := c.notifySource(f.ctx, 0); err != nil {
		t.Fatal(err)
	}
	if ids := c.dispatchSignals.take(); len(ids) != 1 || ids[0] != 0 {
		t.Fatal(ids)
	}
}

func TestSourceNotificationIsAuthenticatedHint(t *testing.T) {
	f, c, source := routedFixture(t)
	c.dispatchSignals, c.outboundSignals = newSourceSignals(), newSourceSignals()
	c.worker = &Worker{cluster: c}
	c.token = "test-peer-token"
	server := httptest.NewServer(c.InternalHandler())
	defer server.Close()
	owner := source
	owner.URL = server.URL
	raw, _ := json.Marshal(owner)
	f.db.data[string(c.coordinator.ownerKey(source.Partition))] = raw
	c.client = server.Client()
	if err := c.notifySource(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if ids := c.dispatchSignals.take(); len(ids) != 1 || ids[0] != 0 {
		t.Fatal(ids)
	}
	// A received hint neither constructs a graph nor advances any watermark.
	h, err := f.services[0].graphReplica.journal.Head(f.ctx)
	if err != nil || h.Sources["0"] != 0 {
		t.Fatal(h, err)
	}
	req, _ := http.NewRequest("POST", server.URL+"/internal/partition", nil)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal(response.StatusCode)
	}
	id := DataPartitionID(99)
	if _, err = c.execute(f.ctx, partitionRPC{Namespace: c.root.Prefix, Partition: sourceID(id), Source: &id, Operation: "wake_source"}); err == nil {
		t.Fatal("invalid source accepted")
	}
}

func TestCompletedSourceWindowSchedulesNextRead(t *testing.T) {
	f, c, owner := routedFixture(t)
	c.dispatchSignals, c.outboundSignals = newSourceSignals(), newSourceSignals()
	f.write(t, "v", false)
	q := deliveryFor(t, f, owner, 0)
	g := f.services[0].graphReplica
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	s := &dataDispatcher{owner: owner, window: q.Events}
	if err := c.dispatchSource(f.ctx, 0, s); err != nil {
		t.Fatal(err)
	}
	if s.completed != 1 || len(s.window) != 0 {
		t.Fatal(s)
	}
	if ids := c.dispatchSignals.take(); len(ids) != 1 || ids[0] != 0 {
		t.Fatal("next window not scheduled", ids)
	}
	if err := c.dispatchSource(f.ctx, 0, s); err != nil {
		t.Fatal(err)
	}
	if len(c.dispatchSignals.take()) != 0 {
		t.Fatal("empty source kept rescheduling itself")
	}
}

func TestGraphCommitNotifiesLocalSourceWithoutOutboundRouting(t *testing.T) {
	f, c, owner := routedFixture(t)
	c.node.ID, c.node.Session = owner.Node, owner.Session
	c.dispatchSignals, c.outboundSignals = newSourceSignals(), newSourceSignals()
	group := &mutationGroup{lease: &appendLease{owner: owner}}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.lease.head.Store(1)
	c.writers = map[DataPartitionID]*mutationGroup{0: group}
	g := f.services[0].graphReplica
	index := g.journal.id
	c.indexes = map[IndexPartitionID]PartitionIndex{index: nil}
	f.write(t, "v", false)
	q := deliveryFor(t, f, owner, 0)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if group.proofs.read(index).through != 0 {
		t.Fatal("admission certified an uncommitted delivery")
	}
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if group.proofs.read(index).through != 1 || len(c.dispatchSignals.take()) != 1 {
		t.Fatal("local commit did not advance and wake the source")
	}
	if len(c.outboundSignals.take()) != 0 || len(c.outboundSignals.takeAcks(0)) != 0 {
		t.Fatal("local commit requested outbound routing")
	}

	f.r.beginTxn = func() (transactionAPI, error) {
		t.Fatal("local commit notification opened a metadata transaction")
		return nil, injected
	}
	q.Through = 2
	c.signalGraphCommit(index, q, nil)
	group.lease.head.Store(2)
	c.signalGraphCommit(index, q, injected)
	if group.proofs.read(index).through != 1 || len(c.dispatchSignals.take()) != 1 || len(c.outboundSignals.take()) != 0 {
		t.Fatal("out-of-bounds or uncertain commit certified a position or missed the local wake")
	}

	group.lease.StepDown()
	next := owner
	next.Epoch++
	successor := &mutationGroup{lease: &appendLease{owner: next}}
	confirmAppendGrant(t, successor.lease, next.Epoch, 1<<60, time.Minute)
	successor.lease.head.Store(2)
	c.writers[0] = successor
	c.signalGraphCommit(index, q, nil)
	acks := c.outboundSignals.takeAcks(0)
	if len(c.outboundSignals.take()) != 1 || len(acks) != 1 || acks[0].Epoch != owner.Epoch || len(c.dispatchSignals.take()) != 0 {
		t.Fatal("late commit did not retain its source epoch for routed reconciliation")
	}
	c.receiveGraphAcks(0, acks)
	if successor.proofs.read(index).through != 0 {
		t.Fatal("late commit was credited to successor")
	}
}
