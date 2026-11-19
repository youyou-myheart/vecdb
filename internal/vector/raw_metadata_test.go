package vector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRawOwnerReadersOnlySeeConfirmedHistory(t *testing.T) {
	ctx := context.Background()
	repo, owner := rawTestRepo(0), rawTestOwner(0, 1)
	store := newRawDataStore(newFakeRawData(), rawTestNamespace)
	repo.raw = store
	writer := rawTestRecover(t, ctx, store, repo, owner)
	group := &mutationGroup{lease: &appendLease{owner: owner}, rawWriter: writer}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	store.local.Store(repo.Prefix, group)
	rawTestAccept(t, ctx, writer, rawTestRequest(t, repo, "confirmed", "req1"))
	_, pending, err := writer.prepare([]*mutationRequest{rawTestRequest(t, repo, "pending", "req2")})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.write(ctx, pending); err != nil {
		t.Fatal(err)
	}
	// Any accidental WAL read panics. Any StartTS request fails the test.
	store.client = nil
	repo.beginTxn = func() (transactionAPI, error) {
		t.Fatal("pure WAL read opened a metadata transaction")
		return nil, injected
	}
	if _, err := group.readOwnedRecord("pending"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	r, err := repo.Get(ctx, "confirmed")
	if err != nil || r.Revision != 1 {
		t.Fatal(r, err)
	}
	page, err := repo.ListRecords(ctx, ListOptions{Limit: 2})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "confirmed" {
		t.Fatal(page, err)
	}
	rows, end, err := repo.Bootstrap(ctx)
	if err != nil || end != 1 || len(rows) != 1 {
		t.Fatal(rows, end, err)
	}
	log, err := repo.ReadEvents(ctx, 0, 0, 2)
	if err != nil || len(log.Events) != 1 || log.Head != 1 {
		t.Fatal(log, err)
	}
	group.lease.Retire()
	if _, err := group.readOwnedRecord("confirmed"); !errors.Is(err, ErrFenced) {
		t.Fatal("retired owner served read", err)
	}
}

func TestRawDeliveryProofCoversSparseRangeAndDeletes(t *testing.T) {
	f, c, owner := routedFixture(t)
	c.token = "shared-cluster-token"
	c.layout.Routing.Centers = [][]float64{{0, 0}, {10, 0}}
	repo := rawTestRepo(0)
	store := newRawDataStore(newFakeRawData(), rawTestNamespace)
	w := rawTestRecover(t, f.ctx, store, repo, owner)
	b := &mutationGroup{rawWriter: w, lease: &appendLease{owner: owner}}
	confirmAppendGrant(t, b.lease, owner.Epoch, 1<<60, time.Minute)
	var events []LogEvent
	for i, vec := range [][]float64{{0, 0}, {10, 0}, nil, {0, 0}} {
		id := string(rune('a' + i))
		q := preparedRequest(t, repo, id, Mutation{RequestID: id, Vector: vec}, vec == nil)
		rawTestAccept(t, f.ctx, w, q)
		entry := w.history.entries[i]
		if i != 1 {
			events = append(events, LogEvent{Sequence: uint64(i + 1), Record: entry.Record})
		}
	}
	target := IndexPartitionID("index-0000")
	q := Delivery{Source: 0, Owner: owner, RouteVersion: c.layout.Epoch, Through: 4, Events: events}
	var err error
	q.proof, err = c.signRawDelivery(target, q, b, 0)
	if err != nil || !c.verifyDelivery(target, q) {
		t.Fatal("complete projected range rejected", err)
	}
	if c.verifyDelivery("index-0001", q) {
		t.Fatal("proof transferred to another index")
	}
	q.Events = []LogEvent{events[0], events[2]}
	q.proof, err = c.signRawDelivery(target, q, b, 0)
	if err != nil || c.verifyDelivery(target, q) {
		t.Fatal("sender certified an omitted delete", err)
	}
	q.Events = events
	forged := q
	forged.Previous = 1
	if _, err := c.signRawDelivery(target, forged, b, 0); err == nil {
		t.Fatal("certified gap without a checkpoint or graph ACK")
	}
	q.Owner.Epoch++
	if c.verifyDelivery(target, q) {
		t.Fatal("proof transferred to another epoch")
	}
}

func TestGraphCommitProofsCoalesceAndRecoverLostNotification(t *testing.T) {
	f, c, owner := routedFixture(t)
	group := &mutationGroup{lease: &appendLease{owner: owner}}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.lease.head.Store(5)
	id := IndexPartitionID("index-0000")
	c.writers = map[DataPartitionID]*mutationGroup{0: group}
	c.indexes = map[IndexPartitionID]PartitionIndex{id: nil}
	ack := graphCommitAck{Index: id, Epoch: owner.Epoch, Session: owner.Session, Through: 1}
	c.receiveGraphAcks(0, []graphCommitAck{ack})
	for _, invalid := range []graphCommitAck{{Index: id, Epoch: owner.Epoch + 1, Session: owner.Session, Through: 5}, {Index: id, Epoch: owner.Epoch, Session: "stale", Through: 5}, {Index: id, Epoch: owner.Epoch, Session: owner.Session, Through: 6}} {
		c.receiveGraphAcks(0, []graphCommitAck{invalid})
	}
	if got := group.proofs.read(id); got.through != 1 {
		t.Fatal(got)
	}
	start := f.r.beginTxn
	s := &dataDispatcher{owner: owner, writer: group}
	f.r.beginTxn = func() (transactionAPI, error) { t.Fatal("fresh proof read graph metadata"); return nil, injected }
	events := map[IndexPartitionID][]LogEvent{id: {{Sequence: 2}}}
	if got, err := c.dispatchCommits(f.ctx, 0, s, []IndexPartitionID{id}, events); err != nil || got[id] != 1 {
		t.Fatal(got, err)
	}
	// An old ACK does not mean a fresh mutation has stalled. Even after
	// an idle interval there must be no graph read before its first offer.
	expired := time.Now().Add(-time.Second)
	group.proofs.byIndex[id] = graphCommitProof{through: 1, offered: 1, checked: expired}
	if _, err := c.dispatchCommits(f.ctx, 0, s, []IndexPartitionID{id}, events); err != nil {
		t.Fatal(err)
	}
	group.proofs.noteDelivery(id, 2)
	first := group.proofs.read(id)
	group.proofs.noteDelivery(id, 2)
	retry := group.proofs.read(id)
	if !first.checked.After(expired) || retry.checked != first.checked || retry.offered != 2 {
		t.Fatal("new delivery or retry used the wrong fallback deadline", first, retry)
	}
	if _, err := c.dispatchCommits(f.ctx, 0, s, []IndexPartitionID{id}, events); err != nil {
		t.Fatal(err)
	}
	f.r.beginTxn = start
	// Commit the next event with no notification, then expire the fallback timer.
	f.write(t, "a", false)
	f.write(t, "b", false)
	q := deliveryFor(t, f, owner, 0)
	if err := f.services[0].graphReplica.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	group.proofs.byIndex[id] = graphCommitProof{through: 1, offered: 2, checked: expired}
	got, err := c.dispatchCommits(f.ctx, 0, s, []IndexPartitionID{id}, events)
	if err != nil || got[id] != 2 {
		t.Fatal("lost ACK did not recover", got, err)
	}
	s.completed, s.windows, s.checkpointAt = 2, 99, time.Now()
	if err := c.flushCompleted(f.ctx, 0, s, false); err != nil || s.persisted != 0 {
		t.Fatal("checkpoint written early", err)
	}
	s.windows++
	if err := c.flushCompleted(f.ctx, 0, s, false); err != nil || s.persisted != 2 {
		t.Fatal("checkpoint did not persist", err)
	}
}

func TestNewTargetFirstDeliverySkipsGraphPollAndTakeoverRecoversItsCommit(t *testing.T) {
	f, c, owner := routedFixture(t)
	f.write(t, "v", false)
	q := deliveryFor(t, f, owner, 0)
	id := f.services[0].graphReplica.journal.id
	c.cachedRouting, c.replicas = true, 2
	if _, err := c.cachedSearchRoutes(f.ctx, false); err != nil {
		t.Fatal(err)
	}
	requests := 0
	c.client = &http.Client{Transport: routeTransport(func(*http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
	s := &dataDispatcher{owner: owner, writer: &mutationGroup{lease: &appendLease{owner: owner}}, recovered: true, window: q.Events}
	start := f.r.beginTxn
	f.r.beginTxn = func() (transactionAPI, error) {
		t.Fatal("initial delivery of new WAL polled durable graph state")
		return nil, injected
	}
	if err := c.dispatchSource(f.ctx, 0, s); err != nil {
		t.Fatal(err)
	}
	proof := s.writer.proofs.read(id)
	if requests != 1 || proof.offered != q.Through || proof.through != 0 {
		t.Fatal("delivery marked an incorrect LSN", requests, proof)
	}
	f.r.beginTxn = start

	// The graph can commit successfully while its former data owner is
	// offline. A new incarnation must seed its backlog before resending.
	if err := f.services[0].graphReplica.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	successor := &dataDispatcher{writer: &mutationGroup{}, recoveryHead: q.Through}
	targets := []IndexPartitionID{id}
	events := map[IndexPartitionID][]LogEvent{id: q.Events}
	got, err := c.dispatchCommits(f.ctx, 0, successor, targets, events)
	if err != nil || got[id] != q.Through {
		t.Fatal("takeover missed durable target watermark", got, err)
	}
	f.r.beginTxn = func() (transactionAPI, error) { t.Fatal("takeover polled a seeded target twice"); return nil, injected }
	if _, err = c.dispatchCommits(f.ctx, 0, successor, targets, events); err != nil {
		t.Fatal(err)
	}
}

func TestRoutedVisibilityWaitsForActualReplicaApply(t *testing.T) {
	f, c, owner := routedFixture(t)
	s := f.services[0]
	g := s.graphReplica
	entry := &localPartition{service: s, owner: g.journal.owner}
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	q := Search{K: 1, Vector: []float64{1, 2}, MinVisible: &Token{ID: "v", Revision: 1}}
	reply := make(chan error, 1)
	go func() {
		result, err := c.searchVisibleReplica(ctx, entry, q)
		if err == nil && (len(result.Hits) != 1 || result.Hits[0].ID != "v") {
			err = errors.New("visibility waiter missed applied record")
		}
		reply <- err
	}()
	select {
	case <-g.input.wake:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	f.write(t, "v", false)
	if err := g.enqueue(f.ctx, deliveryFor(t, f, owner, 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reply:
		t.Fatal("delivery admission released waiter", err)
	default:
	}
	if err := s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-reply; err != nil {
		t.Fatal(err)
	}
	f.r.beginTxn = func() (transactionAPI, error) { t.Fatal("visible token reopened metadata"); return nil, injected }
	if _, err := c.searchVisibleReplica(ctx, entry, q); err != nil {
		t.Fatal(err)
	}
}
