package vector

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
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
	if _, err = writer.write(ctx, pending); err != nil {
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
	group.lease.StepDown()
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

}

func TestRawDispatchReportsProgressOnlyWithLeaseRenewal(t *testing.T) {
	f, c, owner := routedFixture(t)
	data := *f.r
	data.Prefix += "data/0000/"
	store := newRawDataStore(newFakeRawData(), f.r.Prefix)
	f.r.raw, data.raw = store, store
	c.data[0] = &data
	group := &mutationGroup{lease: &appendLease{owner: owner}}
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	group.rawWriter = rawTestRecover(t, f.ctx, store, &data, owner)
	for _, id := range []string{"one", "two", "three"} {
		rawTestAccept(t, f.ctx, group.rawWriter, rawTestRequest(t, &data, id, id))
	}
	store.local.Store(data.Prefix, group)
	group.lease.head.Store(3)
	if _, err := c.coordinator.update(f.ctx, owner, true, false, func(o *Ownership) { o.Offset = 1 }); err != nil {
		t.Fatal(err)
	}
	s := &dataDispatcher{owner: owner, writer: group}
	if err := c.loadDispatchWindow(f.ctx, 0, s); err != nil || s.completed != 1 ||
		len(s.window) != 2 || s.window[0].Sequence != 2 {
		t.Fatal("recovery did not start at the reported progress", s.completed, s.window, err)
	}
	target := c.layout.Bindings[0].Index
	group.proofs.record(target, 3, false)
	begin := f.r.beginTxn
	f.r.beginTxn = func() (transactionAPI, error) {
		t.Fatal("confirming a cached RawKV window opened a metadata transaction")
		return nil, injected
	}
	if err := c.dispatchSource(f.ctx, 0, s); err != nil || group.lease.indexed.Load() != 3 {
		t.Fatal("RawKV progress was not advanced in memory", s.completed, err)
	}
	f.r.beginTxn = begin
	if current, err := c.coordinator.Resolve(f.ctx, owner.Partition); err != nil || current.Offset != 1 {
		t.Fatal("progress changed before renewal", current, err)
	}
	if _, err := c.coordinator.update(f.ctx, owner, true, false, func(o *Ownership) {
		o.Offset = group.lease.indexed.Load()
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.coordinator.Update(f.ctx, owner, false, true); err != nil {
		t.Fatal(err)
	}
	for _, node := range f.nodes {
		if node.ID != owner.Node {
			continue
		}
		next, err := c.coordinator.Acquire(f.ctx, owner.Partition, node)
		if err != nil || next.Epoch <= owner.Epoch || next.Offset != 3 {
			t.Fatal("takeover lost the reported progress", next, err)
		}
	}
	if _, found := f.db.data[string(completedKey(&data))]; found {
		t.Fatal("dispatcher wrote the old progress key")
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
	c.token = "test-internal-token"
	requests := 0
	c.client = &http.Client{Transport: routeTransport(func(req *http.Request) (*http.Response, error) {
		var wire partitionRPC
		if err := json.NewDecoder(req.Body).Decode(&wire); err != nil || wire.Delivery == nil {
			return nil, errors.New("missing projected delivery")
		}
		d := *wire.Delivery
		d.proof, _ = hex.DecodeString(req.Header.Get(deliveryProofHeader))
		if !c.verifyDelivery(id, d) {
			return nil, errors.New("missing valid TxnKV WAL signature")
		}
		d.Events = nil
		if c.verifyDelivery(id, d) {
			return nil, errors.New("WAL proof accepted an omitted event")
		}
		requests++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
	s := &dataDispatcher{owner: owner, writer: &mutationGroup{lease: &appendLease{owner: owner}}, recovered: true, window: q.Events}
	confirmAppendGrant(t, s.writer.lease, owner.Epoch, owner.Expires, time.Minute)
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
	// A successful admission is volatile. Suppress only the exact repeat.
	if err := c.dispatchSource(f.ctx, 0, s); err != nil || requests != 1 || s.completed != 0 {
		t.Fatal("admission advanced the cursor or resent a live batch", requests, s.completed, err)
	}
	proof.admittedAt = time.Now().Add(-2 * dispatchControlInterval)
	s.writer.proofs.byIndex[id] = proof
	if err := c.dispatchSource(f.ctx, 0, s); err != nil || requests != 2 || s.completed != 0 {
		t.Fatal("lost volatile delivery was not retried", requests, s.completed, err)
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

func TestTxnDispatchConsumesDenseLookaheadInOrderedRPCs(t *testing.T) {
	f, c, owner := routedFixture(t)
	const count = 2*dispatchBatchSize + 1
	id := f.services[0].graphReplica.journal.id
	c.cachedRouting, c.replicas, c.token = true, 2, "test-internal-token"
	if _, err := c.cachedSearchRoutes(f.ctx, false); err != nil {
		t.Fatal(err)
	}
	lease := &appendLease{owner: owner}
	confirmAppendGrant(t, lease, owner.Epoch, owner.Expires, time.Minute)
	s := &dataDispatcher{owner: owner, writer: &mutationGroup{lease: lease}, recovered: true}
	for seq := uint64(1); seq <= count; seq++ {
		seed(f.db, f.r.logKey(seq), LogEvent{Sequence: seq, Record: Record{ID: fmt.Sprint(seq), Revision: 1, Vector: []float64{1, 2}}})
	}
	lease.head.Store(count)
	var through uint64
	var sizes []int
	c.client = &http.Client{Transport: routeTransport(func(req *http.Request) (*http.Response, error) {
		var wire partitionRPC
		if err := json.NewDecoder(req.Body).Decode(&wire); err != nil {
			return nil, err
		}
		q := *wire.Delivery
		q.proof, _ = hex.DecodeString(req.Header.Get(deliveryProofHeader))
		if !c.verifyDelivery(id, q) || q.Previous != through || len(q.Events) > dispatchBatchSize {
			return nil, errors.New("invalid or overlapping delivery")
		}
		for _, e := range q.Events {
			through++
			if e.Sequence != through {
				return nil, errors.New("missing event in dense projection")
			}
		}
		if q.Through != through {
			return nil, errors.New("wrong delivery end")
		}
		sizes = append(sizes, len(q.Events))
		s.writer.proofs.record(id, through, false)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
	for n := 0; n < 5 && s.completed < count; n++ {
		if err := c.dispatchSource(f.ctx, 0, s); err != nil {
			t.Fatal(err)
		}
	}
	if s.completed != count || !slices.Equal(sizes, []int{dispatchBatchSize, dispatchBatchSize, 1}) {
		t.Fatal("dense target failed to drain ordered deliveries", s.completed, sizes)
	}
}
