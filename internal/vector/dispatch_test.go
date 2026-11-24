package vector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func routedFixture(t *testing.T) (*graphFixture, *ClusterService, Ownership) {
	t.Helper()
	f := newGraphFixture(t)
	route := &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}
	layout := f.layout
	layout.Routing = route
	c := &ClusterService{root: f.r, coordinator: f.c, objects: f.objects, PartitionedService: &PartitionedService{layout: layout, data: map[DataPartitionID]*Repository{0: f.r}, dimension: 2}}
	c.router = clusterRouter{IdentityPartitionRouter{layout}, route}
	if e := f.c.Schedule(f.ctx, f.master, sourceLayout(1), f.nodes); e != nil {
		t.Fatal(e)
	}
	a, e := f.c.Assignment(f.ctx, sourceID(0))
	if e != nil {
		t.Fatal(e)
	}
	var node ClusterNode
	for _, n := range f.nodes {
		if n.ID == a.Node {
			node = n
		}
	}
	owner, e := f.c.Acquire(f.ctx, sourceID(0), node)
	if e != nil {
		t.Fatal(e)
	}
	owner, e = f.c.Update(f.ctx, owner, true, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range f.services {
		s.graphReplica.input = newRoutedInput(c)
	}
	return f, c, owner
}

func TestPruneAfterDataOwnerHandoff(t *testing.T) {
	f, c, owner := routedFixture(t)
	c.indexes = map[IndexPartitionID]PartitionIndex{c.layout.Bindings[0].Index: f.services[0]}
	c.node.ID, c.node.Session = owner.Node, owner.Session
	l := &appendLease{owner: owner}
	confirmAppendGrant(t, l, owner.Epoch, owner.Expires, time.Second)
	c.writers = map[DataPartitionID]*mutationGroup{0: {lease: l}}
	// The Gateway cached its routes when the handoff was still in progress.
	c.searchRoutes.state = &ClusterStatus{}
	c.searchRoutes.deadline = time.Now().Add(time.Minute)
	if _, err := c.PruneState(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func deliveryFor(t *testing.T, f *graphFixture, o Ownership, after uint64) Delivery {
	t.Helper()
	b, e := f.r.ReadEvents(f.ctx, after, 0, LogBatchSize)
	if e != nil || len(b.Events) == 0 {
		t.Fatal(b, e)
	}
	return Delivery{Source: 0, Owner: o, RouteVersion: 1, Previous: after, Through: b.Events[len(b.Events)-1].Sequence, Events: b.Events}
}
func TestClusterRoutingStableAndIsolated(t *testing.T) {
	r, _ := newFakeRepo()
	route := &ClusterRouting{Version: 1, Centers: [][]float64{{-10, 0}, {10, 0}, {0, 10}}}
	l, e := r.ensureRoutedLayout(context.Background(), 4, route, "hash", "node", true)
	if e != nil {
		t.Fatal(e)
	}
	if len(l.Bindings) != 3 || l.DataPartitions != 4 || route.target([]float64{9, 0}) != "index-0001" || route.target([]float64{0, 0}) != "index-0000" {
		t.Fatal(l)
	}
	route.Centers[0][0] = -99
	if l.Routing.Centers[0][0] != -10 {
		t.Fatal("layout aliases caller")
	}
	if _, e = r.ensureRoutedLayout(context.Background(), 4, route, "hash", "node", true); e == nil {
		t.Fatal("route changed in place")
	}
	if _, e = r.ensureLayout(context.Background(), 4); e == nil {
		t.Fatal("legacy protocol accepted")
	}
}
func TestDispatchContinuousPrefix(t *testing.T) {
	events := []LogEvent{}
	for n := 100; n <= 104; n++ {
		events = append(events, LogEvent{Sequence: uint64(n)})
	}
	route := func(e LogEvent) []IndexPartitionID {
		return map[uint64][]IndexPartitionID{100: {"i0"}, 101: {"i1"}, 102: {"i0"}, 103: {"i2"}, 104: {"i0", "i1"}}[e.Sequence]
	}
	commits := map[IndexPartitionID]uint64{"i0": 104, "i1": 99, "i2": 103}
	if n := completedPrefix(events, 99, route, commits); n != 100 {
		t.Fatal(n)
	}
	commits["i1"] = 101
	if n := completedPrefix(events, 99, route, commits); n != 103 {
		t.Fatal(n)
	}
	commits["i1"] = 104
	if n := completedPrefix(events, 99, route, commits); n != 104 {
		t.Fatal(n)
	}
	if n := completedPrefix(events[1:], 99, route, commits); n != 99 {
		t.Fatal("jumped missing source event", n)
	}
}
func TestRoutedDedupAtomicCommitAndRecovery(t *testing.T) {
	f, _, o := routedFixture(t)
	a, b := f.services[0], f.services[1]
	g := a.graphReplica
	for i := 0; i < 3; i++ {
		f.write(t, fmt.Sprint(i), false)
	}
	q := deliveryFor(t, f, o, 0)
	for i := 0; i < 2; i++ {
		if e := g.enqueue(f.ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	if len(g.input.queue) != 1 {
		t.Fatal("in-flight duplicate queued")
	}
	h, _ := g.journal.Head(f.ctx)
	if h.Committed.Sequence != 1 || h.Sources["0"] != 0 || h.SourceEpochs["0"] != o.Epoch {
		t.Fatal("enqueue must persist only the epoch barrier")
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	h, _ = g.journal.Head(f.ctx)
	if h.Sources["0"] != 3 || h.Committed.Sequence != 2 {
		t.Fatal(h)
	}
	before, _ := a.Index.ExportState()
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	after, _ := a.Index.ExportState()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("duplicate rebuilt graph")
	}
	if e := b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	f.write(t, "next", false)
	q = deliveryFor(t, f, o, 3)
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	h, e := g.journal.Claim(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	f.db.commitErr = injected
	if e = g.commitRouted(f.ctx, a, h); e == nil {
		t.Fatal("injected commit accepted")
	}
	f.db.commitErr = nil
	h, _ = g.journal.Head(f.ctx)
	if h.Sources["0"] != 3 || h.Committed.Sequence != 2 {
		t.Fatal("partial watermark commit", h)
	}
	if e = g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e = g.commitRouted(f.ctx, a, h); e != nil {
		t.Fatal(e)
	} // crash after commit, before local apply
	pending, _ := g.journal.Head(f.ctx)
	if pending.Sources["0"] != 4 || pending.Committed != pending.Visible {
		t.Fatal(pending)
	}
	if _, e = f.c.Update(f.ctx, g.journal.owner, false, true); e != nil {
		t.Fatal(e)
	}
	if e = f.c.ScheduleReplicas(f.ctx, f.master, f.layout, f.nodes, 2); e != nil {
		t.Fatal(e)
	}
	if e = b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	h, _ = b.graphReplica.journal.Head(f.ctx)
	if h.Visible != pending.Committed {
		t.Fatal("pending graph lost")
	}
	if e = b.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Checkpoint(f.ctx); e != nil {
		t.Fatal(e)
	}
	restored, e := newGraphService(f.ctx, b.graphReplica.repo, f.objects, b.graphReplica.journal)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Index.graph.Close()
	equalGraph(t, b.Index, restored.Index)
}
func TestRoutedGapEpochAndQueueLoss(t *testing.T) {
	f, _, o := routedFixture(t)
	g := f.services[0].graphReplica
	f.write(t, "x", false)
	q := deliveryFor(t, f, o, 0)
	bad := q
	bad.RouteVersion = 2
	if e := g.enqueue(f.ctx, bad); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	bad = q
	bad.Previous = 1
	bad.Through = 2
	bad.Events = []LogEvent{{Sequence: 2, Record: q.Events[0].Record}}
	if e := g.enqueue(f.ctx, bad); e == nil {
		t.Fatal("gap accepted")
	}
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	g.input.queue = map[DataPartitionID]*queuedDelivery{}
	g.input.bytes = 0 // lost memory before commit
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if _, e := f.c.Update(f.ctx, o, false, true); e != nil {
		t.Fatal(e)
	}
	if e := f.services[0].Sync(f.ctx); e != nil {
		t.Fatal("accepted epoch should survive source lease expiry until a higher epoch arrives", e)
	}
	h, _ := g.journal.Head(f.ctx)
	if h.Committed.Sequence != 2 || h.Sources["0"] != 1 {
		t.Fatal(h)
	}
}

func TestRoutedCleanupAndReturnToCluster(t *testing.T) {
	f, c, o := routedFixture(t)
	a, b := f.services[0], f.services[1]
	c.layout.Routing.Centers = [][]float64{{0, 0}, {100, 0}}
	r := f.write(t, "moving", false)
	if e := a.graphReplica.enqueue(f.ctx, deliveryFor(t, f, o, 0)); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	r, e := f.r.Mutate(f.ctx, r.ID, Mutation{RequestID: "move-away", Vector: []float64{100, 0}}, false)
	if e != nil {
		t.Fatal(e)
	}
	a.syncMu.Lock()
	e = a.graphReplica.scrub(f.ctx, a)
	a.syncMu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	if !a.Index.records[r.ID].Deleted || a.Index.records[r.ID].Revision != r.Revision {
		t.Fatal("obsolete location retained")
	}
	if e = b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	r, e = f.r.Mutate(f.ctx, r.ID, Mutation{RequestID: "move-back", Vector: []float64{0, 0}}, false)
	if e != nil {
		t.Fatal(e)
	}
	q := Delivery{Source: 0, Owner: o, RouteVersion: 1, Previous: 1, Through: 3, Events: []LogEvent{{Sequence: 3, Record: r}}}
	if e = a.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e = a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	a.syncMu.Lock()
	e = a.graphReplica.scrub(f.ctx, a)
	a.syncMu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	if a.Index.records[r.ID].Deleted || a.Index.records[r.ID].Revision != 3 {
		t.Fatal("cleanup deleted returned revision")
	}
	if c.observations.scrubScanned.Load() != 2 || c.observations.scrubRemoved.Load() != 1 {
		t.Fatal("cleanup observation counts incorrect")
	}
	if e = b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
}

func TestDispatchRecoverySkipsCommittedWindow(t *testing.T) {
	f, c, o := routedFixture(t)
	a := f.services[0]
	for n := 0; n < 3; n++ {
		f.write(t, fmt.Sprint(n), false)
	}
	if e := a.graphReplica.enqueue(f.ctx, deliveryFor(t, f, o, 0)); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	// The source died before recording any progress. Recovery reconstructs only
	// this bounded window, observes durable index watermarks and sends nothing.
	state := &dataDispatcher{owner: o}
	if e := c.dispatchSource(f.ctx, 0, state); e != nil {
		t.Fatal(e)
	}
	tx, _ := f.r.begin()
	defer tx.Rollback()
	done, e := c.completedIn(f.ctx, tx, 0)
	if e != nil || done != 0 || state.completed != 3 || len(state.window) != 0 {
		t.Fatal(done, state, e)
	}
	head, _ := a.graphReplica.journal.Head(f.ctx)
	if head.Committed.Sequence != 2 {
		t.Fatal("recovery rebuilt committed events")
	}
}

func TestDispatchProgressNeedsNoCommit(t *testing.T) {
	f, c, o := routedFixture(t)
	a := f.services[0]
	f.write(t, "first", false)
	q := deliveryFor(t, f, o, 0)
	if e := a.graphReplica.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	state := &dataDispatcher{owner: o}
	f.db.commitErr = injected
	if e := c.dispatchSource(f.ctx, 0, state); e != nil || state.completed != 1 {
		t.Fatal("dispatch progress depended on a storage commit", state.completed, e)
	}
	f.db.commitErr = nil
	f.write(t, "second", false)
	if e := a.graphReplica.enqueue(f.ctx, deliveryFor(t, f, o, 1)); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := c.dispatchSource(f.ctx, 0, state); e != nil {
		t.Fatal(e)
	}
	if state.completed != 2 || len(state.window) != 0 {
		t.Fatalf("dispatcher stuck after in-memory advance: completed=%d window=%v", state.completed, state.window)
	}
}

func TestRoutedRejectsIncompleteOrChangedDelivery(t *testing.T) {
	for _, kind := range []string{"omitted", "changed", "invented"} {
		t.Run(kind, func(t *testing.T) {
			f, _, o := routedFixture(t)
			f.write(t, "first", false)
			f.write(t, "second", false)
			q := deliveryFor(t, f, o, 0)
			switch kind {
			case "omitted":
				q.Events = q.Events[1:]
			case "changed":
				q.Events[0].Record.Vector[0] += 10
			case "invented":
				q.Through++
				q.Events = append(q.Events, LogEvent{Sequence: q.Through, Record: q.Events[0].Record})
			}
			g := f.services[0].graphReplica
			if e := g.enqueue(f.ctx, q); e == nil {
				t.Fatal("accepted delivery that does not cover the durable source log")
			}
			h, _ := g.journal.Head(f.ctx)
			if len(g.input.queue) != 0 || h.Sources["0"] != 0 {
				t.Fatal("invalid delivery advanced state")
			}
		})
	}
}

func TestRoutedAdmissionOwnsValidatedRecords(t *testing.T) {
	f, _, o := routedFixture(t)
	f.write(t, "owned", false)
	q := deliveryFor(t, f, o, 0)
	g := f.services[0].graphReplica
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	q.Events[0].Record.Vector[0] = 999
	if e := g.enqueue(f.ctx, q); e == nil {
		t.Fatal("changed retry accepted")
	}
	if e := f.services[0].Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if f.services[0].Index.records["owned"].Vector[0] != 1 || g.input.bytes != 0 {
		t.Fatal("queued records mutated or byte accounting leaked")
	}
}

func TestDispatchCompletedPrefixAllWatermarks(t *testing.T) {
	// Exhaustively compare sparse destinations and broadcast deletes against an
	// independent per-event model. A later ACK must never bridge an earlier hole.
	events := []LogEvent{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}, {Sequence: 4}}
	targets := func(e LogEvent) []IndexPartitionID {
		return [][]IndexPartitionID{{"a"}, {"b"}, {"a", "b"}, {"b"}}[e.Sequence-1]
	}
	for a := uint64(0); a <= 4; a++ {
		for b := uint64(0); b <= 4; b++ {
			acks := map[IndexPartitionID]uint64{"a": a, "b": b}
			want := uint64(4)
			for _, event := range events {
				for _, target := range targets(event) {
					if acks[target] < event.Sequence {
						want = min(want, event.Sequence-1)
					}
				}
			}
			if got := completedPrefix(events, 0, targets, acks); got != want {
				t.Fatalf("acks=%v got=%d want=%d", acks, got, want)
			}
		}
	}
}

func TestRestartListsRetainedRangeAtCompletedCursor(t *testing.T) {
	f, c, owner := routedFixture(t)
	for i := 1; i <= 6; i++ {
		f.write(t, fmt.Sprint(i), false)
	}
	r := c.data[0]
	seed(f.db, []byte(r.Prefix+"log-floor"), uint64(2))
	delete(f.db.data, string(r.logKey(1)))
	delete(f.db.data, string(r.logKey(2)))
	seed(f.db, completedKey(r), uint64(4))
	restart := func() (*dataDispatcher, error) {
		s := &dataDispatcher{owner: owner}
		return s, c.loadDispatchWindow(f.ctx, 0, s)
	}
	s, err := restart()
	if err != nil || s.completed != 4 || len(s.window) != 2 || s.window[0].Sequence != 5 || s.window[1].Sequence != 6 {
		t.Fatal(s, err)
	}
	// Even a hole before completed must not be mistaken for valid compaction.
	delete(f.db.data, string(r.logKey(3)))
	if _, err = restart(); err == nil {
		t.Fatal("accepted missing first retained key")
	}
	for i := uint64(4); i <= 6; i++ {
		delete(f.db.data, string(r.logKey(i)))
	}
	seed(f.db, []byte(r.Prefix+"log-floor"), uint64(6))
	seed(f.db, completedKey(r), uint64(6))
	s, err = restart()
	if err != nil || s.completed != 6 || len(s.window) != 0 {
		t.Fatal(s, err)
	}
	f.write(t, "next", false)
	if err = c.loadDispatchWindow(f.ctx, 0, s); err != nil || len(s.window) != 1 || s.window[0].Sequence != 7 {
		t.Fatal(s, err)
	}
}

func TestDispatchProjectionReusesWindowAcrossPartialACKs(t *testing.T) {
	_, c, _ := routedFixture(t)
	c.layout.Routing = &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}, {10, 0}}}
	c.layout.Bindings = []PartitionBinding{{Index: "index-0000"}, {Index: "index-0001"}}
	c.router = clusterRouter{IdentityPartitionRouter{c.layout}, c.layout.Routing}
	rows := make([]LogEvent, dispatchBatchSize+1)
	for n := range rows {
		rows[n] = LogEvent{Sequence: uint64(n + 1), Record: Record{Vector: []float64{float64(n%2) * 10, 0}}}
	}
	rows[2].Record = Record{Deleted: true}
	var p dispatchProjection
	p.update(c, rows[:dispatchBatchSize])
	first := &p.routed[0]
	p.update(c, rows[:dispatchBatchSize])
	if first != &p.routed[0] || len(p.destinations(rows[2])) != 2 {
		t.Fatal("recomputed the same window or lost a broadcast")
	}
	p.update(c, rows[2:dispatchBatchSize])
	for _, id := range p.targets {
		if p.byIndex[id][0].Sequence != 3 {
			t.Fatal("partial ACK discarded a broadcast or retained an acknowledged prefix")
		}
	}
	p.update(c, rows[dispatchBatchSize:])
	if len(p.targets) != 1 || len(p.routed) != 1 || len(p.byIndex["index-0000"]) != 1 {
		t.Fatal("new window retained events from its predecessor")
	}
}

func TestCommittedWriteWakesItsDispatcherDuringControlWork(t *testing.T) {
	_, c, _ := routedFixture(t)
	c.dispatchSignals, c.outboundSignals = newSourceSignals(), newSourceSignals()
	wake := make(chan struct{}, 1)
	c.writers = map[DataPartitionID]*mutationGroup{0: {dispatchWake: wake}}
	c.signalSource(0, "write", true)
	c.signalSource(0, "graph_commit", true)
	if len(wake) != 1 || len(c.dispatchSignals.take()) != 0 {
		t.Fatal("source wake waited for the control loop")
	}
}

func TestTxnDispatchLookaheadAndWireBounds(t *testing.T) {
	f, c, owner := routedFixture(t)
	c.token = "test-source-token"
	s := &dataDispatcher{owner: owner, writer: &mutationGroup{lease: &appendLease{owner: owner}}}
	for n := uint64(1); n <= dispatchLookahead+1; n++ {
		e := LogEvent{Sequence: n, Record: Record{ID: fmt.Sprint(n), Revision: 1, Vector: []float64{1, 2}}}
		seed(f.db, f.r.logKey(n), e)
	}
	s.writer.lease.head.Store(dispatchLookahead + 1)
	if err := c.loadDispatchWindow(f.ctx, 0, s); err != nil || len(s.window) != dispatchLookahead {
		t.Fatal("unexpected source lookahead", len(s.window), err)
	}
	id := c.layout.Bindings[0].Index
	q := Delivery{Source: 0, Owner: owner, RouteVersion: c.layout.Epoch, Through: dispatchLookahead, Events: s.window[dispatchLookahead-1:]}
	if err := c.validateDeliveryBounds(f.ctx, nil, id, q, 0); err == nil {
		t.Fatal("unsigned sparse interval exceeded its verification window")
	}
	q.verified = true
	if err := c.validateDeliveryBounds(f.ctx, nil, id, q, 0); err != nil {
		t.Fatal(err)
	}
	q.Events = s.window
	if err := c.validateDeliveryBounds(f.ctx, nil, id, q, 0); err == nil {
		t.Fatal("accepted too many records in an authenticated delivery")
	}
	q.Through++
	q.Events = []LogEvent{{Sequence: q.Through, Record: s.window[0].Record}}
	if err := c.validateDeliveryBounds(f.ctx, nil, id, q, 0); err == nil {
		t.Fatal("accepted a span larger than the source lookahead")
	}
}

func TestRoutedPruneDoesNotWaitForGraphConstruction(t *testing.T) {
	f, _, _ := routedFixture(t)
	enableReferences(f)
	s := f.services[0]
	s.syncMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := s.Prune(f.ctx); done <- err }()
	select {
	case err := <-done:
		s.syncMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		s.syncMu.Unlock()
		<-done
		t.Fatal("routed GC waited for graph construction")
	}
}

func TestRoutedLogNamespacesSeparateReceiptsAndGraphConsumers(t *testing.T) {
	_, c, _ := routedFixture(t)
	r := *c.data[0]
	r.collectionPrefix = "tenant-a/"
	r.Prefix = r.collectionPrefix + "data/0000/"
	r.packed = &packedStore{namespace: r.collectionPrefix, count: 1}
	c.root = &Repository{Prefix: r.collectionPrefix, packed: r.packed}
	c.data[0] = &r
	index := c.layout.Bindings[0].Index
	consumer, err := c.changeSource(ChangeRequest{Partition: index, Stream: "graph"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(consumer.consumerKey("graph")), string(c.indexRepository(index).consumerKey("graph")); got != want || got != "tenant-a/index/"+string(index)+"/consumers/graph" {
		t.Fatal("consumer registration and GC diverged", got, want)
	}
	if got := string(r.key("request", "x")); got != "tenant-a/data/0000/request/78" {
		t.Fatal("unscoped receipt", got)
	}
}
