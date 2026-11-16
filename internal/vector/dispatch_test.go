package vector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
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
		s.graphReplica.input = &routedInput{cluster: c, queue: map[DataPartitionID]*Delivery{}}
	}
	return f, c, owner
}
func deliveryFor(t *testing.T, f *graphFixture, o Ownership, after uint64) Delivery {
	t.Helper()
	b, e := f.r.ReadEvents(f.ctx, after, 0, dispatchBatchSize)
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
	if h.Committed.Sequence != 0 || h.Sources["0"] != 0 {
		t.Fatal("enqueue advanced durability")
	}
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	h, _ = g.journal.Head(f.ctx)
	if h.Sources["0"] != 3 || h.Committed.Sequence != 1 {
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
	if h.Sources["0"] != 3 || h.Committed.Sequence != 1 {
		t.Fatal("partial watermark commit", h)
	}
	if e = g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if e = g.commitRouted(f.ctx, a, h); e != nil {
		t.Fatal(e)
	} // crash after commit, before publish
	pending, _ := g.journal.Head(f.ctx)
	if pending.Sources["0"] != 4 || pending.Committed == pending.Visible {
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
	g.input.queue = map[DataPartitionID]*Delivery{}
	g.input.bytes = 0 // lost memory before commit
	if e := g.enqueue(f.ctx, q); e != nil {
		t.Fatal(e)
	}
	if _, e := f.c.Update(f.ctx, o, false, true); e != nil {
		t.Fatal(e)
	}
	if e := f.services[0].Sync(f.ctx); !errors.Is(e, ErrFenced) {
		t.Fatal("expired dispatcher applied", e)
	}
	h, _ := g.journal.Head(f.ctx)
	if h.Committed.Sequence != 0 {
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
	if e != nil || done != 3 || len(state.window) != 0 {
		t.Fatal(done, state, e)
	}
	head, _ := a.graphReplica.journal.Head(f.ctx)
	if head.Committed.Sequence != 1 {
		t.Fatal("recovery rebuilt committed events")
	}
}
