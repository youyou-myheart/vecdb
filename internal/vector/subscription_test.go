package vector

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestSubscriptionsDurablePrefixAndResume(t *testing.T) {
	f := newGraphFixture(t)
	s := f.services[0]
	g := s.graphReplica
	p := &PartitionedService{layout: f.layout, data: map[DataPartitionID]*Repository{0: f.r}}
	c := &ClusterService{PartitionedService: p, coordinator: f.c, graphReplication: true}
	first := f.write(t, "a", false)
	f.write(t, "b", false)
	q := ChangeRequest{Partition: "index-0000", Stream: "data", Limit: 1}
	page, e := c.ReadChanges(f.ctx, q)
	if e != nil || len(page.Data) != 1 || page.Next != 1 || page.Head != 2 || page.Data[0].Record.ID != first.ID {
		t.Fatal(page, e)
	}
	q.After = page.Next
	page, e = c.ReadChanges(f.ctx, q)
	if e != nil || page.Next != 2 || len(page.Data) != 1 {
		t.Fatal(page, e)
	}
	q.After = 2
	page, e = c.ReadChanges(f.ctx, q)
	if e != nil || page.Next != 2 || len(page.Data) != 0 {
		t.Fatal(page, e)
	}
	// A durable graph entry is immediately subscribable, before any local apply.
	head, e := g.journal.Claim(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	batch, _ := f.r.ReadEvents(f.ctx, 0, 0, 256)
	records := []Record{batch.Events[0].Record, batch.Events[1].Record}
	delta, e := s.Index.GenerateDelta(records)
	if e != nil {
		t.Fatal(e)
	}
	if len(delta.Links) != 2 {
		t.Fatal("missing materialized graph links", delta.Links)
	}
	data, _ := json.Marshal(delta)
	object, e := f.objects.saveBytes(f.ctx, f.r.Namespace(), 2, deltaFormat, data)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = g.journal.Commit(f.ctx, head, 2, "delta", object); e != nil {
		t.Fatal(e)
	}
	q = ChangeRequest{Partition: "index-0000", Stream: "graph", Limit: 1}
	page, e = c.ReadChanges(f.ctx, q)
	if e != nil || page.Head != 1 || len(page.Graph) != 1 || s.Cursor() != 0 {
		t.Fatal("durable event not exposed before local apply", page, e)
	}
	if e = s.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	page, e = c.ReadChanges(f.ctx, q)
	if e != nil || page.Head != 1 || page.Next != 1 || len(page.Graph) != 1 || page.Graph[0].Object.SHA256 != object.SHA256 {
		t.Fatal(page, e)
	}
	// Commit no writes: retrying the same exclusive cursor yields identical entry.
	retry, e := c.ReadChanges(f.ctx, q)
	if e != nil || retry.Graph[0].Position != page.Graph[0].Position {
		t.Fatal(retry, e)
	}
	for _, bad := range []ChangeRequest{{Partition: "missing", Stream: "data"}, {Partition: "index-0000", Stream: "wrong"}, {Partition: "index-0000", Stream: "data", Limit: 257}, {Partition: "index-0000", Stream: "graph", After: 2}} {
		if _, e = c.ReadChanges(f.ctx, bad); !errors.Is(e, ErrInvalidSubscription) {
			t.Fatal(bad, e)
		}
	}
	tx, _ := f.r.begin()
	_ = putJSON(tx, []byte(f.r.Prefix+"log-floor"), uint64(1))
	if e = tx.Commit(f.ctx); e != nil {
		t.Fatal(e)
	}
	page, e = c.ReadChanges(f.ctx, ChangeRequest{Partition: "index-0000", Stream: "data"})
	if e != nil || !page.ResetRequired || len(page.Data) != 0 || page.Floor != 1 || page.Next != 0 {
		t.Fatal("compaction silently skipped events", page, e)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, e = c.ReadChanges(ctx, q); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
