package vector

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorkerOnlyFollowsMasterAssignment(t *testing.T) {
	root, db := newFakeRepo()
	db.nowMillis = 1000
	ctx := context.Background()
	coord := &Coordinator{root: root, ttl: 6000}
	node := ClusterNode{ID: "w", Session: "s", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	if e := coord.Heartbeat(ctx, node); e != nil {
		t.Fatal(e)
	}
	layout, _ := newLayout(1)
	c := &ClusterService{PartitionedService: &PartitionedService{layout: layout}, coordinator: coord, node: node, locals: map[IndexPartitionID]*localPartition{}}
	worker := Worker{cluster: c}
	worker.Tick(ctx)
	if len(c.entries()) != 0 {
		t.Fatal("worker assigned itself without master intent")
	}
	testAssignment(t, coord, node, "index-0000")
	owner, e := coord.Acquire(ctx, "index-0000", node)
	if e != nil {
		t.Fatal(e)
	}
	_, e = coord.Update(ctx, owner, true, false)
	if e != nil {
		t.Fatal(e)
	}
	c.locals[owner.Partition] = &localPartition{owner: owner}
	// No Master exists, but the existing plan and ownership remain valid.
	db.nowMillis = 4000
	worker.Tick(ctx)
	current, e := coord.Resolve(ctx, owner.Partition)
	if e != nil || current.Expires != 10000 {
		t.Fatal(current, e)
	}
	other := node
	other.ID = "other"
	other.Session = "other-session"
	testAssignment(t, coord, other, owner.Partition)
	worker.Tick(ctx)
	if len(c.entries()) != 0 {
		t.Fatal("worker ignored reassignment")
	}
	if _, e = coord.Resolve(ctx, owner.Partition); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
}
func TestRetirementWaitsForLocalReaders(t *testing.T) {
	root, _ := newFakeRepo()
	s, e := NewService(context.Background(), root, partitionObjects())
	if e != nil {
		t.Fatal(e)
	}
	entry := &localPartition{owner: Ownership{Partition: "index-0000"}, service: s}
	c := &ClusterService{locals: map[IndexPartitionID]*localPartition{entry.owner.Partition: entry}}
	entry.mu.RLock()
	c.remove(entry)
	if len(c.entries()) != 0 {
		t.Fatal("retiring graph still routed")
	}
	if _, e = s.Search(Search{Vector: []float64{0, 0}, K: 1}); e != nil {
		t.Fatal("in-flight reader interrupted", e)
	}
	entry.mu.RUnlock()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entry.mu.RLock()
		done := entry.retired && entry.service == nil
		entry.mu.RUnlock()
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("native graph retirement not completed")
}
