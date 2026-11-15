package vector

import (
	"context"
	"errors"
	"testing"
)

func TestOwnershipLifecycleAndFencing(t *testing.T) {
	ctx := context.Background()
	root, db := newFakeRepo()
	db.nowMillis = 1000
	c := &Coordinator{root: root, ttl: 6000}
	a := ClusterNode{Roles: []string{RoleWorker}, ID: "a", Session: "a1", URL: "http://127.0.0.1:1"}
	b := ClusterNode{Roles: []string{RoleWorker}, ID: "b", Session: "b1", URL: "http://127.0.0.1:2"}
	if e := c.Heartbeat(ctx, a); e != nil {
		t.Fatal(e)
	}
	duplicate := a
	duplicate.Session = "a2"
	if e := c.Heartbeat(ctx, duplicate); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	testAssignment(t, c, a, "index-0000")
	owner, e := c.Acquire(ctx, "index-0000", a)
	if e != nil || owner.Epoch != 1 {
		t.Fatal(owner, e)
	}
	if _, e = c.Resolve(ctx, owner.Partition); !errors.Is(e, ErrFenced) {
		t.Fatal("building index ready", e)
	}
	if _, e = c.Update(ctx, owner, true, false); e != nil {
		t.Fatal(e)
	}
	if e = c.Check(ctx, owner); e != nil {
		t.Fatal(e)
	}
	// Renewing from a cached not-ready lease cannot demote a completed load.
	if _, e = c.Update(ctx, owner, false, false); e != nil {
		t.Fatal(e)
	}
	if e = c.Check(ctx, owner); e != nil {
		t.Fatal(e)
	}
	if e = c.Heartbeat(ctx, b); e != nil {
		t.Fatal(e)
	}
	testAssignment(t, c, b, owner.Partition)
	if _, e = c.Acquire(ctx, owner.Partition, b); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	db.nowMillis = 8000
	if e = c.Check(ctx, owner); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	if _, e = c.Update(ctx, owner, true, false); !errors.Is(e, ErrFenced) {
		t.Fatal("expired owner renewed", e)
	}
	if e = c.Heartbeat(ctx, b); e != nil {
		t.Fatal(e)
	}
	testAssignment(t, c, b, owner.Partition)
	next, e := c.Acquire(ctx, owner.Partition, b)
	if e != nil || next.Epoch != 2 {
		t.Fatal(next, e)
	}
	if _, e = c.Update(ctx, next, true, false); e != nil {
		t.Fatal(e)
	}
	root.Fence = c.Fence(owner)
	if e = root.Publish(ctx, "", Manifest{Generation: "stale"}); !errors.Is(e, ErrFenced) {
		t.Fatal("stale publish", e)
	}
	if _, e = root.PruneEvents(ctx, 256); !errors.Is(e, ErrFenced) {
		t.Fatal("stale prune", e)
	}
	if _, ok := db.data[root.Namespace()+"manifest"]; ok {
		t.Fatal("stale manifest committed")
	}
	root.Fence = c.Fence(next)
	if e = root.Publish(ctx, "", Manifest{Generation: "current"}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Update(ctx, next, false, true); e != nil {
		t.Fatal(e)
	}
	if e = c.Check(ctx, next); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
}
func TestCoordinatorPlacementAndMode(t *testing.T) {
	nodes := []ClusterNode{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	reversed := []ClusterNode{nodes[2], nodes[1], nodes[0]}
	for _, id := range []IndexPartitionID{"index-0000", "index-0001", "index-0002"} {
		if preferredOwner(id, nodes) != preferredOwner(id, reversed) {
			t.Fatal("unstable routing")
		}
	}
	root, db := newFakeRepo()
	ctx := context.Background()
	if e := root.ensureMode(ctx, false); e != nil {
		t.Fatal(e)
	}
	if e := root.ensureMode(ctx, true); e != nil {
		t.Fatal(e)
	}
	if e := root.ensureMode(ctx, false); e == nil {
		t.Fatal("single process bypass")
	}
	if string(db.data[root.Prefix+"schema"]) != "v5:dimension=2:metric=l2:cluster-master=1" {
		t.Fatal("old binary not excluded")
	}
	c := &Coordinator{root: root, ttl: 3000}
	for _, u := range []string{"file:///tmp/x", "http://host/path", "http://user:pass@host", ""} {
		if e := c.Heartbeat(ctx, ClusterNode{Roles: []string{RoleWorker}, ID: "n", Session: "s", URL: u}); e == nil {
			t.Fatal(u)
		}
	}
}

// Fixture for isolated worker-lease tests; production assignment writes use Schedule.
func testAssignment(t *testing.T, c *Coordinator, node ClusterNode, id IndexPartitionID) {
	t.Helper()
	tx, e := c.root.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if e = putJSON(tx, c.assignmentKey(id), Assignment{Partition: id, Node: node.ID, Session: node.Session, URL: node.URL, LeaderEpoch: 1}); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(context.Background()); e != nil {
		t.Fatal(e)
	}
}
