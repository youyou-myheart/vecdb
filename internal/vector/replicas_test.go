package vector

import (
	"context"
	"errors"
	"testing"
)

func TestReplicaMigrationAndPublisherFence(t *testing.T) {
	ctx := context.Background()
	r, _ := newFakeRepo()
	c := &Coordinator{root: r, ttl: 60000}
	layout, _ := newLayout(1)
	master := ClusterNode{ID: "m", Session: "m", URL: "http://m", Roles: []string{RoleMaster}}
	_ = c.Heartbeat(ctx, master)
	leader, e := c.Acquire(ctx, masterLeaseID, master)
	if e != nil {
		t.Fatal(e)
	}
	nodes := []ClusterNode{{ID: "a", Session: "a", URL: "http://a", Roles: []string{RoleWorker}}, {ID: "b", Session: "b", URL: "http://b", Roles: []string{RoleWorker}}}
	for _, n := range nodes {
		_ = c.Heartbeat(ctx, n)
	}
	if e = c.ScheduleReplicas(ctx, leader, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	p, _ := c.Plan(ctx, "index-0000")
	owners := map[IndexPartitionID]Ownership{}
	for _, a := range p.Desired {
		var node ClusterNode
		for _, n := range nodes {
			if n.ID == a.Node {
				node = n
			}
		}
		o, e := c.Acquire(ctx, a.Partition, node)
		if e != nil {
			t.Fatal(e)
		}
		o, e = c.Update(ctx, o, true, false)
		if e != nil {
			t.Fatal(e)
		}
		owners[a.Partition] = o
	}
	if e = c.ScheduleReplicas(ctx, leader, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	p, _ = c.Plan(ctx, "index-0000")
	for id, o := range owners {
		tx, _ := r.begin()
		e = c.ReplicaFence("index-0000", o)(ctx, tx)
		_ = tx.Rollback()
		if (id == p.Publisher) != (e == nil) {
			t.Fatalf("publisher %s id %s error %v", p.Publisher, id, e)
		}
	}
	// Pin a migration barrier above the fresh replacement's offset.
	tx, _ := r.begin()
	_ = putJSON(tx, []byte(r.Prefix+"log-head"), uint64(50))
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	for _, owner := range owners {
		if e = c.Progress(ctx, owner, 50); e != nil {
			t.Fatal(e)
		}
	}
	// Drain one worker: old replica remains until the replacement has loaded.
	nodes[0].Draining = true
	third := ClusterNode{ID: "c", Session: "c", URL: "http://c", Roles: []string{RoleWorker}}
	_ = c.Heartbeat(ctx, third)
	nodes = append(nodes, third)
	if e = c.ScheduleReplicas(ctx, leader, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	p, _ = c.Plan(ctx, "index-0000")
	if len(p.Serving) != 3 {
		t.Fatalf("retired before readiness: %+v", p)
	}
	id := replicaID("index-0000", third)
	o, e := c.Acquire(ctx, id, third)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Update(ctx, o, true, false)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.ScheduleReplicas(ctx, leader, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	p, _ = c.Plan(ctx, "index-0000")
	if len(p.Serving) != 3 || p.Barrier != 50 {
		t.Fatalf("ready alone bypassed barrier: %+v", p)
	}
	if e = c.Progress(ctx, o, 50); e != nil {
		t.Fatal(e)
	}
	if e = c.ScheduleReplicas(ctx, leader, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	p, _ = c.Plan(ctx, "index-0000")
	if len(p.Serving) != 2 {
		t.Fatal(p)
	}
	old := owners[replicaID("index-0000", nodes[0])]
	if _, e = c.Update(ctx, old, true, false); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
}

func TestReplicaRetentionAndProtocolIsolation(t *testing.T) {
	ctx := context.Background()
	r, _ := newFakeRepo()
	c := &Coordinator{root: r, ttl: 60000}
	if e := r.ensureClusterMode(ctx, 2); e != nil {
		t.Fatal(e)
	}
	if e := r.ensureClusterMode(ctx, 1); e == nil {
		t.Fatal("legacy worker entered replica namespace")
	}
	tx, _ := r.begin()
	p := ReplicaPlan{Serving: []Assignment{{Partition: "a"}, {Partition: "b"}}}
	_ = putJSON(tx, c.planKey("index-0000"), p)
	_ = putJSON(tx, c.ownerKey("a"), Ownership{Partition: "a", Offset: 100})
	_ = putJSON(tx, c.ownerKey("b"), Ownership{Partition: "b", Offset: 20})
	if e := tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	tx, _ = r.begin()
	end, e := c.RetainReplicas("index-0000")(ctx, tx, 80)
	_ = tx.Rollback()
	if e != nil || end != 20 {
		t.Fatalf("pruned beyond slow replica: %d %v", end, e)
	}
	tx, _ = r.begin()
	p.Serving = append(p.Serving, Assignment{Partition: "warming"})
	_ = putJSON(tx, c.planKey("index-0000"), p)
	_ = tx.Commit(ctx)
	tx, _ = r.begin()
	end, e = c.RetainReplicas("index-0000")(ctx, tx, 80)
	_ = tx.Rollback()
	if e != nil || end != 0 {
		t.Fatalf("warming replica failed to pin log: %d %v", end, e)
	}
}
