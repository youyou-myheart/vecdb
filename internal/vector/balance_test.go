package vector

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBalancedInitialPlacement(t *testing.T) {
	r, _ := newFakeRepo()
	c := &Coordinator{root: r, balanceCooldown: time.Second}
	layout, _ := newLayout(32)
	nodes := []ClusterNode{}
	for i := 0; i < 4; i++ {
		nodes = append(nodes, ClusterNode{ID: fmt.Sprint(i), Session: "s", Capacity: 1})
	}
	tx, _ := r.begin()
	defer tx.Rollback()
	targets, e := c.balancedTargets(context.Background(), tx, layout, nodes, 2)
	if e != nil {
		t.Fatal(e)
	}
	counts := map[string]int{}
	for _, list := range targets {
		if len(list) != 2 || list[0].ID == list[1].ID {
			t.Fatal(list)
		}
		for _, n := range list {
			counts[n.ID]++
		}
	}
	for _, n := range nodes {
		if counts[n.ID] != 16 {
			t.Fatal(counts)
		}
	}
}
func TestBalanceHotShardCooldownAndMigrationGate(t *testing.T) {
	ctx := context.Background()
	r, db := newFakeRepo()
	db.nowMillis = 100000
	c := &Coordinator{root: r, balanceCooldown: 10 * time.Second}
	layout, _ := newLayout(8)
	nodes := []ClusterNode{{ID: "a", Session: "s", Capacity: 1, Load: map[string]ShardLoad{"index-0000": {EstimatedBytes: 512 << 20}}}, {ID: "b", Session: "s", Capacity: 1}, {ID: "c", Session: "s", Capacity: 1}}
	seed, _ := r.begin()
	for _, b := range layout.Bindings {
		p := ReplicaPlan{}
		for _, n := range nodes[:2] {
			a := Assignment{Partition: replicaID(b.Index, n), Node: n.ID, Session: n.Session}
			p.Desired = append(p.Desired, a)
			p.Serving = append(p.Serving, a)
			_ = putJSON(seed, c.ownerKey(a.Partition), Ownership{Partition: a.Partition, Ready: true, Expires: 200000})
		}
		_ = putJSON(seed, c.planKey(b.Index), p)
	}
	_ = seed.Commit(ctx)
	tx, _ := r.begin()
	targets, e := c.balancedTargets(ctx, tx, layout, nodes, 2)
	if e != nil {
		t.Fatal(e)
	}
	if !hasNode(targets["index-0000"], "c") {
		t.Fatal("did not move hot shard", targets)
	}
	moved := 0
	for _, n := range targets {
		if hasNode(n, "c") {
			moved++
		}
	}
	if moved != 1 {
		t.Fatal(moved)
	}
	_ = tx.Commit(ctx)
	tx, _ = r.begin()
	targets, e = c.balancedTargets(ctx, tx, layout, nodes, 2)
	_ = tx.Rollback()
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range targets {
		if hasNode(n, "c") {
			t.Fatal("cooldown ignored")
		}
	}
	db.nowMillis = 120000
	tx, _ = r.begin()
	o, _ := c.readOwner(ctx, tx, replicaID("index-0000", nodes[0]))
	o.Ready = false
	_ = putJSON(tx, c.ownerKey(o.Partition), o)
	_ = tx.Commit(ctx)
	tx, _ = r.begin()
	targets, e = c.balancedTargets(ctx, tx, layout, nodes, 2)
	_ = tx.Rollback()
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range targets {
		if hasNode(n, "c") {
			t.Fatal("moved while previous replica is not ready")
		}
	}
}
func TestSchedulerProtocolAndConfiguration(t *testing.T) {
	ctx := context.Background()
	r, _ := newFakeRepo()
	if e := r.ensureLoadMode(ctx); e != nil {
		t.Fatal(e)
	}
	if e := r.ensureClusterMode(ctx, 2); e == nil {
		t.Fatal("hash mode accepted load namespace")
	}
	if e := r.ensureScheduler(ctx, "load", time.Second); e != nil {
		t.Fatal(e)
	}
	if e := r.ensureScheduler(ctx, "load", 2*time.Second); e == nil {
		t.Fatal("mixed cooldown accepted")
	}
}

func TestWorkerCapacityWeights(t *testing.T) {
	r, _ := newFakeRepo()
	c := &Coordinator{root: r}
	layout, _ := newLayout(32)
	nodes := []ClusterNode{{ID: "large", Session: "s", Capacity: 2}, {ID: "small-a", Session: "s", Capacity: 1}, {ID: "small-b", Session: "s", Capacity: 1}, {ID: "small-c", Session: "s", Capacity: 1}}
	tx, _ := r.begin()
	defer tx.Rollback()
	targets, e := c.balancedTargets(context.Background(), tx, layout, nodes, 2)
	if e != nil {
		t.Fatal(e)
	}
	counts := map[string]int{}
	for _, list := range targets {
		for _, n := range list {
			counts[n.ID]++
		}
	}
	if counts["large"] < 24 || counts["large"] > 28 {
		t.Fatal(counts)
	}
	for _, n := range nodes[1:] {
		if counts[n.ID] < 11 || counts[n.ID] > 14 {
			t.Fatal(counts)
		}
	}
}
