package vector

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMasterElectionScheduleAndStaleLeader(t *testing.T) {
	ctx := context.Background()
	root, db := newFakeRepo()
	db.nowMillis = 1000
	c := &Coordinator{root: root, ttl: 6000}
	layout, _ := newLayout(4)
	a := ClusterNode{ID: "m-a", Session: "ma1", URL: "http://localhost:1", Roles: []string{RoleMaster}}
	b := ClusterNode{ID: "m-b", Session: "mb1", URL: "http://localhost:2", Roles: []string{RoleMaster}}
	w := ClusterNode{ID: "worker", Session: "w1", URL: "http://localhost:3", Roles: []string{RoleWorker}}
	g := ClusterNode{ID: "gateway", Session: "g1", URL: "http://localhost:4", Roles: []string{RoleGateway}}
	for _, n := range []ClusterNode{a, b, w, g} {
		if e := c.Heartbeat(ctx, n); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := c.Acquire(ctx, masterLeaseID, w); !errors.Is(e, ErrFenced) {
		t.Fatal("worker became master", e)
	}
	ma := Master{coordinator: c, node: a, layout: layout}
	mb := Master{coordinator: c, node: b, layout: layout}
	nodes, _ := c.Nodes(ctx)
	if e := ma.Tick(ctx, nodes); e != nil {
		t.Fatal(e)
	}
	first, e := c.Resolve(ctx, masterLeaseID)
	if e != nil || first.Node != a.ID || first.Epoch != 1 {
		t.Fatal(first, e)
	}
	begin := root.beginTxn
	root.beginTxn = func() (transactionAPI, error) { panic("InLeaderLease accessed KV") }
	var before, after uint64
	if !ma.InLeaderLease(&before) || !ma.InLeaderLease(&after) || before != first.Epoch || before != after || mb.InLeaderLease(nil) {
		t.Fatal("bad local master lease", before, after)
	}
	root.beginTxn = begin
	if e = mb.Tick(ctx, nodes); e != nil {
		t.Fatal(e)
	}
	if mb.InLeaderLease(nil) {
		t.Fatal("standby claimed local authority")
	}
	still, _ := c.Resolve(ctx, masterLeaseID)
	if !sameOwner(first, still) {
		t.Fatal("standby displaced leader")
	}
	for _, binding := range layout.Bindings {
		assignment, e := c.Assignment(ctx, binding.Index)
		if e != nil || assignment.Node != w.ID || assignment.LeaderEpoch != 1 {
			t.Fatal(assignment, e)
		}
	}
	workerLease, e := c.Acquire(ctx, "index-0000", w)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Update(ctx, workerLease, true, false); e != nil {
		t.Fatal(e)
	}
	// Master expiry must not prevent an otherwise valid worker from renewing.
	db.nowMillis = 5000
	if _, e = c.Update(ctx, workerLease, true, false); e != nil {
		t.Fatal(e)
	}
	db.nowMillis = 8000
	if _, e = c.Resolve(ctx, masterLeaseID); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	if _, e = c.Update(ctx, workerLease, true, false); e != nil {
		t.Fatal("worker depends on active master", e)
	}
	for _, n := range []ClusterNode{b, w, g} {
		if e = c.Heartbeat(ctx, n); e != nil {
			t.Fatal(e)
		}
	}
	nodes, _ = c.Nodes(ctx)
	if e = mb.Tick(ctx, nodes); e != nil {
		t.Fatal(e)
	}
	second, _ := c.Resolve(ctx, masterLeaseID)
	if second.Node != b.ID || second.Epoch != 2 {
		t.Fatal(second)
	}
	if !mb.InLeaderLease(&after) || after != second.Epoch || after == before {
		t.Fatal("takeover reused the old epoch", before, after)
	}
	if e = c.Heartbeat(ctx, a); e != nil {
		t.Fatal(e)
	}
	if e = ma.Tick(ctx, nodes); e != nil || ma.InLeaderLease(nil) {
		t.Fatal("old master kept local authority after observing takeover", e)
	}
	if e = c.Schedule(ctx, first, layout, nil); !errors.Is(e, ErrFenced) {
		t.Fatal("old master changed plans", e)
	}
	for _, binding := range layout.Bindings {
		assignment, _ := c.Assignment(ctx, binding.Index)
		if assignment.Node != w.ID || assignment.LeaderEpoch != 2 {
			t.Fatal(assignment)
		}
	}
}
func TestRolesAndAssignmentEnforcement(t *testing.T) {
	for _, value := range []string{"master,master", "unknown", ",worker"} {
		if _, e := parseRoles(value); e == nil {
			t.Fatal(value)
		}
	}
	roles, e := parseRoles("")
	if e != nil || len(roles) != 3 {
		t.Fatal(roles, e)
	}
	ctx := context.Background()
	root, _ := newFakeRepo()
	c := &Coordinator{root: root, ttl: 6000}
	a := ClusterNode{ID: "a", Session: "a", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	b := a
	b.ID = "b"
	b.Session = "b"
	for _, n := range []ClusterNode{a, b} {
		if e = c.Heartbeat(ctx, n); e != nil {
			t.Fatal(e)
		}
	}
	testAssignment(t, c, a, "index-0000")
	if _, e = c.Acquire(ctx, "index-0000", b); !errors.Is(e, ErrFenced) {
		t.Fatal("unassigned worker acquired", e)
	}
	owner, e := c.Acquire(ctx, "index-0000", a)
	if e != nil {
		t.Fatal(e)
	}
	testAssignment(t, c, b, "index-0000")
	if _, e = c.Update(ctx, owner, true, false); !errors.Is(e, ErrFenced) {
		t.Fatal("old assignment renewed", e)
	}
	if _, e = c.Update(ctx, owner, false, true); e != nil {
		t.Fatal("old assignment could not release", e)
	}
}

func TestOpenClusterRejectsInvalidConfigBeforeStorage(t *testing.T) {
	base := ClusterConfig{NodeID: "node", AdvertiseURL: "http://localhost:1", Token: "cluster-secret-long", LeaseTTL: 6 * time.Second}
	for _, change := range []func(*ClusterConfig){func(c *ClusterConfig) { c.Token = "short" }, func(c *ClusterConfig) { c.LeaseTTL = time.Second }, func(c *ClusterConfig) { c.Roles = "invalid" }, func(c *ClusterConfig) { c.AdvertiseURL = "file:///tmp/x" }} {
		cfg := base
		change(&cfg)
		if _, e := OpenClusterService(context.Background(), nil, nil, 4, cfg); e == nil {
			t.Fatal("invalid config accepted")
		}
	}
}

func TestMasterScheduleRetriesOnlyDefiniteConflictsAndRechecksFence(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		f := newGraphFixture(t)
		calls := 0
		err := retryMasterSchedule(f.ctx, func(attempt int) error {
			calls++
			if attempt == 0 {
				if revoke {
					if _, e := f.c.Update(f.ctx, f.master, false, true); e != nil {
						t.Fatal(e)
					}
				}
				return indexWriteConflict()
			}
			return f.c.ScheduleReplicas(f.ctx, f.master, f.layout, nil, 2)
		})
		if calls != 2 || (revoke && !errors.Is(err, ErrFenced)) || (!revoke && err != nil) {
			t.Fatal(calls, err)
		}
	}
	for _, expected := range []error{injected, ErrFenced, ErrConflict} {
		calls := 0
		err := retryMasterSchedule(context.Background(), func(int) error { calls++; return expected })
		if !errors.Is(err, expected) || calls != 1 {
			t.Fatal(calls, err)
		}
	}
	calls := 0
	if err := retryMasterSchedule(context.Background(), func(int) error { calls++; return indexWriteConflict() }); err == nil || calls != 4 {
		t.Fatal(calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls = 0
	err := retryMasterSchedule(ctx, func(int) error { calls++; cancel(); return indexWriteConflict() })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(calls, err)
	}
}
