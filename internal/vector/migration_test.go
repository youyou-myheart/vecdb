package vector

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type migrationFixture struct {
	t      *testing.T
	ctx    context.Context
	root   *Repository
	db     *fakeDB
	c      *Coordinator
	layout PartitionLayout
	leader Ownership
	nodes  []ClusterNode
	owners map[IndexPartitionID]Ownership
}

func newMigrationFixture(t *testing.T) *migrationFixture {
	r, db := newFakeRepo()
	db.nowMillis = 100000
	f := &migrationFixture{t: t, ctx: context.Background(), root: r, db: db, c: &Coordinator{root: r, ttl: 60000, balanceCooldown: time.Second}, owners: map[IndexPartitionID]Ownership{}}
	f.layout, _ = newLayout(1)
	m := ClusterNode{ID: "master", Session: "m", URL: "http://master", Roles: []string{RoleMaster}}
	if e := f.c.Heartbeat(f.ctx, m); e != nil {
		t.Fatal(e)
	}
	var e error
	f.leader, e = f.c.Acquire(f.ctx, masterLeaseID, m)
	if e != nil {
		t.Fatal(e)
	}
	f.nodes = []ClusterNode{{ID: "a", Session: "a", URL: "http://a", Roles: []string{RoleWorker}}, {ID: "b", Session: "b", URL: "http://b", Roles: []string{RoleWorker}}}
	for _, n := range f.nodes {
		if e = f.c.Heartbeat(f.ctx, n); e != nil {
			t.Fatal(e)
		}
	}
	return f
}
func (f *migrationFixture) tick() ReplicaPlan {
	f.t.Helper()
	if e := f.c.ScheduleReplicas(f.ctx, f.leader, f.layout, f.nodes, 2); e != nil {
		f.t.Fatal(e)
	}
	p, e := f.c.Plan(f.ctx, "index-0000")
	if e != nil {
		f.t.Fatal(e)
	}
	return p
}
func (f *migrationFixture) task(p ReplicaPlan) MigrationTask {
	f.t.Helper()
	tx, _ := f.root.begin()
	defer tx.Rollback()
	task, e := f.c.taskIn(f.ctx, tx, p.TaskID)
	if e != nil {
		f.t.Fatal(e)
	}
	return task
}
func (f *migrationFixture) ready(p ReplicaPlan, offset uint64) {
	f.t.Helper()
	for _, a := range p.Desired {
		o, ok := f.owners[a.Partition]
		if !ok {
			for _, n := range f.nodes {
				if n.ID == a.Node {
					var e error
					o, e = f.c.Acquire(f.ctx, a.Partition, n)
					if e != nil {
						f.t.Fatal(e)
					}
				}
			}
		}
		var e error
		o, e = f.c.Update(f.ctx, o, true, false)
		if e != nil {
			f.t.Fatal(e)
		}
		if e = f.c.ReportReplica(f.ctx, "index-0000", p.Version, o, "Ready", "", max(offset, o.Offset)); e != nil {
			f.t.Fatal(e)
		}
		o.Offset = max(offset, o.Offset)
		f.owners[a.Partition] = o
	}
}
func TestMigrationDurableLifecycleAndFailover(t *testing.T) {
	f := newMigrationFixture(t)
	p := f.tick()
	initial := p.TaskID
	if p.Version != 1 || f.task(p).Stage != MigrationPending {
		t.Fatal(p, f.task(p))
	}
	f.ready(p, 50)
	p = f.tick()
	if f.task(p).Stage != MigrationCompleted {
		t.Fatal(f.task(p))
	}
	tx, _ := f.root.begin()
	_ = putJSON(tx, f.root.logKey(50), LogEvent{Sequence: 50})
	_ = tx.Commit(f.ctx)
	f.nodes[0].Draining = true
	n := ClusterNode{ID: "c", Session: "c", URL: "http://c", Roles: []string{RoleWorker}}
	_ = f.c.Heartbeat(f.ctx, n)
	f.nodes = append(f.nodes, n)
	p = f.tick()
	if p.TaskID == initial || p.Version != 2 || len(p.Serving) != 3 {
		t.Fatal(p)
	}
	o, e := f.c.Acquire(f.ctx, replicaID("index-0000", n), n)
	if e != nil {
		t.Fatal(e)
	}
	f.owners[o.Partition] = o
	if e = f.c.ReportReplica(f.ctx, "index-0000", p.Version, o, "Error", "snapshot unavailable", 0); e != nil {
		t.Fatal(e)
	}
	p = f.tick()
	if f.task(p).LastError != "snapshot unavailable" {
		t.Fatal(f.task(p))
	}
	// A new Coordinator has no in-memory task state, and still resumes the task.
	f.c = &Coordinator{root: f.root, ttl: 60000}
	f.ready(p, 0)
	p = f.tick()
	if f.task(p).Stage != MigrationCatchingUp || len(p.Serving) != 3 {
		t.Fatal(f.task(p), p)
	}
	oldLeader := f.leader
	_, _ = f.c.Update(f.ctx, oldLeader, false, true)
	nextMaster := ClusterNode{ID: "master2", Session: "m2", URL: "http://master2", Roles: []string{RoleMaster}}
	_ = f.c.Heartbeat(f.ctx, nextMaster)
	f.leader, e = f.c.Acquire(f.ctx, masterLeaseID, nextMaster)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.c.ScheduleReplicas(f.ctx, oldLeader, f.layout, f.nodes, 2); !errors.Is(e, ErrFenced) {
		t.Fatal("stale master accepted", e)
	}
	taskID := p.TaskID
	p = f.tick()
	if p.TaskID != taskID {
		t.Fatal("failover replaced task")
	}
	f.ready(p, 50)
	p = f.tick()
	task := f.task(p)
	if task.Stage != MigrationRetiring || len(p.Serving) != 2 {
		t.Fatal(task, p)
	}
	source := f.owners[replicaID("index-0000", f.nodes[0])]
	if _, e = f.c.Update(f.ctx, source, true, false); !errors.Is(e, ErrFenced) {
		t.Fatal("retired owner renewed", e)
	}
	_, _ = f.c.Update(f.ctx, source, false, true)
	p = f.tick()
	if f.task(p).Stage != MigrationRetiring {
		t.Fatal("lease release bypassed resource retirement")
	}
	ack, _ := f.root.begin()
	_ = putJSON(ack, f.c.retirementKey(source.Partition, source.Epoch), source)
	_ = ack.Commit(f.ctx)
	p = f.tick()
	task = f.task(p)
	if task.Stage != MigrationCompleted || task.LeaderEpoch != f.leader.Epoch {
		t.Fatal(task)
	}
	stages := map[MigrationStage]bool{}
	for _, event := range task.History {
		stages[event.Stage] = true
	}
	for _, stage := range []MigrationStage{MigrationPending, MigrationRestoring, MigrationCatchingUp, MigrationReady, MigrationSwitching, MigrationRetiring, MigrationCompleted} {
		if !stages[stage] {
			t.Fatal("missing milestone", stage)
		}
	}
	before, _ := json.Marshal(task)
	p = f.tick()
	after, _ := json.Marshal(f.task(p))
	if string(before) != string(after) {
		t.Fatal("completed task mutated")
	}
}

func TestMigrationStaleReportTimeoutAndReplacement(t *testing.T) {
	f := newMigrationFixture(t)
	p := f.tick()
	f.ready(p, 0)
	owner := f.owners[p.Desired[0].Partition]
	if e := f.c.ReportReplica(f.ctx, "index-0000", p.Version+1, owner, "Ready", "", 0); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	stale := owner
	stale.Epoch++
	if e := f.c.ReportReplica(f.ctx, "index-0000", p.Version, stale, "Ready", "", 0); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	f.nodes[0].Draining = true
	oldID := p.TaskID
	p = f.tick()
	tx, _ := f.root.begin()
	old, e := f.c.taskIn(f.ctx, tx, oldID)
	_ = tx.Rollback()
	if e != nil || old.Stage != MigrationSuperseded {
		t.Fatal(old, e)
	}
	if e = f.c.ReportReplica(f.ctx, "index-0000", p.Version-1, owner, "Ready", "", 0); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
	// Timeout is visible, but does not force a destructive rollback/switch.
	tx, _ = f.root.begin()
	task := f.task(p)
	task.Stage = MigrationRestoring
	task.StageStartedAt = f.db.nowMillis - 121000
	_ = putJSON(tx, f.c.taskKey(task.ID), task)
	for _, a := range p.Desired {
		o, _ := f.c.readOwner(f.ctx, tx, a.Partition)
		o.Ready = false
		_ = putJSON(tx, f.c.ownerKey(a.Partition), o)
	}
	_ = tx.Commit(f.ctx)
	p = f.tick()
	task = f.task(p)
	if !task.TimedOut || task.Stage != MigrationRestoring {
		t.Fatal(task)
	}
}
func TestMigrationPreviewReadOnlyAndPagination(t *testing.T) {
	f := newMigrationFixture(t)
	p := f.tick()
	f.ready(p, 0)
	f.tick()
	c := &ClusterService{root: f.root, coordinator: f.c, replicas: 2, PartitionedService: &PartitionedService{layout: f.layout}}
	before, _ := json.Marshal(f.db.data)
	commits := f.db.commits
	preview, e := c.Preview(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	if len(preview.Partitions) != 1 || preview.Partitions[0].Changed {
		t.Fatal(preview)
	}
	after, _ := json.Marshal(f.db.data)
	if string(before) != string(after) || f.db.commits != commits {
		t.Fatal("dry-run wrote state")
	}
	page, e := c.Tasks(f.ctx, "", 1)
	if e != nil || len(page.Tasks) != 1 {
		t.Fatal(page, e)
	}
	task, e := c.Task(f.ctx, page.Tasks[0].ID)
	if e != nil || task.ID != p.TaskID {
		t.Fatal(task, e)
	}
	if _, e = c.Tasks(f.ctx, "../invalid", 1); e == nil {
		t.Fatal("bad cursor accepted")
	}
	f.db.iterErr = injected
	if _, e = c.Tasks(f.ctx, "", 1); !errors.Is(e, injected) {
		t.Fatal(e)
	}
}
func TestPlacementBudgetAndImmutableInput(t *testing.T) {
	layout, _ := newLayout(1)
	workers := []ClusterNode{{ID: "a", Session: "a", MemoryBudgetBytes: 1}, {ID: "b", Session: "b"}, {ID: "c", Session: "c"}}
	before := append([]ClusterNode(nil), workers...)
	snapshot := PlacementSnapshot{Workers: workers, Plans: map[IndexPartitionID]ReplicaPlan{}, Owners: map[IndexPartitionID]Ownership{}}
	decision := planPlacement(layout, snapshot, 2)
	if hasNode(decision.Targets["index-0000"], "a") || len(decision.Targets["index-0000"]) != 2 {
		t.Fatal(decision)
	}
	if !reflect.DeepEqual(before, workers) {
		t.Fatal("policy mutated input")
	}
	workers[1].MemoryBudgetBytes = 1
	workers[2].MemoryBudgetBytes = 1
	decision = planPlacement(layout, snapshot, 2)
	if decision.Reason != "insufficient_capacity" || len(decision.Targets["index-0000"]) != 0 {
		t.Fatal(decision)
	}
}

func TestMigrationAtomicFailureAndTaskPagination(t *testing.T) {
	f := newMigrationFixture(t)
	f.db.commitErr = injected
	if e := f.c.ScheduleReplicas(f.ctx, f.leader, f.layout, f.nodes, 2); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	c := &ClusterService{root: f.root, coordinator: f.c}
	page, e := c.Tasks(f.ctx, "", 10)
	if e != nil || len(page.Tasks) != 0 {
		t.Fatal("task leaked from failed transaction", page, e)
	}
	f.db.commitErr = nil
	p := f.tick()
	first := p.TaskID
	f.nodes[0].Draining = true
	p = f.tick()
	page, e = c.Tasks(f.ctx, "", 1)
	if e != nil || page.Next == "" || page.Tasks[0].ID != first {
		t.Fatal(page, e)
	}
	next, e := c.Tasks(f.ctx, page.Next, 1)
	if e != nil || len(next.Tasks) != 1 || next.Tasks[0].ID != p.TaskID || next.Next != "" {
		t.Fatal(next, e)
	}
}
func TestRetirementWaitsForLoaderAndRetriesAcknowledgement(t *testing.T) {
	r, db := newFakeRepo()
	coord := &Coordinator{root: r}
	owner := Ownership{Partition: "replica", Node: "worker", Session: "s", Epoch: 1}
	entry := &localPartition{owner: owner, loadDone: make(chan struct{})}
	c := &ClusterService{root: r, coordinator: coord, replicas: 2, locals: map[IndexPartitionID]*localPartition{owner.Partition: entry}}
	db.commitErr = injected
	c.remove(entry)
	done := make(chan struct{})
	go func() { c.retirementWG.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("retired before loader exited")
	case <-time.After(20 * time.Millisecond):
	}
	close(entry.loadDone)
	<-done
	if len(c.pendingRetirements) != 1 {
		t.Fatal("failed acknowledgement lost")
	}
	db.commitErr = nil
	c.flushRetirements(context.Background())
	if len(c.pendingRetirements) != 0 {
		t.Fatal("acknowledgement did not retry")
	}
	tx, _ := r.begin()
	defer tx.Rollback()
	if _, e := tx.Get(context.Background(), coord.retirementKey(owner.Partition, owner.Epoch)); e != nil {
		t.Fatal(e)
	}
}
func TestMigrationRejectsOldProtocol(t *testing.T) {
	for _, mode := range []string{"v3-replicas-2", "v4-load-2"} {
		r, db := newFakeRepo()
		db.data[r.Prefix+"cluster-mode"] = []byte(mode)
		if e := r.ensureClusterMode(context.Background(), 2); e == nil {
			t.Fatal("old protocol accepted", mode)
		}
		if e := r.ensureLoadMode(context.Background()); e == nil {
			t.Fatal("old protocol accepted", mode)
		}
	}
}

func TestMigrationRecoveryAcrossPersistedStages(t *testing.T) {
	for _, stage := range []MigrationStage{MigrationPending, MigrationRestoring, MigrationCatchingUp, MigrationRetiring} {
		for _, failure := range []string{"master", "source", "target"} {
			t.Run(string(stage)+"/"+failure, func(t *testing.T) {
				f := newMigrationFixture(t)
				p := f.tick()
				f.ready(p, 50)
				f.tick()
				tx, _ := f.root.begin()
				_ = putJSON(tx, f.root.logKey(50), LogEvent{Sequence: 50})
				_ = tx.Commit(f.ctx)
				f.nodes[0].Draining = true
				target := ClusterNode{ID: "c", Session: "c", URL: "http://c", Roles: []string{RoleWorker}}
				_ = f.c.Heartbeat(f.ctx, target)
				f.nodes = append(f.nodes, target)
				p = f.tick()
				if stage != MigrationPending {
					o, e := f.c.Acquire(f.ctx, replicaID("index-0000", target), target)
					if e != nil {
						t.Fatal(e)
					}
					f.owners[o.Partition] = o
				}
				if stage == MigrationCatchingUp {
					f.ready(p, 0)
				}
				if stage == MigrationRetiring {
					f.ready(p, 50)
				}
				p = f.tick()
				taskID := p.TaskID
				switch failure {
				case "master":
					_, _ = f.c.Update(f.ctx, f.leader, false, true)
					m := ClusterNode{ID: "new-master", Session: "new", URL: "http://new", Roles: []string{RoleMaster}}
					_ = f.c.Heartbeat(f.ctx, m)
					var e error
					f.leader, e = f.c.Acquire(f.ctx, masterLeaseID, m)
					if e != nil {
						t.Fatal(e)
					}
					f.c = &Coordinator{root: f.root, ttl: 60000}
					p = f.tick()
					if p.TaskID != taskID {
						t.Fatal("task identity lost on failover")
					}
					f.ready(p, 50)
					p = f.tick()
					if len(p.Serving) != 2 {
						t.Fatal("migration did not resume", p)
					}
				case "source":
					f.nodes = f.nodes[1:]
					tx, _ = f.root.begin()
					_ = tx.Delete(f.c.nodeKey("a"))
					_ = tx.Commit(f.ctx)
					p = f.tick()
					f.ready(p, 50)
					p = f.tick()
					if len(p.Serving) != 2 || f.task(p).Stage != MigrationCompleted {
						t.Fatal("source failure blocked recovery", p, f.task(p))
					}
				case "target":
					f.nodes = f.nodes[:2]
					tx, _ = f.root.begin()
					_ = tx.Delete(f.c.nodeKey("c"))
					_ = tx.Commit(f.ctx)
					old := p
					p = f.tick()
					if p.TaskID == taskID {
						t.Fatal("failed target intent was reused")
					}
					if stage != MigrationRetiring && !containsReplica(p.Serving, replicaID("index-0000", f.nodes[0])) {
						t.Fatal("source retired before replacement")
					}
					if stage == MigrationRetiring && containsReplica(p.Serving, replicaID("index-0000", f.nodes[0])) {
						t.Fatal("post-switch rollback resurrected source")
					}
					if !f.task(old).terminal() {
						t.Fatal("abandoned active task")
					}
				}
			})
		}
	}
}

func TestRetirementUsesCutoverEpoch(t *testing.T) {
	f := newMigrationFixture(t)
	p := f.tick()
	f.ready(p, 10)
	f.tick()
	f.nodes[0].Draining = true
	target := ClusterNode{ID: "c", Session: "c", URL: "http://c", Roles: []string{RoleWorker}}
	_ = f.c.Heartbeat(f.ctx, target)
	f.nodes = append(f.nodes, target)
	p = f.tick()
	sourceID := replicaID("index-0000", f.nodes[0])
	old := f.owners[sourceID]
	_, _ = f.c.Update(f.ctx, old, false, true)
	tx, _ := f.root.begin()
	_ = putJSON(tx, f.c.retirementKey(sourceID, old.Epoch), old)
	_ = tx.Commit(f.ctx)
	current, e := f.c.Acquire(f.ctx, sourceID, f.nodes[0])
	if e != nil {
		t.Fatal(e)
	}
	current, e = f.c.Update(f.ctx, current, true, false)
	if e != nil {
		t.Fatal(e)
	}
	f.ready(p, 10)
	p = f.tick()
	task := f.task(p)
	if task.SourceEpochs[sourceID] != current.Epoch || task.Stage != MigrationRetiring {
		t.Fatal("old acknowledgement retired new source epoch", task)
	}
	_, _ = f.c.Update(f.ctx, current, false, true)
	tx, _ = f.root.begin()
	_ = putJSON(tx, f.c.retirementKey(sourceID, current.Epoch), current)
	_ = tx.Commit(f.ctx)
	p = f.tick()
	if f.task(p).Stage != MigrationCompleted {
		t.Fatal(f.task(p))
	}
}

func TestMigrationBudgetPreservesSourceUntilReplacementFits(t *testing.T) {
	f := newMigrationFixture(t)
	p := f.tick()
	f.ready(p, 0)
	f.tick()
	f.c.loadBalance = true
	f.nodes[0].Draining = true
	target := ClusterNode{ID: "c", Session: "c", URL: "http://c", Roles: []string{RoleWorker}, MemoryBudgetBytes: 1}
	_ = f.c.Heartbeat(f.ctx, target)
	f.nodes = append(f.nodes, target)
	p = f.tick()
	source := replicaID("index-0000", f.nodes[0])
	if len(p.Desired) != 1 || !containsReplica(p.Serving, source) || f.task(p).terminal() {
		t.Fatal("budget shortfall retired source", p, f.task(p))
	}
	blockedID := p.TaskID
	f.nodes[2].MemoryBudgetBytes = 0
	p = f.tick()
	if p.TaskID == blockedID || len(p.Desired) != 2 || !containsReplica(p.Serving, source) {
		t.Fatal("capacity repair failed", p)
	}
	f.ready(p, 0)
	p = f.tick()
	if containsReplica(p.Serving, source) || f.task(p).Stage != MigrationRetiring {
		t.Fatal("cutover did not follow readiness", p)
	}
}
