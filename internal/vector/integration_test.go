package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestTiKVSeaweedIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV and SeaweedFS")
	}
	ctx := context.Background()
	repo, e := NewRepository([]string{"127.0.0.1:12379"}, "test/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer repo.Client.Close()
	objects, e := NewObjects("127.0.0.1:18333", "poc-access", "poc-secret-local-only", "vector-index-poc")
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewService(ctx, repo, objects)
	if e != nil {
		t.Fatal(e)
	}
	t.Run("rollback_record_and_outbox", func(t *testing.T) {
		tx, e := repo.Client.Begin()
		if e != nil {
			t.Fatal(e)
		}
		for _, kind := range []string{"record", "outbox", "request"} {
			if e = tx.Set(repo.key(kind, "rolled-back"), []byte(`{"id":"bad"}`)); e != nil {
				t.Fatal(e)
			}
		}
		if e = tx.Rollback(); e != nil {
			t.Fatal(e)
		}
		read, e := repo.Client.Begin()
		if e != nil {
			t.Fatal(e)
		}
		defer read.Rollback()
		for _, kind := range []string{"record", "outbox", "request"} {
			if _, e = read.Get(ctx, repo.key(kind, "rolled-back")); !tikverr.IsErrNotFound(e) {
				t.Fatalf("rollback leaked %s: %v", kind, e)
			}
		}
	})
	m := Mutation{RequestID: "create", Vector: []float64{0, 0}, Payload: map[string]string{"tag": "yes"}}
	rec, e := repo.Mutate(ctx, "a", m, false)
	if e != nil {
		t.Fatal(e)
	}
	t.Run("atomic_commit_and_idempotency", func(t *testing.T) {
		again, e := repo.Mutate(ctx, "a", m, false)
		if e != nil || again.Revision != rec.Revision {
			t.Fatal(again, e)
		}
		events, e := repo.ReadEvents(ctx, 0, 0, 256)
		if e != nil || len(events.Events) != 1 {
			t.Fatal(events, e)
		}
		changed := m
		changed.Vector = []float64{1, 1}
		if _, e = repo.Mutate(ctx, "a", changed, false); e == nil {
			t.Fatal("reused request accepted")
		}
	})
	t.Run("concurrent_expected_revision", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(chan error, 2)
		revision := rec.Revision
		for n := 0; n < 2; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, e := repo.Mutate(ctx, "a", Mutation{RequestID: uuid.NewString(), Vector: []float64{1, 0}, ExpectedRevision: &revision}, false)
				results <- e
			}()
		}
		wg.Wait()
		close(results)
		success := 0
		for e := range results {
			if e == nil {
				success++
			}
		}
		if success != 1 {
			t.Fatalf("successful competing updates=%d", success)
		}
		events, e := repo.ReadEvents(ctx, 0, 0, 256)
		if e != nil || len(events.Events) != 2 {
			t.Fatal(events, e)
		}
	})
	if _, e = repo.Mutate(ctx, "b", Mutation{RequestID: "b-create", Vector: []float64{2, 0}, Payload: map[string]string{"tag": "yes"}}, false); e != nil {
		t.Fatal(e)
	}
	manifest, e := s.Checkpoint(ctx)
	if e != nil {
		t.Fatal(e)
	}
	t.Run("manifest_cas", func(t *testing.T) {
		if e = repo.Publish(ctx, "stale", manifest); e == nil {
			t.Fatal("stale manifest published")
		}
	})
	t.Run("concurrent_manifest_publish", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for n := 0; n < 2; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				candidate := manifest
				candidate.Generation = uuid.NewString()
				results <- repo.Publish(ctx, manifest.Generation, candidate)
			}()
		}
		wg.Wait()
		close(results)
		success := 0
		for err := range results {
			if err == nil {
				success++
			}
		}
		if success != 1 {
			t.Fatalf("successful concurrent publishers=%d", success)
		}
	})
	deleted, e := repo.Mutate(ctx, "a", Mutation{RequestID: "delete-a"}, true)
	if e != nil {
		t.Fatal(e)
	}
	t.Run("snapshot_plus_later_delete_replay", func(t *testing.T) {
		recovered, e := NewService(ctx, repo, objects)
		if e != nil {
			t.Fatal(e)
		}
		// Feed an older event again after deletion: must remain deleted.
		recovered.Index.Apply([]Record{rec})
		hits, e := recovered.Index.Search(Search{Vector: []float64{0, 0}, K: 10, MinVisible: &Token{"a", deleted.Revision}})
		if e != nil || len(hits) != 1 || hits[0].ID != "b" {
			t.Fatal(hits, e)
		}
	})
	t.Run("corrupt_snapshot_rejected", func(t *testing.T) {
		bad := manifest
		bad.SHA256 = "bad"
		if _, e = objects.Load(ctx, bad); e == nil {
			t.Fatal("corrupt snapshot accepted")
		}
	})
	if t.Failed() {
		return
	}
	result := map[string]any{"status": "passed", "namespace": repo.Prefix, "tests": []string{"rollback_record_outbox_request", "atomic_commit_idempotency", "concurrent_revision_conflict", "manifest_cas", "snapshot_plus_delete_replay", "corrupt_snapshot_rejected"}, "scope": "single PD, single TiKV, single SeaweedFS; no distributed fault tolerance claim"}
	b, _ := json.MarshalIndent(result, "", "  ")
	_ = os.MkdirAll("../../reports", 0755)
	if e = os.WriteFile("../../reports/tikv-integration.json", b, 0644); e != nil {
		t.Fatal(e)
	}
}

func TestIncrementalLogTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV and SeaweedFS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	repo, e := NewRepository([]string{"127.0.0.1:12379"}, "log-test/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer repo.Client.Close()
	objects, e := NewObjects("127.0.0.1:18333", "poc-access", "poc-secret-local-only", "vector-index-poc")
	if e != nil {
		t.Fatal(e)
	}
	service, e := NewService(ctx, repo, objects)
	if e != nil {
		t.Fatal(e)
	}
	// Distinct writers still contend on one sequence key. No confirmed write may disappear.
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 12; j++ {
				id := fmt.Sprintf("%d-%d", w, j)
				m := Mutation{RequestID: id, Vector: []float64{float64(w), float64(j)}}
				var err error
				for retry := 0; retry < 10; retry++ {
					_, err = repo.Mutate(ctx, id, m, false)
					if err == nil {
						break
					}
					if ctx.Err() != nil {
						break
					}
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	b, e := repo.ReadEvents(ctx, 0, 0, 256)
	if e != nil || b.Head != 96 || len(b.Events) != 96 {
		t.Fatal(b.Head, len(b.Events), e)
	}
	for j, event := range b.Events {
		if event.Sequence != uint64(j+1) {
			t.Fatal(event)
		}
	}
	if e = service.Sync(ctx); e != nil || service.Cursor() != 96 {
		t.Fatal(e)
	}
	m, e := service.Checkpoint(ctx)
	if e != nil || m.Offset != 96 {
		t.Fatal(m, e)
	}
	if _, e = repo.Mutate(ctx, "0-0", Mutation{RequestID: "deleted-after-snapshot"}, true); e != nil {
		t.Fatal(e)
	}
	if floor, e := service.Prune(ctx); e != nil || floor != 96 {
		t.Fatal(floor, e)
	}
	b, e = repo.ReadEvents(ctx, 96, 0, 256)
	if e != nil || len(b.Events) != 1 || !b.Events[0].Record.Deleted {
		t.Fatal(b, e)
	}
	restored, e := NewService(ctx, repo, objects)
	if e != nil || restored.Cursor() != 97 {
		t.Fatal(e)
	}
	hits, e := restored.Search(Search{Vector: []float64{0, 0}, K: 100, Exact: true})
	if e != nil || len(hits) != 95 {
		t.Fatal(len(hits), e)
	}
	// Start a lagging reader after pruning. It must bootstrap records and the head together.
	lag, err := NewHNSWIndex(2, DefaultIndexConfig())
	if err != nil {
		t.Fatal(err)
	}
	lagging := &Service{Repo: repo, Objects: objects, Index: lag}
	if e = lagging.Sync(ctx); e != nil || lagging.Cursor() != 97 {
		t.Fatal(e)
	}
	stale := m
	stale.Offset = 95
	if e = repo.Publish(ctx, m.Generation, stale); e == nil {
		t.Fatal("regressed snapshot published")
	}
	t.Log("96 concurrent writes contiguous; 96 events pruned; post-checkpoint delete replayed; lagging consumer recovered")
}

func TestWriteVisibilityLatencyTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real backends")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, e := NewRepository([]string{"127.0.0.1:12379"}, "visibility/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer repo.Client.Close()
	objects, e := NewObjects("127.0.0.1:18333", "poc-access", "poc-secret-local-only", "vector-index-poc")
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewService(ctx, repo, objects)
	if e != nil {
		t.Fatal(e)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	syncErrors := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.Sync(ctx); err != nil {
					syncErrors <- err
					return
				}
			}
		}
	}()
	defer func() { close(stop); <-done }()
	writes, visible := []time.Duration{}, []time.Duration{}
	for j := 0; j < 12; j++ {
		id := fmt.Sprint(j)
		start := time.Now()
		rec, err := repo.Mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{float64(j), 0}}, false)
		if err != nil {
			t.Fatal(err)
		}
		committed := time.Now()
		writes = append(writes, committed.Sub(start))
		for {
			_, err = s.Search(Search{Vector: []float64{float64(j), 0}, K: 1, MinVisible: &Token{id, rec.Revision}})
			if err == nil {
				break
			}
			if !errors.Is(err, ErrNotVisible) {
				t.Fatal(err)
			}
			select {
			case e := <-syncErrors:
				t.Fatal(e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
		visible = append(visible, time.Since(committed))
	}
	report := map[string]any{"samples": len(writes), "write_p95_ms": percentileMS(writes, .95), "write_p99_ms": percentileMS(writes, .99), "commit_to_visible_p95_ms": percentileMS(visible, .95), "commit_to_visible_p99_ms": percentileMS(visible, .99), "poll_interval_ms": 500, "scope": "single PD/TiKV; sequential tiny writes; 12 samples are a smoke test, not production tail-latency estimates"}
	b, _ := json.MarshalIndent(report, "", "  ")
	if e = os.WriteFile("../../reports/visibility-latency.json", b, 0644); e != nil {
		t.Fatal(e)
	}
}

func TestLeaseFenceTransactionConflictTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	root, e := NewRepository([]string{"127.0.0.1:12379"}, "fence-test/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Client.Close()
	coord := &Coordinator{root: root, ttl: 6000}
	a := ClusterNode{Roles: []string{RoleWorker}, ID: "a", Session: "a1", URL: "http://127.0.0.1:1"}
	b := ClusterNode{Roles: []string{RoleWorker}, ID: "b", Session: "b1", URL: "http://127.0.0.1:2"}
	if e = coord.Heartbeat(ctx, a); e != nil {
		t.Fatal(e)
	}
	testAssignment(t, coord, a, "index-0000")
	old, e := coord.Acquire(ctx, "index-0000", a)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = coord.Update(ctx, old, true, false); e != nil {
		t.Fatal(e)
	}
	delayed, e := root.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer delayed.Rollback()
	if e = coord.Fence(old)(ctx, delayed); e != nil {
		t.Fatal(e)
	}
	marker := []byte(root.Prefix + "stale-publication-marker")
	if e = delayed.Set(marker, []byte("should-not-commit")); e != nil {
		t.Fatal(e)
	}
	if _, e = coord.Update(ctx, old, false, true); e != nil {
		t.Fatal(e)
	}
	if e = coord.Heartbeat(ctx, b); e != nil {
		t.Fatal(e)
	}
	testAssignment(t, coord, b, old.Partition)
	next, e := coord.Acquire(ctx, old.Partition, b)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = coord.Update(ctx, next, true, false); e != nil {
		t.Fatal(e)
	}
	if e = delayed.Commit(ctx); e == nil {
		t.Fatal("old in-flight transaction survived ownership change")
	}
	read, e := root.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer read.Rollback()
	if _, e = read.Get(ctx, marker); !tikverr.IsErrNotFound(e) {
		t.Fatal("stale write escaped", e)
	}
	if e = coord.Check(ctx, old); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
}

func TestMasterElectionAndFenceTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	root, e := NewRepository([]string{"127.0.0.1:12379"}, "master-fence/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Client.Close()
	c := &Coordinator{root: root, ttl: 6000}
	nodes := []ClusterNode{{ID: "a", Session: "a1", URL: "http://localhost:1", Roles: []string{RoleMaster}}, {ID: "b", Session: "b1", URL: "http://localhost:2", Roles: []string{RoleMaster}}}
	for _, n := range nodes {
		if e = c.Heartbeat(ctx, n); e != nil {
			t.Fatal(e)
		}
	}
	var wg sync.WaitGroup
	won := make(chan Ownership, 2)
	for _, n := range nodes {
		wg.Add(1)
		go func(n ClusterNode) {
			defer wg.Done()
			if o, e := c.Acquire(ctx, masterLeaseID, n); e == nil {
				won <- o
			}
		}(n)
	}
	wg.Wait()
	close(won)
	winners := []Ownership{}
	for o := range won {
		winners = append(winners, o)
	}
	if len(winners) != 1 {
		t.Fatal("multiple elected masters", winners)
	}
	old := winners[0]
	delayed, e := root.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer delayed.Rollback()
	if e = c.Fence(old)(ctx, delayed); e != nil {
		t.Fatal(e)
	}
	marker := []byte(root.Prefix + "stale-plan-marker")
	if e = delayed.Set(marker, []byte("stale")); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Update(ctx, old, false, true); e != nil {
		t.Fatal(e)
	}
	nextNode := nodes[0]
	if nextNode.ID == old.Node {
		nextNode = nodes[1]
	}
	next, e := c.Acquire(ctx, masterLeaseID, nextNode)
	if e != nil || next.Epoch <= old.Epoch {
		t.Fatal(next, e)
	}
	if e = delayed.Commit(ctx); e == nil {
		t.Fatal("old in-flight scheduler committed after new election")
	}
	tx, e := root.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Get(ctx, marker); !tikverr.IsErrNotFound(e) {
		t.Fatal(e)
	}
	layout, _ := newLayout(4)
	if e = c.Schedule(ctx, old, layout, nil); !errors.Is(e, ErrFenced) {
		t.Fatal("old leader scheduled", e)
	}
}

func TestReplicaPublisherPlanConflictTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	r, e := NewRepository([]string{"127.0.0.1:12379"}, "replica-publisher-fence/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Client.Close()
	c := &Coordinator{root: r, ttl: 60000}
	n := ClusterNode{ID: "worker", Session: "s", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	if e = c.Heartbeat(ctx, n); e != nil {
		t.Fatal(e)
	}
	id := replicaID("index-0000", n)
	tx, _ := r.begin()
	a := Assignment{Partition: id, Node: n.ID, Session: n.Session, URL: n.URL}
	if e = putJSON(tx, c.assignmentKey(id), a); e != nil {
		t.Fatal(e)
	}
	if e = putJSON(tx, c.planKey("index-0000"), ReplicaPlan{Serving: []Assignment{a}, Publisher: id}); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	owner, e := c.Acquire(ctx, id, n)
	if e != nil {
		t.Fatal(e)
	}
	owner, e = c.Update(ctx, owner, true, false)
	if e != nil {
		t.Fatal(e)
	}
	delayed, _ := r.begin()
	defer delayed.Rollback()
	if e = c.ReplicaFence("index-0000", owner)(ctx, delayed); e != nil {
		t.Fatal(e)
	}
	marker := []byte(r.Prefix + "stale-publisher-marker")
	if e = delayed.Set(marker, []byte("stale")); e != nil {
		t.Fatal(e)
	}
	next, _ := r.begin()
	if e = putJSON(next, c.planKey("index-0000"), ReplicaPlan{Serving: []Assignment{a}, Publisher: "replacement"}); e != nil {
		t.Fatal(e)
	}
	if e = next.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = delayed.Commit(ctx); e == nil {
		t.Fatal("old publisher committed after authority moved")
	}
	check, _ := r.begin()
	defer check.Rollback()
	if _, e = check.Get(ctx, marker); !tikverr.IsErrNotFound(e) {
		t.Fatal(e)
	}
	if e = c.ReplicaFence("index-0000", owner)(ctx, check); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
}

func TestMigrationReportPlanConflictTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	r, e := NewRepository([]string{"127.0.0.1:12379"}, "migration-report/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Client.Close()
	c := &Coordinator{root: r, ttl: 60000}
	node := ClusterNode{ID: "worker", Session: "s", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	if e = c.Heartbeat(ctx, node); e != nil {
		t.Fatal(e)
	}
	id := IndexPartitionID("index-0000")
	replica := replicaID(id, node)
	a := Assignment{Partition: replica, Node: node.ID, Session: node.Session, URL: node.URL}
	tx, _ := r.begin()
	_ = putJSON(tx, c.assignmentKey(replica), a)
	plan := ReplicaPlan{Version: 1, Serving: []Assignment{a}, Desired: []Assignment{a}}
	_ = putJSON(tx, c.planKey(id), plan)
	_ = putJSON(tx, c.taskFenceKey(id), uint64(1))
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	owner, e := c.Acquire(ctx, replica, node)
	if e != nil {
		t.Fatal(e)
	}
	delayed, _ := r.begin()
	defer delayed.Rollback()
	if e = c.reportReplicaIn(ctx, delayed, id, 1, owner, "CatchingUp", "", 10); e != nil {
		t.Fatal(e)
	}
	replacement, _ := r.begin()
	plan.Version = 2
	_ = putJSON(replacement, c.planKey(id), plan)
	_ = putJSON(replacement, c.taskFenceKey(id), uint64(2))
	if e = replacement.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = delayed.Commit(ctx); e == nil {
		t.Fatal("stale report committed after plan change")
	}
	check, _ := r.begin()
	defer check.Rollback()
	current, e := c.readOwner(ctx, check, replica)
	if e != nil || current.Offset != 0 {
		t.Fatal(current, e)
	}
}

func TestConsumerAcquireCompactionConflictTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	r, e := NewRepository([]string{"127.0.0.1:12379"}, "retention-conflict/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Client.Close()
	for n := 0; n < 3; n++ {
		if _, e = r.Mutate(ctx, fmt.Sprint(n), Mutation{RequestID: fmt.Sprint(n), Vector: []float64{1, 2}}, false); e != nil {
			t.Fatal(e)
		}
	}
	service, e := NewService(ctx, r, partitionObjects())
	if e != nil {
		t.Fatal(e)
	}
	defer service.Index.graph.Close()
	if _, e = service.Checkpoint(ctx); e != nil {
		t.Fatal(e)
	}
	layout, _ := newLayout(1)
	p := &PartitionedService{layout: layout, data: map[DataPartitionID]*Repository{0: r}}
	delayed, _ := r.begin()
	defer delayed.Rollback()
	if _, e = r.retainConsumers(ctx, delayed, "data", 3); e != nil {
		t.Fatal(e)
	}
	q := ConsumerRequest{Partition: "index-0000", Stream: "data", Action: "acquire", Consumer: "reader", Session: uuid.NewString()}
	state, e := p.manageConsumer(ctx, q, nil)
	if e != nil {
		t.Fatal(e)
	}
	_ = putJSON(delayed, []byte(r.Prefix+"log-floor"), uint64(3))
	if e = delayed.Commit(ctx); e == nil {
		t.Fatal("compaction ignored concurrent lease acquisition")
	}
	if floor, e := r.PruneEvents(ctx, 256); e != nil || floor != 0 {
		t.Fatal(floor, e)
	}
	q.Action = "release"
	q.Epoch = state.Leases["reader"].Epoch
	if _, e = p.manageConsumer(ctx, q, nil); e != nil {
		t.Fatal(e)
	}
	// Inverse race: the stale lease transaction must not resurrect a cursor which
	// has already been removed by a committed prune.
	acquire, _ := r.begin()
	defer acquire.Rollback()
	leases, e := r.consumersIn(ctx, acquire, "data")
	if e != nil {
		t.Fatal(e)
	}
	leases["late"] = ConsumerLease{Consumer: "late", Session: uuid.NewString(), Epoch: 1, Ack: 0, Expires: txnMillis(acquire) + 60000}
	if e = putJSON(acquire, r.consumerKey("data"), leases); e != nil {
		t.Fatal(e)
	}
	if floor, e := r.PruneEvents(ctx, 256); e != nil || floor != 3 {
		t.Fatal(floor, e)
	}
	if e = acquire.Commit(ctx); e == nil {
		t.Fatal("stale lease committed below compacted floor")
	}
}

func TestObjectAdoptionGCConflictTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	r, e := NewRepository([]string{"127.0.0.1:12379"}, "object-gc-conflict/"+uuid.NewString(), 2)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Client.Close()
	j := graphJournal{c: &Coordinator{root: r}, id: "index-0000"}
	m := Manifest{Generation: uuid.NewString(), Key: r.Namespace() + "snapshots/object", SHA256: "digest", Size: 1}
	tx, _ := r.begin()
	o := trackedObject{Manifest: m, State: "staged", Expires: txnMillis(tx) + 3600000}
	_ = putJSON(tx, j.objectKey(m), o)
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	adopt, _ := r.begin()
	defer adopt.Rollback()
	if e = j.adoptObject(ctx, adopt, m); e != nil {
		t.Fatal(e)
	}
	gc, _ := r.begin()
	o.State = "garbage"
	_ = putJSON(gc, j.objectKey(m), o)
	if e = gc.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = adopt.Commit(ctx); e == nil {
		t.Fatal("stale adoption resurrected GC tombstone")
	}
	fresh, _ := r.begin()
	defer fresh.Rollback()
	if e = j.adoptObject(ctx, fresh, m); !errors.Is(e, ErrFenced) {
		t.Fatal(e)
	}
}
