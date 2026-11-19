package vector

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRecallPolicyValidation(t *testing.T) {
	p := DefaultRecallPolicy()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (RecallPolicy{}).Validate(); err != nil {
		t.Fatalf("disabled policy: %v", err)
	}
	for _, configure := range []func(*RecallPolicy){
		func(p *RecallPolicy) { p.Target = math.NaN() },
		func(p *RecallPolicy) { p.Target = 1.1 },
		func(p *RecallPolicy) { p.Target = -.1 },
		func(p *RecallPolicy) { p.Consecutive = 0 },
		func(p *RecallPolicy) { p.Cooldown = 0 },
		func(p *RecallPolicy) { p.Interval = -time.Second },
	} {
		v := p
		configure(&v)
		if v.Validate() == nil {
			t.Fatal("accepted invalid recall policy", v)
		}
	}
}

func TestRecallMonitorConsecutiveAndUnchangedRebuild(t *testing.T) {
	now := time.Unix(1000, 0)
	p := DefaultRecallPolicy()
	m := &recallMonitor{}
	sample := recallProbeResult{Epoch: 40, Queries: 16, Recall: .5}
	run := func() bool {
		t.Helper()
		if !m.due(now, 2, sample.Epoch) {
			t.Fatalf("check unexpectedly skipped at %v: %+v", now, m.status)
		}
		rebuild := m.observe(now, p, sample, sample.Epoch)
		if m.due(now, 2, sample.Epoch) {
			t.Fatal("duplicate sample in the same interval")
		}
		now = now.Add(p.Interval)
		return rebuild
	}
	if run() || run() {
		t.Fatal("single low samples caused a rebuild")
	}
	sample.Recall = p.Target
	if run() || m.status.LowStreak != 0 {
		t.Fatal("healthy sample failed to reset streak", m.status)
	}
	sample.Recall = .8
	if run() || run() || !run() {
		t.Fatal("three consecutive low samples must allow rebuilding")
	}
	m.finished(now, p, 41, nil)
	now = now.Add(p.Interval)
	sample.Epoch = 41
	if run() || !m.status.Suppressed || m.status.Rebuilds != 1 {
		t.Fatal("unsuccessful quality improvement retriggered a rebuild", m.status)
	}
	now = now.Add(p.Cooldown)
	if m.due(now, 2, 41) {
		t.Fatal("unchanged deterministic graph was rescheduled")
	}
	sample.Epoch = 42
	if run() || m.status.Suppressed || m.status.LowStreak != 1 {
		t.Fatal("new graph work did not re-enable probing", m.status)
	}
	if !m.due(now, 3, 42) || m.status.LowStreak != 0 {
		t.Fatal("new publisher inherited previous low streak")
	}
}

func TestRecallMonitorFailureBackoff(t *testing.T) {
	m := &recallMonitor{}
	p := DefaultRecallPolicy()
	now := time.Unix(70, 0)
	m.due(now, 1, 12)
	m.finished(now, p, 0, context.DeadlineExceeded)
	if m.due(now.Add(p.Interval), 1, 12) || !m.due(now.Add(p.Cooldown), 1, 12) {
		t.Fatal("failed build was not delayed and retried")
	}
	if st := m.status; st.Rebuilds != 0 || st.Failures != 1 || st.Suppressed {
		t.Fatal(st)
	}
}

func TestRecallConflictRetainsEvidence(t *testing.T) {
	m := &recallMonitor{}
	p := DefaultRecallPolicy()
	now := time.Unix(55, 0)
	m.due(now, 1, 42)
	sample := recallProbeResult{Queries: 16, Recall: .7, Epoch: 42}
	for n := 0; n < p.Consecutive; n++ {
		now = now.Add(p.Interval)
		m.observe(now, p, sample, sample.Epoch)
	}
	m.finished(now, p, 0, ErrConflict)
	if m.due(now, 1, 42) || !m.due(now.Add(p.Interval), 1, 43) {
		t.Fatal("conflicting build should retry once per interval")
	}
	if !m.observe(now.Add(p.Interval), p, sample, sample.Epoch) || m.status.LowStreak != p.Consecutive {
		t.Fatal("new writes invalidated the diagnostic streak", m.status)
	}
}

func TestRecallWatermarkIgnoresUnpublishedGraphChanges(t *testing.T) {
	s := &Service{graphReplica: &graphReplica{position: GraphPosition{Sequence: 15, DataOffset: 3}}}
	if s.recallWatermark(20) != 3 || s.recallWatermark(22) != 3 {
		t.Fatal("speculative candidate changed durable frontier")
	}
	s.graphReplica.position.Sequence++
	if s.recallWatermark(25) != 3 {
		t.Fatal("control-only append changed data frontier")
	}
	s.graphReplica.position.DataOffset++
	if s.recallWatermark(28) != 4 {
		t.Fatal("committed new input did not advance frontier")
	}
}

func TestRecallMaintenanceRespectsPublisherSmallShardAndStaleness(t *testing.T) {
	idx, err := NewHNSWIndex(2, DefaultIndexConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { idx.graph.Close() }()
	s := &Service{Index: idx}
	p := DefaultRecallPolicy()
	s.setIndexPolicy(IndexPolicy{Recall: &p})

	// A graph follower without a publisher lease must never inspect its local
	// graph or the journal. Even a nil journal is safe on this path.
	follower := &Service{graphReplica: &graphReplica{writer: &indexWriter{}}}
	if err := follower.MaybeRebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.MaybeRebuild(context.Background()); err != nil || s.RecallStatus().Samples != 0 {
		t.Fatalf("small shard probe: %v %+v", err, s.RecallStatus())
	}
	// Disabling recall checks does not turn off existing stale-node reclamation.
	for n := 1; n <= 260; n++ {
		if err = idx.Apply([]Record{{ID: "same", Revision: uint64(n), Vector: []float64{float64(n), 0}}}); err != nil {
			t.Fatal(err)
		}
	}
	s.setIndexPolicy(IndexPolicy{Recall: &RecallPolicy{}})
	if !idx.NeedsRebuild() {
		t.Fatal("setup did not accumulate stale nodes")
	}
	if err = s.MaybeRebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if idx.Stats().StaleNodes != 0 || s.RecallStatus().Rebuilds != 0 {
		t.Fatal(idx.Stats(), s.RecallStatus())
	}
}

func TestRecallMaintenanceIndependentReplicaHasItsOwnGraph(t *testing.T) {
	c := &ClusterService{localOwnerLeases: true, replicas: 2}
	owner := Ownership{Partition: "index-0000~B", Node: "node-b", Session: "b", Epoch: 7, Ready: true, Expires: time.Now().Add(time.Minute).UnixMilli()}
	entry := &localPartition{owner: owner}
	entry.grantReadLease(time.Now(), owner, time.Minute)
	entry.setMaintenancePlan(ReplicaPlan{Version: 11, Publisher: "index-0000~A"})
	index, err := NewHNSWIndex(2, DefaultIndexConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer index.graph.Close()
	s := &Service{Index: index, publisherTerm: func() (uint64, bool) { return c.indexMaintenanceTerm(entry) }}
	threshold := 0
	s.setIndexPolicy(IndexPolicy{ExactThreshold: &threshold})
	records := make([]Record, 64)
	for n := range records {
		records[n] = Record{ID: fmt.Sprintf("point-%02d", n), Revision: 1, Vector: []float64{float64(n), 1}}
	}
	if err = index.Apply(records); err != nil {
		t.Fatal(err)
	}
	term, ok := s.recallTerm()
	if !ok || term != 7 {
		t.Fatalf("independent replica must have own maintenance term: %d, %t", term, ok)
	}
	if err = s.MaybeRebuild(context.Background()); err != nil || s.RecallStatus().Samples != 1 {
		t.Fatalf("independent replica did not probe: %v,%+v", err, s.RecallStatus())
	}
	c.graphReplication = true
	if _, ok := c.indexMaintenanceTerm(entry); ok {
		t.Fatal("graph follower can publish")
	}
	entry.setMaintenancePlan(ReplicaPlan{Version: 12, Publisher: owner.Partition})
	if term, ok := c.indexMaintenanceTerm(entry); !ok || term != 12 {
		t.Fatal("publisher plan did not grant maintenance", term, ok)
	}
	entry.retireReads()
	if _, ok := c.indexMaintenanceTerm(entry); ok {
		t.Fatal("retired replica can publish")
	}
}
