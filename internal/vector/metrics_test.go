package vector

import (
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestClusterMetricsSemantics(t *testing.T) {
	s := ClusterStatus{Ready: true, Routing: &ClusterRouting{Version: 7},
		DataProgress:   map[string]DataProgress{"data-0000": {Head: 20, Completed: 12, Floor: 5, Owner: Ownership{Node: "worker", Epoch: 4, LastError: "failure"}}},
		GraphHeads:     map[IndexPartitionID]GraphHead{"index-0000": {Term: 3, Committed: GraphPosition{Sequence: 9}, Visible: GraphPosition{Sequence: 8}, Sources: map[string]uint64{"0": 17}}},
		ReplicaOwners:  map[IndexPartitionID]ReplicaOwners{"index-0000": {Owners: []Ownership{{Partition: "replica-a"}, {Partition: "replica-b"}}}},
		GraphPositions: map[IndexPartitionID]GraphPosition{"replica-a": {Sequence: 6}},
	}
	text := clusterMetrics(s).String()
	var parser expfmt.TextParser
	metrics, err := parser.TextToMetricFamilies(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"vecdb_data_pending_events": 8, "vecdb_data_retained_events": 15,
		"vecdb_data_dispatch_error": 1, "vecdb_graph_unpublished_entries": 1,
		"vecdb_graph_replica_lag_entries": 3, "vecdb_index_source_commit_lsn": 17,
	} {
		if got := metrics[name].GetMetric(); len(got) != 1 || got[0].GetGauge().GetValue() != want {
			t.Fatalf("%s: %v want %v", name, got, want)
		}
	}
	// Missing replica progress must not be silently reported as zero lag.
	if strings.Contains(text, `vecdb_graph_replica_lag_entries{index_partition="index-0000",replica="replica-b"}`) {
		t.Fatal(text)
	}
	if lag(1, 2) != 0 {
		t.Fatal("unsigned lag underflow")
	}
}

func TestDispatchObservationsTrackFailuresAndDuplicates(t *testing.T) {
	f, c, o := routedFixture(t)
	f.write(t, "v", false)
	g := f.services[0].graphReplica
	q := deliveryFor(t, f, o, 0)
	for n := 0; n < 2; n++ {
		if err := g.enqueue(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	bad := q
	bad.RouteVersion++
	if err := g.enqueue(f.ctx, bad); err == nil {
		t.Fatal("bad route accepted")
	}
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.db.commitErr = injected
	if err := c.dispatchSource(f.ctx, 0, &dataDispatcher{owner: o}); err == nil {
		t.Fatal("injection lost")
	}
	f.db.commitErr = nil
	if err := c.dispatchSource(f.ctx, 0, &dataDispatcher{owner: o}); err != nil {
		t.Fatal(err)
	}
	x := &c.observations
	if x.admissions.Load() != 1 || x.duplicates.Load() != 1 || x.rejections.Load() != 1 || x.dispatches.Load() != 2 || x.dispatchErrors.Load() != 1 || x.dispatchNanos.Load() == 0 {
		t.Fatal("observation counts do not match actual outcomes")
	}
}

func TestDataOwnerAvailabilityAfterRelease(t *testing.T) {
	f, c, owner := routedFixture(t)
	s, err := c.Status(f.ctx)
	if err != nil || !s.DataProgress["data-0000"].OwnerAvailable {
		t.Fatal(s, err)
	}
	if _, err = c.coordinator.Update(f.ctx, owner, false, true); err != nil {
		t.Fatal(err)
	}
	s, err = c.Status(f.ctx)
	d := s.DataProgress["data-0000"]
	if err != nil || d.OwnerAvailable || d.Owner.Node == "" || d.Owner.Epoch != owner.Epoch {
		t.Fatal("historical ownership must not imply a live dispatcher", d, err)
	}
}
