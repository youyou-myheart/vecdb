package vector

import (
	"strings"
	"testing"
)

func TestStatusDuringGraphRunClosureOmitsUnverifiedProgress(t *testing.T) {
	f := newGraphFixture(t)
	c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, graphReplication: true,
		PartitionedService: &PartitionedService{layout: f.layout, data: map[DataPartitionID]*Repository{0: f.r}}}
	j := f.services[0].graphReplica.journal
	h, err := j.Head(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	original := h
	h.RawActive = &graphRawRun{ID: "closing", After: h.Committed.Sequence, Closing: true}
	seed(f.db, j.key("head"), h)
	state, err := c.Status(f.ctx)
	if err != nil || state.Ready || state.FullyReplicated {
		t.Fatal("closure failed status or reported ready", state, err)
	}
	if _, found := state.GraphHeads[j.id]; found {
		t.Fatal("unverified head reported")
	}
	if strings.Contains(clusterMetrics(state).String(), "vecdb_graph_committed_sequence") {
		t.Fatal("unknown progress reported as zero")
	}
	seed(f.db, j.key("head"), original)
	state, err = c.Status(f.ctx)
	if err != nil || !state.Ready {
		t.Fatal("status did not recover", state, err)
	}
	// Actual data corruption must still be returned, not hidden as takeover.
	original.Visible.Sequence++
	seed(f.db, j.key("head"), original)
	if _, err = c.Status(f.ctx); err == nil {
		t.Fatal("corrupt head hidden")
	}
}
