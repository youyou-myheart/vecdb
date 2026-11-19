package vector

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestRecallBoundaryTiesCannotReplaceStrictNeighbors(t *testing.T) {
	closest := make(topK, 0, recallK)
	closest.offer(candidate{distance: 0}, recallK)
	hits := make([]Hit, recallK)
	for n := range hits {
		closest.offer(candidate{distance: 1}, recallK)
		hits[n].Distance = 1
	}
	if got := recallMatches(closest, hits); got != 9 {
		t.Fatalf("missing zero-distance neighbor: credited %d of 10", got)
	}
	hits[3].Distance = 0
	if got := recallMatches(closest, hits); got != 10 {
		t.Fatalf("equivalent boundary neighbors: credited %d of 10", got)
	}
	hits[9].Distance = 2
	if got := recallMatches(closest, hits); got != 9 {
		t.Fatalf("neighbor outside boundary: credited %d of 10", got)
	}
}

func TestRecallProbeReusesOlderLiveAnchors(t *testing.T) {
	cfg := DefaultIndexConfig()
	cfg.ExactThreshold = 0
	index, err := NewHNSWIndex(8, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer index.graph.Close()
	rows := dataset(80, 8)
	if err := index.Apply(rows); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := index.probeRecall(ctx, nil)
	if err != nil || first.Queries != 2*recallAnchorCount || first.Recall != 1 {
		t.Fatalf("initial recall probe: %+v %v", first, err)
	}
	for _, id := range first.Anchors {
		if id > rows[19].ID {
			t.Fatalf("anchor not in oldest quarter of live records: %s", id)
		}
	}
	field := index.records[first.Anchors[0]]
	if nearby := nearbyRecallVector(field); slices.Equal(nearby, field.Vector) || !slices.Equal(nearby, nearbyRecallVector(field)) {
		t.Fatal("nearby queries should be reproducible and differ from the original")
	}
	more := dataset(80, 8)
	for n := range more {
		more[n].ID = "new-" + more[n].ID
	}
	if err := index.Apply(more); err != nil {
		t.Fatal(err)
	}
	second, err := index.probeRecall(ctx, first.Anchors)
	if err != nil || !slices.Equal(second.Anchors, first.Anchors) || second.Epoch <= first.Epoch {
		t.Fatalf("window anchors changed: %+v %v", second, err)
	}
	if err := index.Apply([]Record{{ID: first.Anchors[0], Revision: 2, Deleted: true}}); err != nil {
		t.Fatal(err)
	}
	third, err := index.probeRecall(ctx, first.Anchors)
	if err != nil || third.Queries != 2*recallAnchorCount || slices.Contains(third.Anchors, first.Anchors[0]) || !slices.Equal(third.Anchors[:recallAnchorCount-1], first.Anchors[1:]) {
		t.Fatalf("removed anchor was not replaced: %+v %v", third, err)
	}

	index.exactThreshold = index.live
	if skipped, err := index.probeRecall(ctx, nil); err != nil || skipped.Queries != 0 || len(skipped.Anchors) != 0 {
		t.Fatalf("small shard should skip sampling: %+v %v", skipped, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := index.probeRecall(canceled, first.Anchors); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe returned %v", err)
	}
}

func TestRecallProbeRecognizesExactFallbackWithoutScanning(t *testing.T) {
	cfg := DefaultIndexConfig()
	cfg.ExactThreshold = 0
	cfg.MaxCandidates = 1
	index, err := NewHNSWIndex(3, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer index.graph.Close()
	if err := index.Apply(dataset(32, 3)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, queries, vectors, err := index.recallView(ctx, nil)
	if err != nil || len(vectors) != 0 || len(queries) != 2*recallAnchorCount {
		t.Fatalf("unexpected exact-fallback view: queries=%d vectors=%d err=%v", len(queries), len(vectors), err)
	}
	for _, q := range queries {
		if !q.exact || len(q.hits) != 0 {
			t.Fatal("probe materialized fallback hits")
		}
	}
	result, err := index.probeRecall(ctx, nil)
	if err != nil || result.Recall != 1 {
		t.Fatalf("exact fallback recall: %+v %v", result, err)
	}
	hits, stats, err := index.SearchWithStats(Search{Vector: queries[0].vector, K: recallK})
	if err != nil || !stats.Fallback || len(hits) != recallK {
		t.Fatalf("normal caller missing fallback results: %v %+v %v", hits, stats, err)
	}
}
