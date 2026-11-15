package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func dataset(n, d int) []Record {
	rng := rand.New(rand.NewSource(123))
	out := make([]Record, n)
	for j := range out {
		v := make([]float64, d)
		for k := range v {
			v[k] = rng.NormFloat64()
		}
		out[j] = Record{ID: fmt.Sprintf("id-%06d", j), Revision: 1, Vector: v, Payload: map[string]string{"group": fmt.Sprint(j % 10), "rare": fmt.Sprint(j % 1000)}}
	}
	return out
}
func mustGraph(t *testing.T, d int) *HNSWIndex {
	t.Helper()
	i, e := NewHNSWIndex(d, DefaultIndexConfig())
	if e != nil {
		t.Fatal(e)
	}
	return i
}
func TestHeapMatchesFullSort(t *testing.T) {
	records := dataset(200, 8)
	i := NewIndex(8)
	if e := i.Apply(records); e != nil {
		t.Fatal(e)
	}
	for _, k := range []int{1, 10, 100, 300} {
		q := Search{Vector: records[7].Vector, K: k}
		got, e := i.Search(q)
		if e != nil {
			t.Fatal(e)
		}
		want := legacySearch(records, q)
		if !reflect.DeepEqual(got, want) {
			t.Fatal("heap changed results", k)
		}
	}
}
func legacySearch(records []Record, q Search) []Hit {
	out := []Hit{}
	for _, r := range records {
		if !r.Deleted && matches(r, q.Filter) {
			out = append(out, Hit{r.ID, r.Revision, l2(q.Vector, r.Vector), cloneRecord(r).Payload})
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Distance == out[b].Distance {
			return out[a].ID < out[b].ID
		}
		return out[a].Distance < out[b].Distance
	})
	if len(out) > q.K {
		out = out[:q.K]
	}
	return out
}
func TestHNSWFilterVersionsAndRebuild(t *testing.T) {
	i := mustGraph(t, 8)
	records := dataset(500, 8)
	if e := i.Apply(records); e != nil {
		t.Fatal(e)
	}
	q := Search{Vector: records[0].Vector, K: 5, Filter: map[string]string{"rare": "0"}}
	hits, stats, e := i.SearchWithStats(q)
	if e != nil || len(hits) != 1 || hits[0].ID != records[0].ID || stats.Plan != "exact" || stats.DistanceComputations != 1 {
		t.Fatal(hits, stats, e)
	}
	if e = i.Apply([]Record{{ID: records[0].ID, Revision: 2, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	_ = i.Apply([]Record{records[0]})
	hits, _, e = i.SearchWithStats(q)
	if e != nil || len(hits) != 0 {
		t.Fatal(hits, e)
	}
	r := records[0]
	r.Revision = 3
	r.Payload = map[string]string{"group": "new"}
	_ = i.Apply([]Record{r})
	hits, e = i.Search(Search{Vector: r.Vector, K: 1, Filter: map[string]string{"group": "new"}, MinVisible: &Token{r.ID, 3}})
	if e != nil || len(hits) != 1 {
		t.Fatal(hits, e)
	}
	if i.Stats().StaleNodes != 1 {
		t.Fatal(i.Stats())
	}
	before := i.Snapshot()
	if e = i.Rebuild(context.Background()); e != nil {
		t.Fatal(e)
	}
	if i.Stats().StaleNodes != 0 || !reflect.DeepEqual(before, i.Snapshot()) {
		t.Fatal("rebuild changed logical records")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = i.Rebuild(ctx); e == nil {
		t.Fatal("canceled rebuild published")
	}
	fresh := mustGraph(t, 8)
	if e = i.publishRebuild(0, fresh); e != ErrConflict {
		t.Fatal("stale rebuild published", e)
	}
}
func TestHNSWFallbackAndValidation(t *testing.T) {
	c := DefaultIndexConfig()
	c.ExactThreshold = 0
	c.MaxCandidates = 1
	i, e := NewHNSWIndex(8, c)
	if e != nil {
		t.Fatal(e)
	}
	_ = i.Apply(dataset(200, 8))
	hits, stats, e := i.SearchWithStats(Search{Vector: make([]float64, 8), K: 10, Filter: map[string]string{"group": "3"}})
	if e != nil || len(hits) != 10 || !stats.Fallback {
		t.Fatal(hits, stats, e)
	}
	for _, q := range []Search{{Vector: make([]float64, 8), K: 1, EFSearch: 4097}, {Vector: make([]float64, 8), K: 1, MinVisible: &Token{"missing", 1}}} {
		if _, e = i.Search(q); e == nil {
			t.Fatal("bad query accepted")
		}
	}
	before := i.Stats()
	if e = i.Apply([]Record{{ID: "valid", Revision: 1, Vector: make([]float64, 8)}, {ID: "bad", Revision: 1, Vector: []float64{0}}}); e == nil || i.Stats() != before {
		t.Fatal("partial batch")
	}
	if _, e = NewHNSWIndex(0, c); e == nil {
		t.Fatal("bad dimension")
	}
	c.M = 0
	if _, e = NewHNSWIndex(8, c); e == nil {
		t.Fatal("bad config")
	}
}
func TestHNSWGraphSnapshot(t *testing.T) {
	i := mustGraph(t, 8)
	records := dataset(300, 8)
	_ = i.Apply(records)
	_ = i.Apply([]Record{{ID: records[0].ID, Revision: 2, Deleted: true}})
	state, e := i.ExportState()
	if e != nil {
		t.Fatal(e)
	}
	restored, e := RestoreHNSW(state)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(i.Snapshot(), restored.Snapshot()) || i.Stats() != restored.Stats() {
		t.Fatal("state mismatch")
	}
	if e = restored.Apply([]Record{{ID: "after-restore", Revision: 1, Vector: make([]float64, 8)}}); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"format", "dimension", "truncated", "trailing", "mapping", "record-vector", "duplicate-record", "missing-live-node"} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(state)
			var s IndexSnapshot
			_ = json.Unmarshal(raw, &s)
			switch name {
			case "format":
				s.Format = "unknown"
			case "dimension":
				s.Dimension++
			case "truncated":
				s.Graph = s.Graph[:len(s.Graph)/2]
			case "trailing":
				s.Graph = append(s.Graph, 0)
			case "mapping":
				s.Next = 0
			case "record-vector":
				s.Records[1].Vector[0]++
			case "duplicate-record":
				s.Records = append(s.Records, s.Records[0])
			case "missing-live-node":
				s.Nodes = map[uint64]nodeVersion{}
			}
			if _, e := RestoreHNSW(s); e == nil {
				t.Fatal("corrupt state accepted")
			}
		})
	}
	// Empty graphs and all-deleted graphs are valid snapshots.
	for _, n := range []int{0, 1} {
		x := mustGraph(t, 2)
		if n == 1 {
			_ = x.Apply([]Record{{ID: "a", Revision: 1, Vector: []float64{1, 2}}, {ID: "a", Revision: 2, Deleted: true}})
		}
		s, e := x.ExportState()
		if e != nil {
			t.Fatal(e)
		}
		if _, e = RestoreHNSW(s); e != nil {
			t.Fatal(e)
		}
	}
}
func TestPayloadIntersection(t *testing.T) {
	p := payloadIndex{}
	p.add(Record{ID: "a", Payload: map[string]string{"x": "", "y": "b"}})
	p.add(Record{ID: "b", Payload: map[string]string{"y": "b"}})
	if len(p.selectIDs(map[string]string{"x": "", "y": "b"})) != 1 {
		t.Fatal("intersection")
	}
	if len(p.selectIDs(map[string]string{"missing": ""})) != 0 {
		t.Fatal("missing")
	}
	if p.selectIDs(nil) != nil {
		t.Fatal("unfiltered")
	}
	p.remove(Record{ID: "a", Payload: map[string]string{"x": "", "y": "b"}})
	if len(p.selectIDs(map[string]string{"x": ""})) != 0 {
		t.Fatal("stale posting")
	}
}
func TestHNSWConcurrentSearchApplyRebuild(t *testing.T) {
	i := mustGraph(t, 8)
	records := dataset(300, 8)
	_ = i.Apply(records)
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if n == 0 {
					r := records[j]
					r.Revision = uint64(j + 2)
					if e := i.Apply([]Record{r}); e != nil {
						t.Error(e)
					}
				} else {
					if _, e := i.Search(Search{Vector: records[j].Vector, K: 10}); e != nil {
						t.Error(e)
					}
				}
			}
		}(n)
	}
	if e := i.Rebuild(context.Background()); e != nil && e != ErrConflict {
		t.Fatal(e)
	}
	wg.Wait()
}
func TestHNSWRecallAcceptance(t *testing.T) {
	if os.Getenv("ANN_ACCEPTANCE") != "1" {
		t.Skip("set ANN_ACCEPTANCE=1 for recall/performance acceptance")
	}
	const n = 10000
	const dim = 128
	const queries = 50
	records := dataset(n, dim)
	exact := NewIndex(dim)
	_ = exact.Apply(records)
	ann := mustGraph(t, dim)
	start := time.Now()
	if e := ann.Apply(records); e != nil {
		t.Fatal(e)
	}
	build := time.Since(start)
	rng := rand.New(rand.NewSource(2026))
	total := 0
	overlap := 0
	distances := 0
	var exactTime, annTime time.Duration
	for j := 0; j < queries; j++ {
		v := make([]float64, dim)
		for k := range v {
			v[k] = rng.NormFloat64()
		}
		q := Search{Vector: v, K: 10}
		start = time.Now()
		truth, e := exact.Search(q)
		exactTime += time.Since(start)
		if e != nil {
			t.Fatal(e)
		}
		start = time.Now()
		got, stats, e := ann.SearchWithStats(q)
		annTime += time.Since(start)
		if e != nil || stats.Plan != "hnsw" {
			t.Fatal(stats, e)
		}
		distances += stats.DistanceComputations
		set := map[string]bool{}
		for _, h := range truth {
			set[h.ID] = true
		}
		for _, h := range got {
			if set[h.ID] {
				overlap++
			}
		}
		total += len(truth)
	}
	recall := float64(overlap) / float64(total)
	result := map[string]any{"vectors": n, "dimensions": dim, "queries": queries, "k": 10, "recall_at_10": recall, "mean_distance_computations": float64(distances) / queries, "exact_mean_ms": float64(exactTime.Microseconds()) / queries / 1000, "hnsw_mean_ms": float64(annTime.Microseconds()) / queries / 1000, "build_seconds": build.Seconds(), "scope": "seeded synthetic normal vectors; single process; no production latency SLA"}
	b, _ := json.MarshalIndent(result, "", "  ")
	_ = os.MkdirAll("../../reports", 0755)
	_ = os.WriteFile("../../reports/ann-acceptance.json", b, 0644)
	t.Log(string(b))
	if recall < 0.95 {
		t.Fatalf("recall %.3f below .95", recall)
	}
	if distances >= n*queries {
		t.Fatal("ANN did not reduce distance computations")
	}
}
func BenchmarkSearch(b *testing.B) {
	records := dataset(10000, 128)
	exact := NewIndex(128)
	_ = exact.Apply(records)
	ann, _ := NewHNSWIndex(128, DefaultIndexConfig())
	_ = ann.Apply(records)
	q := Search{Vector: records[100].Vector, K: 10}
	b.Run("legacy-full-sort-copy", func(b *testing.B) {
		b.ReportAllocs()
		for j := 0; j < b.N; j++ {
			_ = legacySearch(records, q)
		}
	})
	b.Run("exact-heap", func(b *testing.B) {
		b.ReportAllocs()
		for j := 0; j < b.N; j++ {
			_, _ = exact.Search(q)
		}
	})
	b.Run("hnsw", func(b *testing.B) {
		b.ReportAllocs()
		for j := 0; j < b.N; j++ {
			_, _ = ann.Search(q)
		}
	})
	b.Run("filtered-exact", func(b *testing.B) {
		q.Filter = map[string]string{"rare": "100"}
		b.ReportAllocs()
		for j := 0; j < b.N; j++ {
			_, _ = ann.Search(q)
		}
	})
}

func TestHNSWAutomaticRebuildEligibility(t *testing.T) {
	i := mustGraph(t, 8)
	records := dataset(300, 8)
	_ = i.Apply(records)
	if i.NeedsRebuild() {
		t.Fatal("fresh graph")
	}
	for j := 0; j < 150; j++ {
		r := records[j]
		r.Revision = 2
		_ = i.Apply([]Record{r})
	}
	if !i.NeedsRebuild() {
		t.Fatal("stale graph not eligible")
	}
	if e := i.Rebuild(context.Background()); e != nil {
		t.Fatal(e)
	}
	if i.NeedsRebuild() || i.Stats().StaleNodes != 0 {
		t.Fatal(i.Stats())
	}
}
func TestGraphObjectAndServiceRecovery(t *testing.T) {
	ctx := context.Background()
	blobs := &fakeBlobs{exists: true}
	objects := &Objects{Client: blobs, Bucket: "test"}
	repo := &fakeRepo{events: dataset(300, 2)}
	service, e := NewService(ctx, repo, objects)
	if e != nil {
		t.Fatal(e)
	}
	m, e := service.Checkpoint(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if m.Format != graphFormat {
		t.Fatal(m)
	}
	repo.events = append(repo.events, Record{ID: repo.events[0].ID, Revision: 2, Deleted: true})
	restored, e := NewService(ctx, repo, objects)
	if e != nil {
		t.Fatal(e)
	}
	hits, e := restored.Search(Search{Vector: []float64{0, 0}, K: 1000, Exact: true})
	if e != nil || len(hits) != 299 {
		t.Fatal(len(hits), e)
	}
	if e = restored.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	if restored.Index.Stats().StaleNodes != 0 {
		t.Fatal(restored.Index.Stats())
	}
	old := append([]byte(nil), blobs.data...)
	blobs.data = []byte("bad")
	if _, e = NewService(ctx, repo, objects); e == nil {
		t.Fatal("bad graph accepted")
	}
	blobs.data = old
	repo.manifest.Dimension = 3
	if _, e = NewService(ctx, repo, objects); e == nil {
		t.Fatal("dimension mismatch")
	}
	repo.manifest.Dimension = 2
	repo.manifest.Format = "future"
	if _, e = NewService(ctx, repo, objects); e == nil {
		t.Fatal("format mismatch")
	}
}

func TestHNSWFilteredANNAndRestoredSearch(t *testing.T) {
	i := mustGraph(t, 8)
	records := dataset(2000, 8)
	if err := i.Apply(records); err != nil {
		t.Fatal(err)
	}
	// More than ExactThreshold matches exercises graph filtering, not tiny-set fallback.
	q := Search{Vector: records[3].Vector, K: 10, Filter: map[string]string{"group": "3"}}
	hits, stats, err := i.SearchWithStats(q)
	if err != nil || stats.Plan != "hnsw" || len(hits) != 10 {
		t.Fatal(stats, err, len(hits))
	}
	for _, h := range hits {
		if h.Payload["group"] != "3" {
			t.Fatal(h)
		}
	}
	// Old graph nodes must not expose old payloads or deleted records.
	updates := []Record{{ID: hits[0].ID, Revision: 2, Deleted: true}, {ID: hits[1].ID, Revision: 2, Vector: records[3].Vector, Payload: map[string]string{"group": "other"}}}
	if err = i.Apply(updates); err != nil {
		t.Fatal(err)
	}
	hits, err = i.Search(q)
	if err != nil || len(hits) != 10 {
		t.Fatal(hits, err)
	}
	for _, h := range hits {
		if h.ID == updates[0].ID || h.ID == updates[1].ID {
			t.Fatal("stale hit", h)
		}
	}
	state, err := i.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreHNSW(state)
	if err != nil {
		t.Fatal(err)
	}
	after, err := restored.Search(q)
	if err != nil || !reflect.DeepEqual(hits, after) {
		t.Fatal("graph restore changed hits", err)
	}
	if err = restored.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.Exact = true
	want, _ := i.Search(q)
	got, _ := restored.Search(q)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("rebuild changed exact results")
	}
}

func TestRebuildCatchesConcurrentWrites(t *testing.T) {
	i := mustGraph(t, 16)
	records := dataset(5000, 16)
	if e := i.Apply(records); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { done <- i.Rebuild(ctx) }()
	for {
		i.mu.RLock()
		active := i.rebuildDelta != nil
		i.mu.RUnlock()
		if active {
			break
		}
		select {
		case e := <-done:
			t.Fatal("rebuild finished before update exercise", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for j := 0; j < 300; j++ {
		r := records[j]
		r.Revision = 2
		if j%3 == 0 {
			r.Deleted = true
			r.Vector = nil
			r.Payload = nil
		} else {
			r.Vector = make([]float64, 16)
			r.Payload = map[string]string{"updated": "yes"}
		}
		if e := i.Apply([]Record{r}); e != nil {
			t.Fatal(e)
		}
	}
	if e := <-done; e != nil {
		t.Fatal("rebuild failed under writes", e)
	}
	snapshot := i.Snapshot()
	for _, r := range snapshot {
		if r.ID <= records[299].ID && r.Revision != 2 {
			t.Fatal("lost update", r)
		}
	}
	if i.Stats().Live != 4900 {
		t.Fatal(i.Stats())
	}
	hits, e := i.Search(Search{Vector: make([]float64, 16), K: 1000, Filter: map[string]string{"updated": "yes"}, Exact: true})
	if e != nil || len(hits) != 200 {
		t.Fatal(len(hits), e)
	}
}
