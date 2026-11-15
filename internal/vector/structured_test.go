package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

// Deterministic low-rank topic manifold: correlated dimensions, skewed density,
// local variants and distant outliers. Queries use a different random stream.
func topicVector(topic, d int, rng *rand.Rand) []float64 {
	latent := make([]float64, 8)
	width := .3
	if topic == 0 {
		width = .7
	}
	for j := range latent {
		latent[j] = rng.NormFloat64() * width
	}
	v := make([]float64, d)
	for k := range v {
		v[k] = 3 * math.Sin(float64((topic+1)*(k+1))*.071)
		for j, x := range latent {
			v[k] += x * math.Sin(float64((j+1)*(k+1))*.17) / math.Sqrt(8)
		}
		v[k] += .025 * rng.NormFloat64()
	}
	return v
}
func structuredDataset(n, d int) []Record {
	rng := rand.New(rand.NewSource(20261112))
	records := make([]Record, n)
	for j := range records {
		topic := 0
		if j >= n/2 {
			topic = 1 + (j-n/2)%15
		}
		v := topicVector(topic, d, rng)
		kind := "topic"
		if j%97 == 0 {
			kind = "outlier"
			for k := range v {
				v[k] = float64(k%7-3)*6 + 2*rng.NormFloat64()
			}
		}
		if j > 0 && j%20 == 0 {
			kind = "near_duplicate"
			v = append([]float64(nil), records[j-1].Vector...)
			topic = (j-1-n/2)%15 + 1
			if j-1 < n/2 {
				topic = 0
			}
			for k := range v {
				v[k] += .0001 * rng.NormFloat64()
			}
		}
		records[j] = Record{ID: fmt.Sprintf("structured-%06d", j), Revision: 1, Vector: v, Payload: map[string]string{"topic": fmt.Sprint(topic), "tenant": fmt.Sprint(j % 20), "rare": fmt.Sprint(j % 1000), "kind": kind}}
	}
	return records
}
func TestStructuredGenerator(t *testing.T) {
	a, b := structuredDataset(2000, 32), structuredDataset(2000, 32)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("not reproducible")
	}
	counts := map[string]int{}
	topics := map[string]int{}
	for _, r := range a {
		counts[r.Payload["kind"]]++
		topics[r.Payload["topic"]]++
	}
	if counts["outlier"] == 0 || counts["near_duplicate"] != 99 || topics["0"] < 990 || len(topics) != 16 {
		t.Fatal(counts, topics)
	}
	if l2(a[20].Vector, a[19].Vector) > 1e-4 {
		t.Fatal("near duplicate structure lost")
	}
}

func TestStructuredAcceptance(t *testing.T) {
	if os.Getenv("STRUCTURED_ACCEPTANCE") != "1" {
		t.Skip("set STRUCTURED_ACCEPTANCE=1")
	}
	_ = os.MkdirAll("../../reports", 0755)
	_ = os.WriteFile("../../reports/structured.status", []byte("running\n"), 0644)
	t.Cleanup(func() {
		status := "passed\n"
		if t.Failed() {
			status = "failed\n"
		}
		_ = os.WriteFile("../../reports/structured.status", []byte(status), 0644)
	})
	reports := []map[string]any{}
	for _, n := range []int{10000, 50000} {
		records := structuredDataset(n, 128)
		sample := []Record{}
		for _, j := range []int{0, 1, 19, 20, n / 2, n/2 + 1, n - 1} {
			sample = append(sample, records[j])
		}
		sampleBytes, _ := json.MarshalIndent(sample, "", "  ")
		if err := os.WriteFile(fmt.Sprintf("../../reports/structured-sample-%d.json", n), sampleBytes, 0644); err != nil {
			t.Fatal(err)
		}
		config := DefaultIndexConfig()
		config.EFSearch = 512
		if n > 10000 {
			config.EFSearch = 1024
		}
		index, e := NewHNSWIndex(128, config)
		if e != nil {
			t.Fatal(e)
		}
		start := time.Now()
		if e = index.Apply(records); e != nil {
			t.Fatal(e)
		}
		build := time.Since(start)
		rng := rand.New(rand.NewSource(20261113))
		scenarios := map[string][]Search{}
		for j := 0; j < 40; j++ {
			topic := j % 16
			q := topicVector(topic, 128, rng)
			scenarios["topic_independent"] = append(scenarios["topic_independent"], Search{Vector: q, K: 10})
			boundary := topicVector((topic+1)%16, 128, rng)
			for k := range boundary {
				boundary[k] = (boundary[k] + q[k]) / 2
			}
			scenarios["topic_boundary"] = append(scenarios["topic_boundary"], Search{Vector: boundary, K: 10})
			near := append([]float64(nil), records[20*(j+1)].Vector...)
			for k := range near {
				near[k] += .00005 * rng.NormFloat64()
			}
			scenarios["near_duplicate"] = append(scenarios["near_duplicate"], Search{Vector: near, K: 10})
			far := make([]float64, 128)
			for k := range far {
				far[k] = float64(k%7-3)*6 + 2*rng.NormFloat64()
			}
			scenarios["outlier_independent"] = append(scenarios["outlier_independent"], Search{Vector: far, K: 10})
			scenarios["correlated_topic_filter"] = append(scenarios["correlated_topic_filter"], Search{Vector: q, K: 10, Filter: map[string]string{"topic": fmt.Sprint(topic)}})
			scenarios["cross_tenant_filter"] = append(scenarios["cross_tenant_filter"], Search{Vector: q, K: 10, Filter: map[string]string{"tenant": fmt.Sprint(j % 20)}})
			scenarios["rare_and_filter"] = append(scenarios["rare_and_filter"], Search{Vector: q, K: 10, Filter: map[string]string{"rare": fmt.Sprint(j), "tenant": fmt.Sprint(j % 20)}})
			scenarios["empty_filter"] = append(scenarios["empty_filter"], Search{Vector: q, K: 10, Filter: map[string]string{"topic": "absent"}})
		}
		measure := func(stage string) {
			for _, name := range []string{"topic_independent", "topic_boundary", "near_duplicate", "outlier_independent", "correlated_topic_filter", "cross_tenant_filter", "rare_and_filter", "empty_filter"} {
				overlap, total, fallbacks, computations := 0, 0, 0, 0
				lat := []time.Duration{}
				exactLat := []time.Duration{}
				plans := map[string]int{}
				for _, q := range scenarios[name] {
					truthQuery := q
					truthQuery.Exact = true
					start := time.Now()
					truth, err := index.Search(truthQuery)
					exactLat = append(exactLat, time.Since(start))
					if err != nil {
						t.Fatal(err)
					}
					start = time.Now()
					got, stats, err := index.SearchWithStats(q)
					lat = append(lat, time.Since(start))
					if err != nil || len(got) != len(truth) {
						t.Fatal(name, "result count", len(got), len(truth), err)
					}
					plans[stats.Plan]++
					if stats.Fallback {
						fallbacks++
					}
					computations += stats.DistanceComputations
					ids := map[string]bool{}
					for _, h := range truth {
						ids[h.ID] = true
					}
					seen := map[string]bool{}
					for _, h := range got {
						if seen[h.ID] {
							t.Fatal("duplicate hit")
						}
						seen[h.ID] = true
						current := index.records[h.ID]
						if current.Deleted || current.Revision != h.Revision || h.Distance != l2(q.Vector, current.Vector) || !reflect.DeepEqual(h.Payload, current.Payload) {
							t.Fatal("stale or inconsistent hit", name, h.ID)
						}
						for field, value := range q.Filter {
							if actual, ok := h.Payload[field]; !ok || actual != value {
								t.Fatal("filter leak", name, h)
							}
						}
						if ids[h.ID] {
							overlap++
						}
					}
					total += len(truth)
				}
				recall := 1.0
				if total != 0 {
					recall = float64(overlap) / float64(total)
				}
				reports = append(reports, map[string]any{"vectors": n, "stage": stage, "scenario": name, "queries": len(scenarios[name]), "ef_search": config.EFSearch, "recall_at_10": recall, "returned_truth_count": total, "p95_ms": percentileMS(lat, .95), "p99_ms": percentileMS(lat, .99), "exact_p95_ms": percentileMS(exactLat, .95), "fallback_queries": fallbacks, "plans": plans, "mean_distance_computations": float64(computations) / float64(len(lat)), "build_seconds": build.Seconds()})
				if recall < .95 {
					t.Errorf("N=%d stage=%s scenario=%s Recall@10 %.3f below .95", n, stage, name, recall)
				}
			}
		}
		measure("initial")
		// Move 5% of records across topics and delete another 5%; filters change too.
		changes := make([]Record, 0, n/10)
		for j := 0; j < n; j++ {
			if j%20 != 0 && j%20 != 1 {
				continue
			}
			r := cloneRecord(records[j])
			r.Revision = 2
			if j%20 == 0 {
				r.Deleted = true
				r.Vector = nil
				r.Payload = nil
			} else {
				topic := (j + 7) % 16
				r.Vector = topicVector(topic, 128, rng)
				r.Payload["topic"] = fmt.Sprint(topic)
				r.Payload["tenant"] = "moved"
			}
			changes = append(changes, r)
		}
		if e = index.Apply(changes); e != nil {
			t.Fatal(e)
		}
		measure("updated_deleted")
		// Exact full-view identity and ANN queries survive persisted graph restore and rebuild.
		state, err := index.ExportState()
		if err != nil {
			t.Fatal(err)
		}
		restored, err := RestoreHNSW(state)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(index.Snapshot(), restored.Snapshot()) {
			t.Fatal("snapshot lost records")
		}
		index.graph.Close()
		index = restored
		if e = index.Rebuild(context.Background()); e != nil {
			t.Fatal(e)
		}
		if index.Stats().StaleNodes != 0 {
			t.Fatal("rebuild retained stale nodes")
		}
		measure("restored_rebuilt")
		index.graph.Close()
	}
	status := "passed"
	if t.Failed() {
		status = "failed"
	}
	data, _ := json.MarshalIndent(map[string]any{"status": status, "seed": 20261112, "query_seed": 20261113, "dimensions": 128, "distribution": "16 low-rank topics; dominant topic ~50%; 5% near duplicates; ~1% outliers; topic-correlated and cross-cutting attributes", "scope": "structured synthetic data, not production embeddings; exact search is ground truth; empty-result recall=1 by convention, check returned_truth_count", "results": reports}, "", "  ")
	if e := os.WriteFile("../../reports/structured.json", data, 0644); e != nil {
		t.Fatal(e)
	}
}
