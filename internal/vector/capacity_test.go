package vector

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
)

func percentileMS(values []time.Duration, p float64) float64 {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return float64(values[min(len(values)-1, int(float64(len(values))*p))].Microseconds()) / 1000
}
func TestCapacityAcceptance(t *testing.T) {
	if os.Getenv("CAPACITY_ACCEPTANCE") != "1" {
		t.Skip("set CAPACITY_ACCEPTANCE=1")
	}
	reports := []map[string]any{}
	_ = os.MkdirAll("../../reports", 0755)
	_ = os.WriteFile("../../reports/capacity.status", []byte("running\n"), 0644)
	t.Cleanup(func() {
		status := "passed\n"
		if t.Failed() {
			status = "failed\n"
		}
		_ = os.WriteFile("../../reports/capacity.status", []byte(status), 0644)
	})
	sizes := []int{10000, 50000}
	var external []Record
	if path := os.Getenv("VECTOR_DATASET"); path != "" {
		b, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(b, &external); e != nil || len(external) == 0 {
			t.Fatal("dataset must be non-empty Record JSON array", e)
		}
		sizes = []int{len(external)}
	}
	for _, n := range sizes {
		records := external
		source := "seeded synthetic Gaussian"
		if records == nil {
			records = dataset(n, 128)
		} else {
			source = "user-supplied dataset"
		}
		d := len(records[0].Vector)
		if err := validateBatch(records, d); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, r := range records {
			if r.Deleted || seen[r.ID] {
				t.Fatal("capacity dataset requires unique live records")
			}
			seen[r.ID] = true
		}
		config := DefaultIndexConfig()
		if n > 10000 {
			config.EFSearch = 1024
		}
		i, err := NewHNSWIndex(d, config)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if err = i.Apply(records); err != nil {
			t.Fatal(err)
		}
		build := time.Since(started)
		rng := rand.New(rand.NewSource(456))
		queries := make([]Search, 50)
		for j := range queries {
			v := make([]float64, d)
			if external != nil {
				copy(v, records[(j*7919)%len(records)].Vector)
				for k := range v {
					v[k] += rng.NormFloat64() * 0.01
				}
			} else {
				for k := range v {
					v[k] = rng.NormFloat64()
				}
			}
			queries[j] = Search{Vector: v, K: 10}
		}
		overlap, total := 0, 0
		lat := []time.Duration{}
		exactLat := []time.Duration{}
		for _, q := range queries {
			eq := q
			eq.Exact = true
			started = time.Now()
			want, e := i.Search(eq)
			exactLat = append(exactLat, time.Since(started))
			if e != nil {
				t.Fatal(e)
			}
			started = time.Now()
			got, e := i.Search(q)
			lat = append(lat, time.Since(started))
			if e != nil {
				t.Fatal(e)
			}
			ids := map[string]bool{}
			for _, h := range want {
				ids[h.ID] = true
			}
			for _, h := range got {
				if ids[h.ID] {
					overlap++
				}
			}
			total += len(want)
		}
		recall := float64(overlap) / float64(total)
		if recall < 0.95 {
			t.Errorf("N=%d recall %.3f < .95", n, recall)
		}
		concurrency := []map[string]any{}
		for _, serialized := range []bool{true, false} {
			var gate sync.Mutex
			var wg sync.WaitGroup
			const workers = 4
			const perWorker = 100
			times := make([][]time.Duration, workers)
			started = time.Now()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for j := 0; j < perWorker; j++ {
						start := time.Now()
						if serialized {
							gate.Lock()
						}
						_, e := i.Search(queries[(w+j)%len(queries)])
						if serialized {
							gate.Unlock()
						}
						times[w] = append(times[w], time.Since(start))
						if e != nil {
							t.Error(e)
							return
						}
					}
				}(w)
			}
			wg.Wait()
			elapsed := time.Since(started)
			all := []time.Duration{}
			for _, v := range times {
				all = append(all, v...)
			}
			concurrency = append(concurrency, map[string]any{"serialized_baseline": serialized, "workers": workers, "qps": float64(workers*perWorker) / elapsed.Seconds(), "p95_ms": percentileMS(all, .95), "p99_ms": percentileMS(all, .99)})
		}
		// Keep writes flowing while a full graph rebuild catches its delta.
		stopWrites := make(chan struct{})
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			j := 0
			for {
				select {
				case <-stopWrites:
					return
				case <-ticker.C:
					r := cloneRecord(records[j%len(records)])
					r.Revision += uint64(j/len(records) + 1)
					r.Vector[0] += 0.1
					if e := i.Apply([]Record{r}); e != nil {
						t.Error(e)
						return
					}
					j++
				}
			}
		}()
		rebuildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		started = time.Now()
		rebuildErr := i.Rebuild(rebuildCtx)
		rebuildDuration := time.Since(started)
		cancel()
		close(stopWrites)
		<-writerDone
		if rebuildErr != nil {
			t.Fatal(rebuildErr)
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		var usage syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
		rss := uint64(usage.Maxrss)
		if runtime.GOOS == "linux" {
			rss *= 1024
		}
		reports = append(reports, map[string]any{"vectors": n, "dimensions": d, "source": source, "ef_search": config.EFSearch, "recall_at_10": recall, "build_seconds": build.Seconds(), "rebuild_with_writes_seconds": rebuildDuration.Seconds(), "ann_p95_ms": percentileMS(lat, .95), "ann_p99_ms": percentileMS(lat, .99), "exact_p95_ms": percentileMS(exactLat, .95), "process_peak_rss_bytes": rss, "go_heap_alloc_bytes": memory.HeapAlloc, "concurrency": concurrency})
		i.graph.Close()
		runtime.GC()
	}
	data, _ := json.MarshalIndent(map[string]any{"workloads": reports, "scope": "single process; 50 recall queries, 400 queries per concurrency mode; RSS is process lifetime peak including rebuild with target 100 writes/s; external queries perturb sampled vectors, not independent production queries"}, "", "  ")
	_ = os.MkdirAll("../../reports", 0755)
	if e := os.WriteFile("../../reports/capacity.json", data, 0644); e != nil {
		t.Fatal(e)
	}
	t.Log(string(data))
}
