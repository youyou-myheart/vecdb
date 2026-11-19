package vector

import (
	"context"
	"hash/fnv"
	"math"
	"math/rand"
)

const (
	recallAnchorCount = 8
	recallK           = 10
)

// Bound CPU and the memory used for retained vector references across all local shards.
var recallProbeSlot = make(chan struct{}, 1)

type recallProbeResult struct {
	Anchors        []string
	Recall         float64
	OriginalRecall float64
	NearbyRecall   float64
	Queries        int
	Epoch          uint64
	Live           int
}

type recallQuery struct {
	vector []float64
	hits   []Hit
	exact  bool
}

// Reuse still-live anchors across windows. Replace missing anchors by a deterministic
// reservoir sample from the oldest quarter of live graph labels, skipping obsolete nodes.
func (i *HNSWIndex) recallAnchorsLocked(ctx context.Context, previous []string) ([]string, error) {
	target := min(recallAnchorCount, i.live)
	anchors := make([]string, 0, target)
	kept := make(map[string]bool, target)
	for _, id := range previous {
		if r, ok := i.records[id]; ok && !r.Deleted && !kept[id] {
			anchors = append(anchors, id)
			kept[id] = true
			if len(anchors) == target {
				return anchors, nil
			}
		}
	}

	needed := target - len(anchors)
	oldest := max(target, i.live/4)
	sample := make([]string, 0, needed)
	rng := rand.New(rand.NewSource(42))
	live, eligible := 0, 0
	for n := uint64(0); n < i.next && live < oldest; n++ {
		if n%256 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		node := i.nodes[n+1]
		r, ok := i.records[node.ID]
		if !ok || r.Deleted || r.Revision != node.Revision {
			continue
		}
		live++
		if kept[r.ID] {
			continue
		}
		eligible++
		if len(sample) < needed {
			sample = append(sample, r.ID)
		} else if replace := rng.Intn(eligible); replace < needed {
			sample[replace] = r.ID
		}
	}
	return append(anchors, sample...), nil
}

func nearbyRecallVector(r Record) []float64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(r.ID))
	rng := rand.New(rand.NewSource(int64(hash.Sum64())))
	var norm float64
	for _, x := range r.Vector {
		norm += x * x
	}
	scale := max(1e-6, 0.02*math.Sqrt(norm/float64(len(r.Vector))))
	v := make([]float64, len(r.Vector))
	for n, x := range r.Vector {
		v[n] = max(-1e15, min(1e15, x+(2*rng.Float64()-1)*scale))
	}
	return v
}

// Records own immutable vector slices. Holding their references lets the expensive
// exhaustive pass use the exact ANN view after releasing the index read lock.
func (i *HNSWIndex) recallView(ctx context.Context, previous []string) (recallProbeResult, []recallQuery, [][]float64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	result := recallProbeResult{Epoch: i.epoch, Live: i.live}
	if err := ctx.Err(); err != nil {
		return result, nil, nil, err
	}
	if i.poison != nil {
		return result, nil, nil, i.poison
	}
	if i.live <= recallK || i.live <= i.exactThreshold {
		return result, nil, nil, nil
	}
	anchors, err := i.recallAnchorsLocked(ctx, previous)
	if err != nil {
		return result, nil, nil, err
	}
	result.Anchors = anchors
	queries := make([]recallQuery, 0, len(anchors)*2)
	needsOracle := false
	for _, id := range anchors {
		r := i.records[id]
		for _, v := range [][]float64{r.Vector, nearbyRecallVector(r)} {
			if err := ctx.Err(); err != nil {
				return result, nil, nil, err
			}
			// An exact fallback has perfect recall. Avoid its exhaustive scan while
			// holding the index lock; only approximate hits need an exact oracle.
			hits, stats, err := i.searchWithStatsLocked(Search{Vector: v, K: recallK}, false)
			if err != nil {
				return result, nil, nil, err
			}
			exact := stats.Fallback || stats.Plan == "exact"
			queries = append(queries, recallQuery{vector: v, hits: hits, exact: exact})
			needsOracle = needsOracle || !exact
		}
	}
	if !needsOracle {
		return result, queries, nil, nil
	}
	vectors := make([][]float64, 0, i.live)
	visited := 0
	for _, r := range i.records {
		if visited%256 == 0 && ctx.Err() != nil {
			return result, nil, nil, ctx.Err()
		}
		visited++
		if !r.Deleted {
			vectors = append(vectors, r.Vector)
		}
	}
	return result, queries, vectors, nil
}

func (i *HNSWIndex) probeRecall(ctx context.Context, previous []string) (recallProbeResult, error) {
	select {
	case recallProbeSlot <- struct{}{}:
		defer func() { <-recallProbeSlot }()
	case <-ctx.Done():
		return recallProbeResult{}, ctx.Err()
	}
	result, queries, vectors, err := i.recallView(ctx, previous)
	if err != nil || len(queries) == 0 {
		return result, err
	}
	bounds := make([]topK, len(queries))
	for q := range bounds {
		bounds[q] = make(topK, 0, recallK)
	}
	for n, v := range vectors {
		if n%64 == 0 && ctx.Err() != nil {
			return result, ctx.Err()
		}
		for j, q := range queries {
			if !q.exact {
				bounds[j].offer(candidate{distance: l2(q.vector, v)}, recallK)
			}
		}
	}
	var found [2]int
	for j, q := range queries {
		if q.exact {
			found[j%2] += recallK
		} else {
			found[j%2] += recallMatches(bounds[j], q.hits)
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	denominator := float64(len(result.Anchors) * recallK)
	result.OriginalRecall = float64(found[0]) / denominator
	result.NearbyRecall = float64(found[1]) / denominator
	result.Recall = (result.OriginalRecall + result.NearbyRecall) / 2
	result.Queries = len(queries)
	return result, nil
}

// Distinct records at the kth distance are interchangeable, but they cannot
// replace missing neighbors strictly closer than the boundary.
func recallMatches(closest topK, hits []Hit) int {
	cutoff := closest[0].distance
	slots := 0
	for _, n := range closest {
		if n.distance == cutoff {
			slots++
		}
	}
	strict, ties := 0, 0
	for _, h := range hits {
		if h.Distance < cutoff {
			strict++
		} else if h.Distance == cutoff {
			ties++
		}
	}
	return strict + min(ties, slots)
}
