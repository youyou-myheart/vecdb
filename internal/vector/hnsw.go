package vector

import (
	"context"
	"errors"
	"math/rand"

	"sort"
	"sync"

	"example.com/vector-storage/internal/ann"
)

type IndexConfig struct {
	M              int `json:"m"`
	EFConstruction int `json:"ef_construction"`
	EFSearch       int `json:"ef_search"`
	ExactThreshold int `json:"exact_threshold"`
	MaxCandidates  int `json:"max_candidates"`
}

func DefaultIndexConfig() IndexConfig { return IndexConfig{16, 200, 256, 128, 4096} }
func (c IndexConfig) validate() error {
	if c.M < 2 || c.M > 64 || c.EFConstruction < c.M || c.EFConstruction > 4096 || c.EFSearch < 1 || c.EFSearch > 4096 || c.ExactThreshold < 0 || c.MaxCandidates < 1 || c.MaxCandidates > 100000 {
		return errors.New("invalid HNSW configuration")
	}
	return nil
}
func float32Vector(v []float64) []float32 {
	out := make([]float32, len(v))
	for j, x := range v {
		out[j] = float32(x)
	}
	return out
}

type nodeVersion struct {
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
}
type HNSWIndex struct {
	// Local query strategy; config is also persisted in graph snapshots and deltas.
	exactThreshold       int
	poison               error
	rebuildMu            sync.Mutex
	rebuildDelta         map[string]Record
	mu                   sync.RWMutex
	graph                *ann.Graph
	records              map[string]Record
	nodes                map[uint64]nodeVersion
	postings             payloadIndex
	dimension            int
	config               IndexConfig
	next                 uint64
	epoch                uint64
	lastReplacementEpoch uint64 // Protected by mu; data updates only advance epoch.
	live                 int
}

func NewHNSWIndex(d int, c IndexConfig) (*HNSWIndex, error) {
	if d < 1 || d > 4096 {
		return nil, errors.New("invalid dimension")
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	graph, err := ann.New(d, c.M, c.EFConstruction)
	if err != nil {
		return nil, err
	}
	return &HNSWIndex{graph: graph, records: map[string]Record{}, nodes: map[uint64]nodeVersion{}, postings: payloadIndex{}, dimension: d, config: c, exactThreshold: c.ExactThreshold}, nil
}
func (i *HNSWIndex) Apply(batch []Record) error {
	if err := validateBatch(batch, i.dimension); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.applyLocked(batch)
}
func (i *HNSWIndex) applyLocked(batch []Record) error {
	if i.poison != nil {
		return i.poison
	}
	// Coalesce a replay batch: only its highest new version needs a graph node.
	latest := map[string]Record{}
	for _, r := range batch {
		if r.Revision > i.records[r.ID].Revision && r.Revision > latest[r.ID].Revision {
			latest[r.ID] = r
		}
	}
	if uint64(len(latest)) > ^uint64(0)-i.next {
		return errors.New("node IDs exhausted")
	}
	ids := make([]string, 0, len(latest))
	for id := range latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := latest[id]
		old := i.records[id]
		if !r.Deleted {
			i.next++
			if err := i.graph.Add(i.next, float32Vector(r.Vector)); err != nil {
				return err
			}
			i.nodes[i.next] = nodeVersion{r.ID, r.Revision}
		}
		i.postings.remove(old)
		if old.Revision != 0 && !old.Deleted {
			i.live--
		}
		if !r.Deleted {
			i.live++
		}
		i.postings.add(r)
		i.records[id] = cloneRecord(r)
		if i.rebuildDelta != nil {
			i.rebuildDelta[id] = i.records[id]
		}
		i.epoch++
	}
	return nil
}
func (i *HNSWIndex) Snapshot() []Record {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return snapshotRecords(i.records)
}
func snapshotRecords(records map[string]Record) []Record {
	out := make([]Record, 0, len(records))
	for _, r := range records {
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

type QueryStats struct {
	Plan                 string `json:"plan"`
	DistanceComputations int    `json:"distance_computations"`
	Candidates           int    `json:"candidates"`
	Fallback             bool   `json:"fallback"`
}

func (i *HNSWIndex) Search(q Search) ([]Hit, error) {
	hits, _, err := i.SearchWithStats(q)
	return hits, err
}
func (i *HNSWIndex) SearchWithStats(q Search) ([]Hit, QueryStats, error) {
	if err := validateQuery(q, i.dimension); err != nil {
		return nil, QueryStats{}, err
	}
	if q.EFSearch < 0 || q.EFSearch > 4096 {
		return nil, QueryStats{}, errors.New("ef_search must be 0..4096")
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.searchWithStatsLocked(q, true)
}

func (i *HNSWIndex) searchWithStatsLocked(q Search, returnExactHits bool) ([]Hit, QueryStats, error) {
	stats := QueryStats{}
	if i.poison != nil {
		return nil, stats, i.poison
	}
	if q.MinVisible != nil && i.records[q.MinVisible.ID].Revision < q.MinVisible.Revision {
		return nil, stats, ErrNotVisible
	}
	allowed := i.postings.selectIDs(q.Filter)
	matching := i.live
	if allowed != nil {
		matching = len(allowed)
	}
	exact := func() []Hit {
		if !returnExactHits {
			return nil
		}
		best := make(topK, 0, q.K)
		score := func(r Record) {
			if r.Deleted {
				return
			}
			stats.DistanceComputations++
			best.offer(candidate{r.ID, r.Revision, l2(q.Vector, r.Vector)}, q.K)
		}
		if allowed != nil {
			for id := range allowed {
				score(i.records[id])
			}
		} else {
			for _, r := range i.records {
				score(r)
			}
		}
		return materialize(best, i.records)
	}
	if q.Exact || matching <= i.exactThreshold || matching <= q.K {
		stats.Plan = "exact"
		return exact(), stats, nil
	}
	stats.Plan = "hnsw"
	query := float32Vector(q.Vector)
	ef := i.config.EFSearch
	if q.EFSearch != 0 {
		ef = q.EFSearch
	}
	limit := min(i.config.MaxCandidates, len(i.nodes))
	budget := min(limit, max(q.K*4, ef))
	need := min(q.K, matching)
	for {
		candidates, computations, err := i.graph.Search(query, budget, ef)
		if err != nil {
			return nil, stats, err
		}
		stats.DistanceComputations += computations
		stats.Candidates += len(candidates)
		best := make(topK, 0, q.K)
		for _, node := range candidates {
			meta := i.nodes[node]
			r := i.records[meta.ID]
			if r.Deleted || r.Revision != meta.Revision {
				continue
			}
			if allowed != nil {
				if _, ok := allowed[r.ID]; !ok {
					continue
				}
			}
			stats.DistanceComputations++
			best.offer(candidate{r.ID, r.Revision, l2(q.Vector, r.Vector)}, q.K)
		}
		if len(best) >= need {
			return materialize(best, i.records), stats, nil
		}
		if budget >= limit {
			stats.Plan = "exact-fallback"
			stats.Fallback = true
			return exact(), stats, nil
		}
		budget = min(limit, budget*2)
	}
}
func (i *HNSWIndex) NeedsRebuild() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.nodes) >= 256 && (len(i.nodes)-i.live)*10 >= len(i.nodes)*3
}

type IndexStats struct {
	Live       int `json:"live"`
	GraphNodes int `json:"graph_nodes"`
	StaleNodes int `json:"stale_nodes"`
}

func (i *HNSWIndex) Stats() IndexStats {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return IndexStats{i.live, len(i.nodes), len(i.nodes) - i.live}
}

// Build offline, chase coalesced updates, and briefly fence writes for the final tail.
func (i *HNSWIndex) Rebuild(ctx context.Context) error {
	return i.rebuild(ctx, nil, nil)
}

// Reject quality observations superseded by a graph replacement. New data
// remains eligible and its installation epoch is returned under the index lock.
func (i *HNSWIndex) rebuild(ctx context.Context, epoch *uint64, canInstall func() bool) error {
	if !i.rebuildMu.TryLock() {
		return ErrConflict
	}
	defer i.rebuildMu.Unlock()
	i.mu.Lock()
	if epoch != nil && i.lastReplacementEpoch > *epoch {
		i.mu.Unlock()
		return ErrConflict
	}
	records := snapshotRecords(i.records)
	// IDs can be ordered by topic/time. A deterministic shuffle avoids building
	// long runs from one cluster and degrading cross-cluster graph connectivity.
	rand.New(rand.NewSource(42)).Shuffle(len(records), func(a, b int) { records[a], records[b] = records[b], records[a] })
	i.rebuildDelta = map[string]Record{}
	i.mu.Unlock()
	defer func() { i.mu.Lock(); i.rebuildDelta = nil; i.mu.Unlock() }()
	fresh, err := NewHNSWIndex(i.dimension, i.config)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			fresh.graph.Close()
		}
	}()
	apply := func(records []Record) error {
		for _, r := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fresh.Apply([]Record{r}); err != nil {
				return err
			}
		}
		return nil
	}
	if err = apply(records); err != nil {
		return err
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		i.mu.Lock()
		delta := snapshotRecords(i.rebuildDelta)
		i.rebuildDelta = map[string]Record{}
		if len(delta) > 256 {
			i.mu.Unlock()
			if err = apply(delta); err != nil {
				return err
			}
			continue
		}
		// At most 256 latest versions are applied while writes are fenced.
		if err = apply(delta); err != nil {
			i.mu.Unlock()
			return err
		}
		if err = ctx.Err(); err != nil {
			i.mu.Unlock()
			return err
		}
		if canInstall != nil && !canInstall() {
			i.mu.Unlock()
			return ErrFenced
		}
		i.replaceGraphLocked(fresh)
		if epoch != nil {
			*epoch = i.epoch
		}
		published = true
		i.mu.Unlock()
		return nil
	}
}
func (i *HNSWIndex) publishRebuild(expected uint64, fresh *HNSWIndex) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.epoch != expected {
		return ErrConflict
	}
	i.replaceGraphLocked(fresh)
	return nil
}
func (i *HNSWIndex) replaceGraphLocked(fresh *HNSWIndex) {
	oldGraph := i.graph
	i.graph = fresh.graph
	i.nodes = fresh.nodes
	i.postings = fresh.postings
	i.records = fresh.records
	i.next = fresh.next
	i.live = fresh.live
	i.epoch++
	i.lastReplacementEpoch = i.epoch
	oldGraph.Close()
}
