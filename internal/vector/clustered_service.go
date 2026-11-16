package vector

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (c *ClusterService) Mutate(ctx context.Context, id string, m Mutation, deleted bool) (_ Record, err error) {
	partition := c.router.DataForID(id)
	defer c.observations.pipeline.track("data_mutate", string(sourceID(partition)))(&err)
	record, err := c.PartitionedService.Mutate(ctx, id, m, deleted)
	if err == nil {
		c.signalSource(partition, "write", false)
	}
	return record, err
}

func (c *ClusterService) Sync(ctx context.Context) error {
	if c.layout.Routing == nil {
		return c.PartitionedService.Sync(ctx)
	}
	// Capture a finite data frontier; ongoing writes cannot extend this wait.
	targets := map[DataPartitionID]uint64{}
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	for d, r := range c.data {
		targets[d], e = r.readCounter(ctx, tx, "log-head")
		if e != nil {
			_ = tx.Rollback()
			return e
		}
	}
	_ = tx.Rollback()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		tx, e = c.root.begin()
		if e != nil {
			return e
		}
		ready := true
		for d, target := range targets {
			n, err := c.completedIn(ctx, tx, d)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			if n < target {
				ready = false
			}
		}
		_ = tx.Rollback()
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	// All input effects are durable. Publish pending batches on each current leader.
	return c.PartitionedService.Sync(ctx)
}
func (c *ClusterService) Search(q Search) ([]Hit, error) {
	return c.SearchContext(context.Background(), q)
}
func (c *ClusterService) SearchContext(ctx context.Context, q Search) ([]Hit, error) {
	if c.layout.Routing == nil {
		return c.PartitionedService.SearchContext(ctx, q)
	}
	if e := validateQuery(q, c.dimension); e != nil {
		return nil, e
	}
	if q.MinVisible != nil {
		r, e := c.Get(ctx, q.MinVisible.ID)
		if e != nil {
			return nil, e
		}
		if r.Revision < q.MinVisible.Revision {
			return nil, ErrNotVisible
		}
		if e = c.Sync(ctx); e != nil {
			return nil, e
		}
		if r.Deleted {
			q.MinVisible = nil
		} else {
			q.MinVisible = &Token{ID: r.ID, Revision: r.Revision}
		}
	}
	return c.PartitionedService.SearchContext(ctx, q)
}
func (c *ClusterService) Checkpoint(ctx context.Context) (Manifest, error) {
	if c.layout.Routing == nil {
		return c.PartitionedService.Checkpoint(ctx)
	}
	if e := c.Sync(ctx); e != nil {
		return Manifest{}, e
	}
	result, e := c.PartitionedService.Checkpoint(ctx)
	if e != nil {
		return result, e
	}
	if result.Partitions == nil {
		single := result
		result = Manifest{Format: "partition-set-v1", Generation: single.Generation, Dimension: c.dimension, Partitions: map[string]Manifest{string(c.layout.Bindings[0].Index): single}}
	}
	for d, r := range c.data {
		old, err := r.Manifest(ctx)
		if err != nil {
			return result, err
		}
		expected := ""
		if old != nil {
			expected = old.Generation
		}
		rows, offset, err := r.Bootstrap(ctx)
		if err != nil {
			return result, err
		}
		m, err := c.objects.Save(ctx, r.Namespace(), r.Dimension, rows)
		if err != nil {
			return result, err
		}
		m.Format = "data-records-v1"
		m.Offset = offset
		m.LogVersion = LogVersion
		if err = r.Publish(ctx, expected, m); err != nil {
			return result, err
		}
		if result.Partitions == nil {
			result.Partitions = map[string]Manifest{}
		}
		result.Partitions[string(sourceID(d))] = m
	}
	return result, nil
}
func (c *ClusterService) PruneState(ctx context.Context) (any, error) {
	if c.layout.Routing == nil {
		return c.PartitionedService.PruneState(ctx)
	}
	_, e := c.PartitionedService.PruneState(ctx)
	if e != nil {
		return nil, e
	}
	floors := map[string]uint64{}
	for d, r := range c.data {
		owner, err := c.coordinator.Resolve(ctx, sourceID(d))
		if err != nil {
			return nil, err
		}
		repo := *r
		repo.Fence = c.coordinator.Fence(owner)
		repo.Retain = func(ctx context.Context, tx transactionAPI, end uint64) (uint64, error) {
			n, e := c.completedIn(ctx, tx, d)
			return min(end, n), e
		}
		n, err := repo.PruneEvents(ctx, LogBatchSize)
		if err != nil {
			return nil, err
		}
		floors[string(sourceID(d))] = n
	}
	return map[string]any{"partitions": floors}, nil
}

func (s *Service) searchRouted(ctx context.Context, q Search) ([]Hit, GraphPosition, error) {
	g := s.graphReplica
	c := g.input.cluster
	want := q.K
	// Filter stale/misplaced candidates against one authoritative KV snapshot per
	// attempt. Expand candidates when obsolete entries occupy the local top K.
	for {
		g.viewMu.RLock()
		hits, e := s.Index.Search(q)
		pos := g.position
		g.viewMu.RUnlock()
		if e != nil {
			return nil, pos, e
		}
		tx, e := c.root.begin()
		if e != nil {
			return nil, pos, e
		}
		result := make([]Hit, 0, want)
		for _, hit := range hits {
			repo := c.data[c.router.DataForID(hit.ID)]
			r, err := readRecord(ctx, tx, repo.key("record", hit.ID))
			if errors.Is(err, ErrNotFound) {
				c.observations.staleCandidates.Add(1)
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				return nil, pos, err
			}
			if !r.Deleted && r.Revision == hit.Revision && c.layout.Routing.target(r.Vector) == g.journal.id {
				result = append(result, hit)
				if len(result) == want {
					break
				}
			} else {
				c.observations.staleCandidates.Add(1)
			}
		}
		_ = tx.Rollback()
		if q.K == 1000 && len(result) < want && len(hits) == q.K {
			c.observations.exactFallbacks.Add(1)
			return s.searchCurrentExact(ctx, q, want)
		}
		if len(result) >= want || len(hits) < q.K {
			return result, pos, nil
		}
		q.K = min(1000, q.K*2)
	}
}

func (p *PartitionedService) clusteredSource(q ChangeRequest) (*Repository, error) {
	if q.Stream == "data" {
		for d, r := range p.data {
			if q.Partition == sourceID(d) {
				return r, nil
			}
		}
	} else if q.Stream == "graph" {
		for _, b := range p.layout.Bindings {
			if b.Index == q.Partition {
				r := *p.data[0]
				r.Prefix = r.ReceiptPrefix + "index/" + string(b.Index) + "/"
				r.IndexPrefix = r.Prefix
				return &r, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: use data-NNNN for data and index-NNNN for graph", ErrInvalidSubscription)
}

// A bounded ANN budget must not turn stale entries into a silently truncated
// exact result. This uncommon fallback scans the local view with authoritative
// revision checks; cleanup normally removes the need for it.
func (s *Service) searchCurrentExact(ctx context.Context, q Search, want int) ([]Hit, GraphPosition, error) {
	g := s.graphReplica
	c := g.input.cluster
	tx, e := c.root.begin()
	if e != nil {
		return nil, GraphPosition{}, e
	}
	defer tx.Rollback()
	g.viewMu.RLock()
	defer g.viewMu.RUnlock()
	s.Index.mu.RLock()
	defer s.Index.mu.RUnlock()
	best := make(topK, 0, want)
	allowed := s.Index.postings.selectIDs(q.Filter)
	for id, r := range s.Index.records {
		if e = ctx.Err(); e != nil {
			return nil, g.position, e
		}
		if r.Deleted {
			continue
		}
		if allowed != nil {
			if _, ok := allowed[id]; !ok {
				continue
			}
		}
		repo := c.data[c.router.DataForID(id)]
		current, err := readRecord(ctx, tx, repo.key("record", id))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, g.position, err
		}
		if current.Deleted || current.Revision != r.Revision || c.layout.Routing.target(current.Vector) != g.journal.id {
			continue
		}
		best.offer(candidate{r.ID, r.Revision, l2(q.Vector, r.Vector)}, want)
	}
	return materialize(best, s.Index.records), g.position, nil
}
