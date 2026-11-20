package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (c *ClusterService) Get(ctx context.Context, id string) (Record, error) {
	d := c.router.DataForID(id)
	repo := c.data[d]
	if !repo.usesRawShardData() {
		return repo.Get(ctx, id)
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if group, ok := repo.raw.local.Load(repo.Prefix); ok {
		r, err := group.(*mutationGroup).readOwnedRecord(id)
		if !errors.Is(err, ErrFenced) {
			return r, err
		}
	}
	if c.routeHintsEnabled() {
		if state, err := c.cachedSearchRoutes(ctx, false); err == nil {
			o := state.Owners[sourceID(d)]
			if o.Epoch != 0 {
				reply, err := c.sendPartition(ctx, o, partitionRPC{Namespace: c.root.Prefix, Partition: o.Partition, Epoch: o.Epoch, Session: o.Session, Source: &d, Operation: "get_owned", ID: id})
				if err == nil {
					if reply.Record == nil {
						return Record{}, ErrNotFound
					}
					return *reply.Record, nil
				}
				if errors.Is(err, ErrFenced) {
					c.expireSearchRoutes(state)
				}
			}
		}
	}
	return repo.Get(ctx, id)
}

func (c *ClusterService) Mutate(ctx context.Context, id string, m Mutation, deleted bool) (_ Record, err error) {
	partition := c.router.DataForID(id)
	defer c.observations.pipeline.track("data_mutate", string(sourceID(partition)))(&err)
	if c.layout.Routing != nil {
		return c.routeMutation(ctx, partition, id, m, deleted)
	}
	if group := c.mutationGroups[partition]; group != nil {
		return group.mutate(ctx, id, m, deleted)
	}
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
	tx, e := c.data[0].beginDataRead(ctx)
	if e != nil {
		return e
	}
	for d, r := range c.data {
		targets[d], e = r.logHead(ctx, tx)
		if e != nil {
			_ = tx.Rollback()
			return e
		}
	}
	_ = tx.Rollback()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for len(targets) > 0 {
		c.writerMu.RLock()
		var keys [][]byte
		for id, n := range targets {
			group := c.writers[id]
			if n == 0 || group != nil && group.lease.InLeaderLease(nil) && group.lease.indexed.Load() >= n {
				delete(targets, id)
				continue
			}
			keys = append(keys, completedKey(c.data[id]))
		}
		c.writerMu.RUnlock()
		if len(keys) == 0 {
			break
		}
		tx, err := c.root.begin()
		if err != nil {
			return err
		}
		values, err := batchGet(ctx, tx, keys)
		_ = tx.Rollback()
		if err != nil {
			return err
		}
		for id, n := range targets {
			var completed uint64
			if value, ok := values[string(completedKey(c.data[id]))]; ok {
				if err := json.Unmarshal(value, &completed); err != nil {
					return err
				}
			}
			if completed >= n {
				delete(targets, id)
			}
		}
		if len(targets) == 0 {
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
		// Routed graph replicas enforce the token on their applied view.
		if !c.graphReplication {
			if e = c.Sync(ctx); e != nil {
				return nil, e
			}
		}
		if r.Deleted {
			// Deletes are broadcast. Every selected replica must have applied
			// the tombstone; clearing the token could expose a lagging copy.
			q.MinVisible = &Token{ID: r.ID, Revision: r.Revision}
			q.visibilityAll = true
		} else {
			q.MinVisible = &Token{ID: r.ID, Revision: r.Revision}
			q.visibilityIndex, e = c.router.IndexForRecord(c.router.DataForID(r.ID), r)
			if e != nil {
				return nil, e
			}
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
		err = retryIndexCommit(ctx, func() error { return r.Publish(ctx, expected, m) }, func() {})
		if errors.Is(err, ErrConflict) {
			// Automatic source maintenance may publish a newer full data image
			// while this manual checkpoint uploads. Reuse that proven coverage.
			current, readErr := r.Manifest(ctx)
			if readErr == nil && current != nil && current.LogVersion == m.LogVersion && current.Format == m.Format && current.Offset >= m.Offset {
				m = *current
				err = nil
			}
		}
		if err != nil {
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
	for d := range c.data {
		id := sourceID(d)
		// A cached search route can predate a completed data handoff.
		owner, err := c.coordinator.Resolve(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("%w: prune %s: %w", ErrPartitionUnavailable, id, err)
		}
		reply, err := c.sendPartition(ctx, owner, partitionRPC{Namespace: c.root.Prefix, Partition: id, Session: owner.Session, Epoch: owner.Epoch, Source: &d, Operation: "prune_data"})
		if err != nil {
			return nil, fmt.Errorf("prune %s: %w", id, err)
		}
		floors[string(id)] = reply.Floor
	}
	return map[string]any{"partitions": floors}, nil
}

func (c *ClusterService) pruneData(ctx context.Context, d DataPartitionID, owner Ownership) (floor uint64, err error) {
	repo := *c.data[d]
	repo.Fence = c.coordinator.Fence(owner)
	repo.Retain = func(ctx context.Context, tx transactionAPI, end uint64) (uint64, error) {
		done, err := c.completedIn(ctx, tx, d)
		if err != nil {
			return 0, err
		}
		return c.retainGraphReferences(ctx, tx, d, min(end, done))
	}
	err = retryIndexCommit(ctx, func() error {
		floor, err = repo.PruneEvents(ctx, LogBatchSize)
		return err
	}, func() {})
	return floor, err
}

// Search reads the replica's applied in-memory view. No authoritative record
// reads: updates, moves and deletes may remain visible until this replica applies.
func (s *Service) searchRouted(ctx context.Context, q Search) ([]Hit, GraphPosition, error) {
	if err := ctx.Err(); err != nil {
		return nil, GraphPosition{}, err
	}
	g := s.graphReplica
	g.viewMu.RLock()
	defer g.viewMu.RUnlock()
	hits, err := s.Index.Search(q)
	return hits, g.position, err
}

// Wait on the replica that must actually serve the token. No partition or
// index lock is held while waiting for its graph consumer to apply a revision.
func (c *ClusterService) searchVisibleReplica(ctx context.Context, entry *localPartition, q Search) (partitionReply, error) {
	for ctx.Err() == nil {
		entry.mu.RLock()
		s := entry.service
		if entry.retired || s == nil || s.graphReplica == nil || s.graphReplica.input == nil {
			entry.mu.RUnlock()
			return partitionReply{}, ErrFenced
		}
		g := s.graphReplica
		started := time.Now()
		hits, pos, err := s.searchRouted(ctx, q)
		entry.queryNanos.Add(time.Since(started).Nanoseconds())
		if !errors.Is(err, ErrNotVisible) {
			entry.mu.RUnlock()
			return partitionReply{Hits: hits, GraphPosition: &pos, AppliedOffset: pos.DataOffset}, err
		}
		g.viewMu.Lock()
		if g.position != pos {
			g.viewMu.Unlock()
			entry.mu.RUnlock()
			continue
		}
		if g.appliedChange == nil {
			g.appliedChange = make(chan struct{})
		}
		change := g.appliedChange
		g.viewMu.Unlock()
		g.input.notify()
		entry.mu.RUnlock()
		select {
		case <-ctx.Done():
			return partitionReply{}, ctx.Err()
		case <-change:
		}
	}
	return partitionReply{}, ctx.Err()
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
				r.Prefix = r.collectionPrefix + "index/" + string(b.Index) + "/"
				r.IndexPrefix = r.Prefix
				return &r, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: use data-NNNN for data and index-NNNN for graph", ErrInvalidSubscription)
}

// Routed SearchContext owns the one finite visibility barrier.
func (c *ClusterService) HandlesMinVisible() bool { return c.layout.Routing != nil }
