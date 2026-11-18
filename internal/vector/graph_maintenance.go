package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const DefaultGraphCheckpointBytes uint64 = 30_000_000
const DefaultGraphCheckpointAge = 3 * time.Minute

// The first uncovered entry supplies the clock across restarts and failover.
// A quiet graph has no deadline and never creates repeated empty checkpoints.
func (j graphJournal) checkpointDue(ctx context.Context, bytes uint64, age time.Duration) (bool, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	h, err := j.headIn(ctx, tx)
	if err != nil {
		return false, err
	}
	if h.Committed.Sequence == h.CheckpointPosition.Sequence {
		return false, nil
	}
	if h.LogBytes < h.CheckpointBytes {
		return false, errors.New("checkpoint byte watermark exceeds graph tail")
	}
	if h.LogBytes-h.CheckpointBytes >= bytes {
		return true, nil
	}
	var first GraphEntry
	if err = readJSON(ctx, tx, j.key(fmt.Sprintf("entries/%020d", h.CheckpointPosition.Sequence+1)), &first); err != nil {
		return false, err
	}
	if first.CreatedMS <= 0 {
		return false, errors.New("graph entry missing durable timestamp")
	}
	return txnMillis(tx)-first.CreatedMS >= age.Milliseconds(), nil
}

func (c *ClusterService) runGraphMaintenance(ctx context.Context, entry *localPartition) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		func() {
			entry.mu.RLock()
			defer entry.mu.RUnlock()
			s := entry.service
			if s == nil || s.graphReplica == nil {
				return
			}
			g := s.graphReplica
			plan, err := c.coordinator.Plan(ctx, g.journal.id)
			if err != nil || plan.Publisher != entry.owner.Partition {
				return
			}
			work, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			err = c.maintainGraph(work, s)
			if err != nil && ctx.Err() == nil && !errors.Is(err, ErrFenced) && !errors.Is(err, ErrConflict) {
				log.Printf("graph maintenance %s: %v", g.journal.id, err)
			}
		}()
	}
}
func (c *ClusterService) maintainGraph(ctx context.Context, s *Service) (err error) {
	g := s.graphReplica
	due, err := g.journal.checkpointDue(ctx, c.checkpointBytes, c.checkpointAge)
	if err != nil {
		return err
	}
	if due {
		measure := c.observations.pipeline.track("graph_checkpoint", string(g.journal.id))
		_, err = g.checkpoint(ctx, s)
		measure(&err)
		if err != nil {
			return err
		}
	}
	measure := c.observations.pipeline.track("graph_gc", string(g.journal.id))
	_, err = s.Prune(ctx)
	measure(&err)
	return err
}

// A source can delete only the contiguous prefix whose actual target indexes
// have retired the referencing entries. Live commit ACKs alone are insufficient.
func (c *ClusterService) retainGraphReferences(ctx context.Context, tx transactionAPI, d DataPartitionID, end uint64) (uint64, error) {
	if !c.coordinator.referenceLogs {
		return end, nil
	}
	r := c.data[d]
	floor, err := r.readCounter(ctx, tx, "log-floor")
	if err != nil {
		return 0, err
	}
	if end <= floor {
		return floor, nil
	}
	commits := map[IndexPartitionID]uint64{}
	// FloorSources lives in the control anchor and changes only with graph GC.
	// Reading immutable tails here is both unnecessary and expensive: it would
	// multiply reverse scans by data partitions * index partitions every tick.
	keys := make([][]byte, len(c.layout.Bindings))
	for n, b := range c.layout.Bindings {
		keys[n] = (graphJournal{c: c.coordinator, id: b.Index}).key("head")
	}
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return 0, err
	}
	var maximum uint64
	for n, b := range c.layout.Bindings {
		var h GraphHead
		if raw, ok := values[string(keys[n])]; ok {
			if err = json.Unmarshal(raw, &h); err != nil {
				return 0, err
			}
		}
		if h.Floor.Sequence > h.CheckpointPosition.Sequence {
			return 0, errors.New("graph floor exceeds recovery checkpoint")
		}
		commits[b.Index] = h.FloorSources[fmt.Sprint(d)]
		maximum = max(maximum, commits[b.Index])
	}
	if maximum <= floor {
		return floor, nil
	}
	batch, err := r.readEventsIn(ctx, tx, floor, min(end, floor+LogBatchSize), LogBatchSize, 0)
	if err != nil {
		return 0, err
	}
	return completedPrefix(batch.Events, floor, c.eventTargets, commits), nil
}

// A local writer is a candidate for maintenance, never proof of ownership.
// Copy the small set so no writer lock spans a KV lookup, upload or GC.
func (c *ClusterService) localDataOwners() map[DataPartitionID]Ownership {
	c.writerMu.RLock()
	defer c.writerMu.RUnlock()
	owners := make(map[DataPartitionID]Ownership, len(c.writers))
	for id, group := range c.writers {
		if !group.lease.uncertain.Load() {
			owners[id] = group.lease.owner
		}
	}
	return owners
}
func (c *ClusterService) dataMaintenanceLoop(ctx context.Context) {
	defer close(c.maintenanceDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for d, owner := range c.localDataOwners() {
			if err := c.coordinator.Check(ctx, owner); err != nil {
				continue
			}
			work, cancel := context.WithTimeout(ctx, 2*time.Minute)
			measure := c.observations.pipeline.track("data_checkpoint_gc", string(sourceID(d)))
			err := c.maintainData(work, d, owner)
			measure(&err)
			cancel()
			if err != nil && ctx.Err() == nil && !errors.Is(err, ErrFenced) && !errors.Is(err, ErrConflict) {
				log.Printf("data maintenance %s: %v", sourceID(d), err)
			}
		}
	}
}
func (c *ClusterService) maintainData(ctx context.Context, d DataPartitionID, owner Ownership) error {
	r := *c.data[d]
	r.Fence = c.coordinator.Fence(owner)
	r.Retain = func(ctx context.Context, tx transactionAPI, end uint64) (uint64, error) {
		done, err := c.completedIn(ctx, tx, d)
		if err != nil {
			return 0, err
		}
		return c.retainGraphReferences(ctx, tx, d, min(end, done))
	}
	tx, err := r.begin()
	if err != nil {
		return err
	}
	floor, err := r.readCounter(ctx, tx, "log-floor")
	var safe uint64
	if err == nil {
		safe, err = r.Retain(ctx, tx, ^uint64(0))
	}
	_ = tx.Rollback()
	if err != nil || safe <= floor {
		return err
	}
	old, err := r.Manifest(ctx)
	if err != nil {
		return err
	}
	if old == nil || old.Offset < safe {
		expected := ""
		if old != nil {
			expected = old.Generation
		}
		rows, offset, err := r.Bootstrap(ctx)
		if err != nil {
			return err
		}
		m, err := c.objects.Save(ctx, r.Namespace(), r.Dimension, rows)
		if err != nil {
			return err
		}
		m.Format = "data-records-v1"
		m.Offset = offset
		m.LogVersion = LogVersion
		if err = r.Publish(ctx, expected, m); err != nil {
			return err
		}
	}
	_, err = r.PruneEvents(ctx, LogBatchSize)
	return err
}
