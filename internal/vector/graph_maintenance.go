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
	_, delay, err := j.nextCheckpoint(ctx, bytes, age)
	return delay == 0 && err == nil, err
}

// A negative delay means the checkpoint covers the entire journal.
func (j graphJournal) nextCheckpoint(ctx context.Context, bytes uint64, age time.Duration) (GraphHead, time.Duration, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return GraphHead{}, 0, err
	}
	defer tx.Rollback()
	h, err := j.headIn(ctx, tx)
	if err != nil {
		return h, 0, err
	}
	if h.Committed.Sequence == h.CheckpointPosition.Sequence {
		return h, -1, nil
	}
	if h.LogBytes < h.CheckpointBytes {
		return h, 0, errors.New("checkpoint byte watermark exceeds graph tail")
	}
	if h.LogBytes-h.CheckpointBytes >= bytes {
		return h, 0, nil
	}
	var first GraphEntry
	if err = readGraphEntry(ctx, tx, j.stream().Key(h.CheckpointPosition.Sequence+1), &first, false); err != nil {
		return h, 0, err
	}
	if first.CreatedMS <= 0 {
		return h, 0, errors.New("graph entry missing durable timestamp")
	}
	return h, max(time.Duration(0), age-time.Duration(txnMillis(tx)-first.CreatedMS)*time.Millisecond), nil
}

type graphMaintenanceSchedule struct {
	generation uint64
	checkpoint GraphPosition
	bytes      uint64
	deadline   time.Time
	gcAt       time.Time
}

func (m *graphMaintenanceSchedule) canWait(w *indexWriter, threshold uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	grant, live := w.leaderLease.Snapshot()
	if !live || !w.replicaLive() {
		return true
	}
	if !w.known || grant.Epoch != m.generation || !time.Now().Before(m.gcAt) {
		return false
	}
	if w.head.Committed == m.checkpoint {
		return true
	}
	return !m.deadline.IsZero() && time.Now().Before(m.deadline) && w.head.LogBytes >= m.bytes && w.head.LogBytes-m.bytes < threshold
}

func (c *ClusterService) runGraphMaintenance(ctx context.Context, entry *localPartition) {
	var schedule graphMaintenanceSchedule
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
			// A role hint only: checkpoint/GC still enforce ReplicaFence in
			// their changing transaction, including the current publisher.
			var plan ReplicaPlan
			var err error
			if g.writer != nil {
				if schedule.canWait(g.writer, c.checkpointBytes) {
					return
				}
				plan.Publisher = entry.owner.Partition
			} else if c.routeHintsEnabled() {
				var state *ClusterStatus
				state, err = c.cachedSearchRoutes(ctx, false)
				if err == nil {
					plan = state.ReplicaPlans[g.journal.id]
				}
			} else {
				plan, err = c.coordinator.Plan(ctx, g.journal.id)
			}
			if err != nil || plan.Publisher != entry.owner.Partition {
				return
			}
			work, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			err = c.maintainGraph(work, s, &schedule)
			if err != nil && ctx.Err() == nil && !errors.Is(err, ErrFenced) && !errors.Is(err, ErrConflict) {
				log.Printf("graph maintenance %s: %v", g.journal.id, err)
			}
		}()
	}
}
func (c *ClusterService) maintainGraph(ctx context.Context, s *Service, m *graphMaintenanceSchedule) (err error) {
	g := s.graphReplica
	var generation uint64
	if g.writer != nil && !g.writer.leaderLease.InLeaderLease(&generation) {
		return ErrFenced
	}
	h, delay, err := g.journal.nextCheckpoint(ctx, c.checkpointBytes, c.checkpointAge)
	if err != nil {
		return err
	}
	if delay == 0 {
		measure := c.observations.pipeline.track("graph_checkpoint", string(g.journal.id))
		_, err = g.checkpoint(ctx, s)
		measure(&err)
		if err != nil {
			return err
		}
	}
	m.generation, m.checkpoint, m.bytes = generation, h.CheckpointPosition, h.CheckpointBytes
	m.deadline = time.Time{}
	if delay > 0 {
		m.deadline = time.Now().Add(delay)
	}
	if delay != 0 && time.Now().Before(m.gcAt) {
		return nil
	}
	measure := c.observations.pipeline.track("graph_gc", string(g.journal.id))
	_, err = s.Prune(ctx)
	if err == nil {
		m.gcAt = time.Now().Add(10 * time.Second)
	}
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
	floors, err := c.retiredGraphSources(ctx, tx)
	if err != nil {
		return 0, err
	}
	source := fmt.Sprint(d)
	commits := make(map[IndexPartitionID]uint64, len(floors))
	var maximum uint64
	for id, retired := range floors {
		commits[id] = retired[source]
		maximum = max(maximum, commits[id])
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

func (c *ClusterService) retiredGraphSources(ctx context.Context, tx transactionAPI) (map[IndexPartitionID]map[string]uint64, error) {
	// FloorSources lives in the control anchor and changes only with graph GC.
	// Reading immutable tails here is both unnecessary and expensive: it would
	// multiply reverse scans by data partitions * index partitions every tick.
	keys := make([][]byte, len(c.layout.Bindings))
	for n, b := range c.layout.Bindings {
		keys[n] = (graphJournal{c: c.coordinator, id: b.Index}).key("head")
	}
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	sources := make(map[IndexPartitionID]map[string]uint64, len(keys))
	for n, b := range c.layout.Bindings {
		var h GraphHead
		if raw, ok := values[string(keys[n])]; ok {
			if err = json.Unmarshal(raw, &h); err != nil {
				return nil, err
			}
		}
		if h.Floor.Sequence > h.CheckpointPosition.Sequence {
			return nil, errors.New("graph floor exceeds recovery checkpoint")
		}
		sources[b.Index] = h.FloorSources
	}
	return sources, nil
}

// Each worker preflights all its owned sources at a single MVCC snapshot.
// A zero retired watermark or an already reclaimed prefix needs no per-source
// transaction. Positive candidates still pass every check inside maintainData.
func (c *ClusterService) dataMaintenanceCandidates(ctx context.Context, owners map[DataPartitionID]Ownership) (map[DataPartitionID]Ownership, error) {
	if len(owners) == 0 {
		return nil, nil
	}
	tx, err := c.root.begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	floors, err := c.retiredGraphSources(ctx, tx)
	if err != nil {
		return nil, err
	}
	var keys [][]byte
	limits := make(map[DataPartitionID]uint64, len(owners))
	for d := range owners {
		source := fmt.Sprint(d)
		var highest uint64
		for _, retired := range floors {
			highest = max(highest, retired[source])
		}
		if highest == 0 {
			delete(owners, d)
			continue
		}
		limits[d] = highest
		keys = append(keys, []byte(c.data[d].Prefix+"log-floor"), completedKey(c.data[d]))
	}
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	cached := prefetchedTransaction{transactionAPI: tx, values: values}
	for d, highest := range limits {
		floor, floorErr := c.data[d].readCounter(ctx, cached, "log-floor")
		done, doneErr := c.completedIn(ctx, cached, d)
		// Leave malformed counters to the owning source's maintenance path
		// so it reports the error independently of healthy local sources.
		if floorErr == nil && doneErr == nil && min(highest, done) <= floor {
			delete(owners, d)
		}
	}
	return owners, ctx.Err()
}

// Start maintenance for live local writers. Publishing and pruning still
// fence their mutations at commit, without holding writerMu during I/O.
func (c *ClusterService) localDataOwners() map[DataPartitionID]Ownership {
	c.writerMu.RLock()
	defer c.writerMu.RUnlock()
	owners := make(map[DataPartitionID]Ownership, len(c.writers))
	for id, group := range c.writers {
		if group.lease.InLeaderLease(nil) {
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
		measure := c.observations.pipeline.track("data_gc_preflight", "")
		owners, err := c.dataMaintenanceCandidates(ctx, c.localDataOwners())
		measure(&err)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("data maintenance preflight: %v", err)
			}
			continue
		}
		for d, owner := range owners {
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
