package vector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type localPartition struct {
	cancel          context.CancelFunc
	loadDone        chan struct{}
	lastGraphReport atomic.Pointer[replicaGraphReport]
	queryNanos      atomic.Int64
	retired         bool
	owner           Ownership
	mu              sync.RWMutex
	service         *Service
}

func (c *ClusterService) entries() []*localPartition {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []*localPartition{}
	for _, entry := range c.locals {
		out = append(out, entry)
	}
	return out
}
func (c *ClusterService) remove(entry *localPartition) {
	c.mu.Lock()
	if c.locals[entry.owner.Partition] != entry {
		c.mu.Unlock()
		return
	}
	delete(c.locals, entry.owner.Partition)
	c.retirementWG.Add(1)
	c.mu.Unlock()
	if entry.cancel != nil {
		entry.cancel()
	}
	entry.mu.RLock()
	if entry.service != nil && entry.service.graphReplica != nil && entry.service.graphReplica.writer != nil {
		entry.service.graphReplica.writer.retire()
		g := entry.service.graphReplica
		g.viewMu.RLock()
		q := g.followerReads
		g.viewMu.RUnlock()
		if q != nil {
			q.close()
		}
	}
	entry.mu.RUnlock()
	// Retire outside the heartbeat loop. Every user of the local graph holds
	// entry.mu.RLock, so native memory is freed only after in-flight operations end.
	go func() {
		defer c.retirementWG.Done()
		entry.mu.Lock()
		entry.retired = true
		if entry.service != nil {
			entry.service.Index.mu.Lock()
			entry.service.Index.graph.Close()
			entry.service.Index.mu.Unlock()
			entry.service = nil
		}
		entry.mu.Unlock()
		if entry.loadDone != nil {
			<-entry.loadDone
		}
		c.acknowledgeRetirement(entry.owner)
	}()
}
func (c *ClusterService) load(ctx context.Context, b PartitionBinding, entry *localPartition) {
	var repo Repository
	if c.layout.Routing != nil {
		repo = *c.indexRepository(b.Index)
	} else {
		repo = *c.data[b.Data]
	}
	if c.layout.DataPartitions > 1 {
		repo.IndexPrefix = fmt.Sprintf("%sindex/%s/", c.root.Prefix, b.Index)
	}
	repo.Fence = c.coordinator.Fence(entry.owner)
	if c.replicas > 1 {
		repo.Fence = c.coordinator.ReplicaFence(b.Index, entry.owner)
		repo.Retain = c.coordinator.RetainReplicas(b.Index)
	}
	var plan ReplicaPlan
	if c.replicas > 1 {
		plan, _ = c.coordinator.Plan(ctx, b.Index)
		_ = c.coordinator.ReportReplica(ctx, b.Index, plan.Version, entry.owner, "Restoring", "", 0)
	}
	var restoreOffset uint64
	var service *Service
	var e error
	if c.graphReplication {
		service, e = newGraphService(ctx, &repo, c.objects, graphJournal{c.coordinator, b.Index, entry.owner})
		if e == nil {
			service.graphReplica.retention = c.consumerRetention || c.coordinator.referenceLogs
			if c.layout.Routing != nil {
				service.graphReplica.input = newRoutedInput(c)
				if c.coordinator.leaseReads {
					service.graphReplica.writer = &indexWriter{}
				}
			}
		}
	} else {
		service, e = newServiceWithProgress(ctx, &repo, c.objects, func(offset uint64) {
			restoreOffset = offset
			if c.replicas > 1 {
				_ = c.coordinator.ReportReplica(ctx, b.Index, plan.Version, entry.owner, "CatchingUp", "", offset)
			}
		})
	}
	if e != nil {
		log.Printf("partition %s restore failed: %v", b.Index, e)
		if c.replicas > 1 {
			_ = c.coordinator.ReportReplica(ctx, b.Index, plan.Version, entry.owner, "Error", e.Error(), restoreOffset)
		}
		_, _ = c.coordinator.Update(ctx, entry.owner, false, true)
		c.remove(entry)
		return
	}
	entry.mu.Lock()
	if entry.retired {
		entry.mu.Unlock()
		service.Index.graph.Close()
		return
	}
	entry.service = service
	entry.mu.Unlock()
	started := time.Now()
	if _, e = c.coordinator.Update(ctx, entry.owner, true, false); e != nil && errors.Is(e, ErrFenced) {
		c.remove(entry)
	} else if e == nil && service.graphReplica != nil && service.graphReplica.writer != nil {
		if current, err := c.coordinator.Plan(ctx, b.Index); err == nil {
			_ = service.graphReplica.refreshLease(ctx, current, started)
		}
	}
}
func (c *ClusterService) SyncOwned(ctx context.Context) (err error) {
	c.observations.pipeline.syncStarted(time.Now())
	defer c.observations.pipeline.track("worker_sync", "")(&err)
	for _, entry := range c.entries() {
		if e := c.coordinator.Check(ctx, entry.owner); e != nil {
			continue
		}
		entry.mu.RLock()
		s := entry.service
		var err error
		if s != nil {
			err = s.Sync(ctx)
		}
		entry.mu.RUnlock()
		if err != nil {
			if errors.Is(err, ErrGraphCorrupt) {
				_, _ = c.coordinator.Update(ctx, entry.owner, false, true)
				c.remove(entry)
			}
			return err
		}
	}
	return nil
}
func (c *ClusterService) MaybeRebuild(ctx context.Context) error {
	for _, entry := range c.entries() {
		if e := c.coordinator.Check(ctx, entry.owner); e != nil {
			continue
		}
		entry.mu.RLock()
		s := entry.service
		var err error
		if s != nil {
			err = s.MaybeRebuild(ctx)
		}
		entry.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

type replicaGraphReport struct {
	version  uint64
	position GraphPosition
}

// The owner renewal already returned its durable phase and offset. Report the
// graph position and replica status together only when the last confirmed
// report or the renewed status differs. A new incarnation has an empty cache.
func (entry *localPartition) reportGraphProgress(ctx context.Context, j graphJournal, version uint64, renewed Ownership, pos GraphPosition, p *pipelineMetrics) (err error) {
	now := replicaGraphReport{version: version, position: pos}
	if last := entry.lastGraphReport.Load(); last != nil && *last == now && renewed.Phase == "Ready" && renewed.LastError == "" && renewed.Offset == pos.DataOffset {
		return nil
	}
	defer p.track("graph_progress_report", string(j.id))(&err)
	tx, err := j.c.root.begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = j.c.reportReplicaIn(ctx, tx, j.id, version, j.owner, "Ready", "", pos.DataOffset); err != nil {
		return err
	}
	if err = putJSON(tx, j.key("progress/"+string(j.owner.Partition)), graphAck{j.owner, pos}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	entry.lastGraphReport.Store(&now)
	return nil
}

func (c *ClusterService) replicaTick(ctx context.Context) {
	allowed := map[IndexPartitionID]PartitionBinding{}
	versions := map[IndexPartitionID]uint64{}
	publishers := map[IndexPartitionID]IndexPartitionID{}
	plans := map[IndexPartitionID]ReplicaPlan{}
	for _, b := range c.layout.Bindings {
		p, e := c.coordinator.Plan(ctx, b.Index)
		if e != nil {
			continue
		}
		for _, a := range p.Serving {
			if a.Node == c.node.ID && a.Session == c.node.Session {
				allowed[a.Partition] = b
				versions[a.Partition] = p.Version
				publishers[a.Partition] = p.Publisher
				plans[a.Partition] = p
			}
		}
	}
	for _, entry := range c.entries() {
		if _, ok := allowed[entry.owner.Partition]; !ok {
			_, e := c.coordinator.Update(ctx, entry.owner, false, true)
			if e == nil || errors.Is(e, ErrFenced) {
				c.remove(entry)
			}
			continue
		}
		entry.mu.RLock()
		ready := entry.service != nil
		var offset uint64
		var graphPos GraphPosition
		var journal *graphJournal
		if ready {
			if g := entry.service.graphReplica; g != nil {
				if g.input != nil {
					g.input.planChanged(versions[entry.owner.Partition], publishers[entry.owner.Partition])
				}
				g.viewMu.RLock()
				graphPos = g.position
				g.viewMu.RUnlock()
				offset = graphPos.DataOffset
				journal = &g.journal
			} else {
				entry.service.syncMu.Lock()
				offset = entry.service.cursor
				entry.service.syncMu.Unlock()
			}
		}
		entry.mu.RUnlock()
		started := time.Now()
		if renewed, e := c.coordinator.Update(ctx, entry.owner, ready, false); errors.Is(e, ErrFenced) {
			c.remove(entry)
		} else if e == nil && ready {
			entry.mu.RLock()
			if entry.service != nil && entry.service.graphReplica != nil {
				_ = entry.service.graphReplica.refreshLease(ctx, plans[entry.owner.Partition], started)
			}
			entry.mu.RUnlock()
			id, version := allowed[entry.owner.Partition].Index, versions[entry.owner.Partition]
			if journal == nil {
				_ = c.coordinator.ReportReplica(ctx, id, version, entry.owner, "Ready", "", offset)
			} else {
				_ = entry.reportGraphProgress(ctx, *journal, version, renewed, graphPos, &c.observations.pipeline)
			}
		}
	}
	for id, b := range allowed {
		c.mu.Lock()
		_, exists := c.locals[id]
		c.mu.Unlock()
		if exists {
			continue
		}
		o, e := c.coordinator.Acquire(ctx, id, c.node)
		if e != nil {
			continue
		}
		c.startLoad(ctx, b, o)
	}
}
