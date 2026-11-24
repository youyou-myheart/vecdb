package vector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"example.com/vector-storage/internal/lease"
	"golang.org/x/sync/errgroup"
)

type localPartition struct {
	maintenanceTerm atomic.Uint64
	readLease       lease.Handle
	cancel          context.CancelFunc
	loadDone        chan struct{}
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
	entry.maintenanceTerm.Store(0)
	entry.retireReads()
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
			if g := entry.service.graphReplica; g != nil {
				g.viewMu.Lock()
				if g.appliedChange != nil {
					close(g.appliedChange)
					g.appliedChange = nil
				}
				g.viewMu.Unlock()
			}
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
		service, e = newGraphServiceWithPolicy(ctx, &repo, c.objects, graphJournal{c.coordinator, b.Index, entry.owner}, c.indexPolicy)
		if e == nil {
			service.graphReplica.retention = c.consumerRetention || c.coordinator.referenceLogs
			if c.layout.Routing != nil {
				service.graphReplica.input = newRoutedInput(c)
				if c.coordinator.leaseReads {
					service.graphReplica.writer = &indexWriter{replicaLease: &entry.readLease}
				}
			}
		}
	} else {
		service, e = openIndexService(ctx, &repo, c.objects, c.indexPolicy, func(offset uint64) {
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
		_ = c.coordinator.releaseOwner(ctx, entry.owner, nil)
		c.remove(entry)
		return
	}
	entry.mu.Lock()
	if entry.retired {
		entry.mu.Unlock()
		service.Index.graph.Close()
		return
	}
	service.publisherTerm = func() (uint64, bool) { return c.indexMaintenanceTerm(entry) }
	entry.service = service
	entry.mu.Unlock()
	if renewed, started, updateErr := c.coordinator.renewOwner(ctx, entry.owner, true); errors.Is(updateErr, ErrFenced) {
		c.remove(entry)
	} else if updateErr == nil {
		entry.grantReadLease(started, renewed, time.Duration(c.coordinator.ttl)*time.Millisecond)
		if service.graphReplica != nil && service.graphReplica.writer != nil {
			if current, err := c.coordinator.Plan(ctx, b.Index); err == nil {
				_ = service.graphReplica.refreshLease(ctx, current)
			}
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
				entry.retireReads()
				_ = c.coordinator.releaseOwner(ctx, entry.owner, nil)
				c.remove(entry)
			}
			return err
		}
	}
	return nil
}
func (c *ClusterService) indexMaintenanceTerm(entry *localPartition) (uint64, bool) {
	term := entry.owner.Epoch
	if c.graphReplication && c.replicas > 1 {
		// Only the publisher appends resets to the replicated graph journal.
		// Independent replicas each own a graph which needs its own rebuild.
		term = entry.maintenanceTerm.Load()
	}
	return term, term != 0 && (!c.localReadLeases() || entry.readLease.InLeaderLease(nil))
}

func (entry *localPartition) setMaintenancePlan(plan ReplicaPlan) {
	if plan.Publisher == entry.owner.Partition && !plan.LeaseDraining {
		entry.maintenanceTerm.Store(plan.Version)
	} else {
		entry.maintenanceTerm.Store(0)
	}
}

func (c *ClusterService) MaybeRebuild(ctx context.Context) error {
	var first error
	for _, entry := range c.entries() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.localReadLeases() {
			if !entry.readLease.InLeaderLease(nil) {
				continue
			}
		} else if e := c.coordinator.Check(ctx, entry.owner); e != nil {
			continue
		}
		entry.mu.RLock()
		s := entry.service
		var err error
		if s != nil {
			err = s.MaybeRebuild(ctx)
		}
		entry.mu.RUnlock()
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *ClusterService) replicaTick(ctx context.Context) {
	hints, err := c.indexPlacement(ctx)
	if err != nil {
		return
	}
	checkedPlans := map[IndexPartitionID]bool{}
	allowed := map[IndexPartitionID]PartitionBinding{}
	plans := map[IndexPartitionID]ReplicaPlan{}
	for _, b := range c.layout.Bindings {
		p, e := c.coordinator.planIn(ctx, hints, b.Index)
		if e != nil {
			continue
		}
		checkedPlans[b.Index] = true
		for _, a := range p.Serving {
			if a.Node == c.node.ID && a.Session == c.node.Session {
				allowed[a.Partition] = b
				plans[a.Partition] = p
			}
		}
	}
	var renewals errgroup.Group
	renewals.SetLimit(4)
	for _, entry := range c.entries() {
		if c.graphReplication {
			entry.setMaintenancePlan(plans[entry.owner.Partition])
		}
		if _, ok := allowed[entry.owner.Partition]; !ok {
			logical := IndexPartitionID(strings.SplitN(string(entry.owner.Partition), "~", 2)[0])
			if c.localReadLeases() && !checkedPlans[logical] {
				// An unreadable plan is not a retirement decision. Do not renew
				// authority; the previously confirmed local lease still expires.
				continue
			}
			leased := c.localReadLeases()
			if leased {
				entry.retireReads()
			}
			releaseErr := c.coordinator.releaseOwner(ctx, entry.owner, nil)
			if leased || releaseErr == nil || errors.Is(releaseErr, ErrFenced) {
				c.remove(entry)
			}
			continue
		}
		binding, plan := allowed[entry.owner.Partition], plans[entry.owner.Partition]
		renewals.Go(func() error {
			c.renewAssignedReplica(ctx, entry, binding, plan)
			return nil
		})
	}
	_ = renewals.Wait()
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

func (c *ClusterService) renewAssignedReplica(ctx context.Context, entry *localPartition, binding PartitionBinding, plan ReplicaPlan) {
	entry.mu.RLock()
	ready := entry.service != nil
	var offset uint64
	var report func(*Ownership)
	if ready {
		if g := entry.service.graphReplica; g != nil {
			if g.input != nil {
				g.input.planChanged(plan.Version, plan.Publisher)
			}
			g.viewMu.RLock()
			graphPos := g.position
			g.viewMu.RUnlock()
			report = func(o *Ownership) {
				// An older capture cannot replace newer progress from this owner.
				if graphPos.DataOffset < o.Offset || o.GraphPosition != nil && graphPos.Sequence < o.GraphPosition.Sequence {
					return
				}
				o.Offset = graphPos.DataOffset
				o.GraphPosition = &graphPos
				o.Phase, o.LastError = "Ready", ""
			}
		} else {
			entry.service.syncMu.Lock()
			offset = entry.service.cursor
			entry.service.syncMu.Unlock()
		}
	}
	entry.mu.RUnlock()
	if _, e := c.renewReplica(ctx, entry, ready, report); errors.Is(e, ErrFenced) {
		c.remove(entry)
	} else if e == nil && ready {
		entry.mu.RLock()
		if entry.service != nil && entry.service.graphReplica != nil {
			_ = entry.service.graphReplica.refreshLease(ctx, plan)
		}
		entry.mu.RUnlock()
		if report == nil {
			_ = c.coordinator.ReportReplica(ctx, binding.Index, plan.Version, entry.owner, "Ready", "", offset)
		}
	}
}
