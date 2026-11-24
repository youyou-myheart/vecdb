package vector

import (
	"context"
	"errors"
	"time"

	"example.com/vector-storage/internal/lease"
)

// Worker follows durable master intent. It never computes placement itself.
type Worker struct{ cluster *ClusterService }

func (c *ClusterService) renewReplica(ctx context.Context, entry *localPartition, ready bool, report func(*Ownership)) (Ownership, error) {
	ttl := time.Duration(c.coordinator.ttl) * time.Millisecond
	if ready && c.localReadLeases() {
		if grant, live := entry.readLease.Snapshot(); live && time.Until(grant.Deadline) > ttl/2 {
			return Ownership{}, nil
		}
	}
	o, started, err := lease.Refresh(ctx, func(ctx context.Context) (Ownership, error) {
		return c.coordinator.update(ctx, entry.owner, ready, false, report)
	})
	if err == nil && ready {
		entry.grantReadLease(started, o, ttl)
	}
	return o, err
}

func (w *Worker) Tick(ctx context.Context) {
	c := w.cluster
	if c.replicas > 1 {
		c.replicaTick(ctx)
		return
	}
	hints, err := c.indexPlacement(ctx)
	if err != nil {
		return
	}
	for _, entry := range c.entries() {
		a, e := c.coordinator.assignmentIn(ctx, hints, entry.owner.Partition)
		if e != nil {
			continue
		}
		if a.Node != c.node.ID || a.Session != c.node.Session {
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
		entry.mu.RLock()
		ready := entry.service != nil
		entry.mu.RUnlock()
		if _, err := c.renewReplica(ctx, entry, ready, nil); errors.Is(err, ErrFenced) {
			c.remove(entry)
		}
	}
	for _, b := range c.layout.Bindings {
		a, e := c.coordinator.assignmentIn(ctx, hints, b.Index)
		if e != nil || a.Node != c.node.ID || a.Session != c.node.Session {
			continue
		}
		c.mu.Lock()
		_, exists := c.locals[b.Index]
		c.mu.Unlock()
		if exists {
			continue
		}
		owner, e := c.coordinator.Acquire(ctx, b.Index, c.node)
		if e != nil {
			continue
		}
		c.startLoad(ctx, b, owner)
	}
}
