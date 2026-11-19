package vector

import (
	"context"
	"errors"
	"time"
)

// Worker follows durable master intent. It never computes placement itself.
type Worker struct{ cluster *ClusterService }

func (w *Worker) Tick(ctx context.Context) {
	c := w.cluster
	if c.replicas > 1 {
		c.replicaTick(ctx)
		return
	}
	for _, entry := range c.entries() {
		a, e := c.coordinator.Assignment(ctx, entry.owner.Partition)
		if e != nil {
			continue
		}
		if a.Node != c.node.ID || a.Session != c.node.Session {
			leased := c.localReadLeases()
			if leased {
				entry.retireReads()
			}
			_, releaseErr := c.coordinator.Update(ctx, entry.owner, false, true)
			if leased || releaseErr == nil || errors.Is(releaseErr, ErrFenced) {
				c.remove(entry)
			}
			continue
		}
		entry.mu.RLock()
		ready := entry.service != nil
		entry.mu.RUnlock()
		started := time.Now()
		if renewed, updateErr := c.coordinator.Update(ctx, entry.owner, ready, false); updateErr != nil && errors.Is(updateErr, ErrFenced) {
			c.remove(entry)
		} else if updateErr == nil && ready {
			entry.grantReadLease(started, renewed, time.Duration(c.coordinator.ttl)*time.Millisecond)
		}
	}
	for _, b := range c.layout.Bindings {
		a, e := c.coordinator.Assignment(ctx, b.Index)
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
