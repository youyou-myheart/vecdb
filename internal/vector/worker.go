package vector

import (
	"context"
	"errors"
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
			if _, e = c.coordinator.Update(ctx, entry.owner, false, true); e == nil || errors.Is(e, ErrFenced) {
				c.remove(entry)
			}
			continue
		}
		entry.mu.RLock()
		ready := entry.service != nil
		entry.mu.RUnlock()
		if _, e = c.coordinator.Update(ctx, entry.owner, ready, false); e != nil && errors.Is(e, ErrFenced) {
			c.remove(entry)
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
