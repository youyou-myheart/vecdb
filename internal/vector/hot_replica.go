package vector

import (
	"context"
	"fmt"
	"time"
)

func (c *ClusterService) hotReplicas() bool {
	return c.graphReplication || c.coordinator != nil && (c.coordinator.failureDomain == "host" || c.coordinator.failureDomain == "zone")
}
func routeNodeKey(o Ownership) string { return o.Node + "/" + o.Session }
func (c *ClusterService) routeCooling(o Ownership) bool {
	c.routingMu.Lock()
	defer c.routingMu.Unlock()
	return time.Now().Before(c.unhealthy[routeNodeKey(o)])
}
func (c *ClusterService) markRoute(o Ownership, failed bool) {
	c.routingMu.Lock()
	defer c.routingMu.Unlock()
	key := routeNodeKey(o)
	if !failed {
		delete(c.unhealthy, key)
		return
	}
	if c.unhealthy == nil {
		c.unhealthy = map[string]time.Time{}
	}
	now := time.Now()
	for k, until := range c.unhealthy {
		if !now.Before(until) {
			delete(c.unhealthy, k)
		}
	}
	if len(c.unhealthy) < 1024 {
		c.unhealthy[key] = now.Add(5 * time.Second)
	}
}
func (c *ClusterService) routingHealth() map[string]int64 {
	c.routingMu.Lock()
	defer c.routingMu.Unlock()
	out := map[string]int64{}
	for key, until := range c.unhealthy {
		if time.Now().Before(until) {
			out[key] = until.UnixMilli()
		}
	}
	return out
}
func (c *ClusterService) partitionHead(ctx context.Context, id IndexPartitionID) (uint64, error) {
	if c.layout.Routing != nil {
		h, e := (graphJournal{c: c.coordinator, id: id}).Head(ctx)
		return h.Committed.DataOffset, e
	}
	for _, b := range c.layout.Bindings {
		if b.Index != id {
			continue
		}
		repo := c.data[b.Data]
		if repo == nil {
			break
		}
		tx, e := repo.begin()
		if e != nil {
			return 0, e
		}
		defer tx.Rollback()
		return repo.logHead(ctx, tx)
	}
	return 0, fmt.Errorf("%w: unknown data binding %s", ErrPartitionUnavailable, id)
}
