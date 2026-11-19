package vector

import (
	"context"
	"errors"
	"sync"
	"time"
)

const routePlanReadBudget = 100 * time.Millisecond

// Legacy/non-lease routing: a cached plan is only a read-routing hint. Every chosen replica must still
// pass Resolve and the Worker's before/after owner checks. Mutations and
// maintenance never use this fallback to establish publisher authority.
type routePlanCache struct {
	mu     sync.Mutex
	plan   ReplicaPlan
	readAt time.Time
}

func (c *routePlanCache) hint() (ReplicaPlan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.plan, len(c.plan.Serving) > 0
}

func (c *routePlanCache) read(ctx context.Context, fetch func(context.Context) (ReplicaPlan, error)) (ReplicaPlan, bool, error) {
	started := time.Now()
	readCtx := ctx
	if _, available := c.hint(); available {
		var cancel context.CancelFunc
		readCtx, cancel = context.WithTimeout(ctx, routePlanReadBudget)
		defer cancel()
	}
	plan, err := fetch(readCtx)
	if err == nil {
		c.mu.Lock()
		// Concurrent requests must not replace a newer observation with an old
		// snapshot that took longer to return. Empty plans also invalidate hints.
		if !started.Before(c.readAt) && plan.Version >= c.plan.Version {
			c.plan, c.readAt = plan, started
		}
		c.mu.Unlock()
		return plan, false, nil
	}
	if ctx.Err() != nil {
		return ReplicaPlan{}, false, ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if cached, available := c.hint(); available {
			return cached, true, nil
		}
	} else {
		// Missing/corrupt plans and other errors are not hidden by old routes.
		c.mu.Lock()
		if !started.Before(c.readAt) {
			c.plan.Serving, c.readAt = nil, started
		}
		c.mu.Unlock()
	}
	return ReplicaPlan{}, false, err
}

func (p *remotePartition) routingPlan(ctx context.Context, op string) (ReplicaPlan, error) {
	fetch := func(ctx context.Context) (plan ReplicaPlan, err error) {
		defer p.cluster.observations.pipeline.track("route_plan", string(p.id))(&err)
		return p.cluster.coordinator.Plan(ctx, p.id)
	}
	if op != "search" || !p.cluster.graphReplication {
		return fetch(ctx)
	}
	plan, fallback, err := p.plans.read(ctx, fetch)
	if fallback {
		metrics := &p.cluster.observations.pipeline
		metrics.init()
		metrics.routePlanFallback.WithLabelValues(string(p.id)).Inc()
	}
	return plan, err
}
