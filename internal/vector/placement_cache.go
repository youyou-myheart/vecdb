package vector

import (
	"context"
	"sync"
	"time"
)

const dispatchControlInterval = 500 * time.Millisecond

// Placement is a hint. Each loop can use a shared snapshot once, within one
// 500 ms data inspection interval. The election transaction still validates the assignment.
type placementCache struct {
	mu      sync.Mutex
	loading chan struct{}
	started time.Time
	values  map[string][]byte
	forData bool // the opposite loop may consume values once
}

func (c *ClusterService) placementKeys(data bool) [][]byte {
	if data {
		keys := make([][]byte, c.layout.DataPartitions)
		for i := range keys {
			keys[i] = c.coordinator.assignmentKey(sourceID(DataPartitionID(i)))
		}
		return keys
	}
	key := c.coordinator.assignmentKey
	if c.replicas > 1 {
		key = c.coordinator.planKey
	}
	keys := make([][]byte, len(c.layout.Bindings))
	for i, b := range c.layout.Bindings {
		keys[i] = key(b.Index)
	}
	return keys
}

func (c *ClusterService) indexPlacement(ctx context.Context) (prefetchedTransaction, error) {
	values, err := c.placementValues(ctx, false)
	return prefetchedTransaction{values: values}, err
}

func (c *ClusterService) placementValues(ctx context.Context, data bool) (map[string][]byte, error) {
	read := c.coordinator.readControlKeys
	if c.layout.Routing == nil {
		return read(ctx, c.placementKeys(data))
	}
	cache := &c.placement
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cache.mu.Lock()
		if flight := cache.loading; flight != nil {
			cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-flight:
			}
			continue
		}
		age := time.Since(cache.started)
		if cache.values != nil && cache.forData == data && age < dispatchControlInterval {
			values := cache.values
			cache.values = nil
			cache.mu.Unlock()
			return values, nil
		}
		// The first inspector initializes both families. Subsequently, only
		// an index tick prefetches the data keys. The data timer can never
		// raise index inspection frequency, even if heartbeats are delayed.
		if data && !cache.started.IsZero() {
			cache.mu.Unlock()
			return read(ctx, c.placementKeys(true))
		}
		cache.loading = make(chan struct{})
		cache.started = time.Now()
		cache.mu.Unlock()

		keys := append(c.placementKeys(data), c.placementKeys(!data)...)
		// A slow key family may hold up its peer for at most one data tick.
		// On timeout each loop resumes with an independent snapshot.
		jointCtx, cancel := context.WithTimeout(ctx, dispatchControlInterval)
		values, err := read(jointCtx, keys)
		cancel()
		cache.mu.Lock()
		cache.values = nil
		if err == nil {
			cache.values = values
			cache.forData = !data
		}
		close(cache.loading)
		cache.loading = nil
		cache.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			// An inaccessible Region in the other key family must not stop
			// valid assignments in this loop from being reconciled.
			return read(ctx, c.placementKeys(data))
		}
		return values, err
	}
}
