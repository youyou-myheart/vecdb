package vector

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (c *Coordinator) retirementKey(id IndexPartitionID, epoch uint64) []byte {
	return []byte(fmt.Sprintf("%scluster/retired/%s/%020d", c.root.Prefix, id, epoch))
}

// Called only after the local graph and its loader have both exited.
func (c *ClusterService) flushRetirements(ctx context.Context) {
	c.mu.Lock()
	pending := make(map[string]Ownership, len(c.pendingRetirements))
	for key, o := range c.pendingRetirements {
		pending[key] = o
	}
	c.mu.Unlock()
	for key, o := range pending {
		tx, e := c.root.begin()
		if e != nil {
			continue
		}
		e = putJSON(tx, []byte(key), o)
		if e == nil {
			e = tx.Commit(ctx)
		}
		_ = tx.Rollback()
		if e == nil {
			c.mu.Lock()
			delete(c.pendingRetirements, key)
			c.mu.Unlock()
		}
	}
}
func (c *ClusterService) acknowledgeRetirement(o Ownership) {
	if c.replicas < 2 {
		return
	}
	key := string(c.coordinator.retirementKey(o.Partition, o.Epoch))
	c.mu.Lock()
	if c.pendingRetirements == nil {
		c.pendingRetirements = map[string]Ownership{}
	}
	c.pendingRetirements[key] = o
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c.flushRetirements(ctx)
}
func (c *ClusterService) startLoad(ctx context.Context, b PartitionBinding, o Ownership) {
	loadCtx, cancel := context.WithCancel(ctx)
	entry := &localPartition{owner: o, cancel: cancel, loadDone: make(chan struct{})}
	c.mu.Lock()
	c.locals[o.Partition] = entry
	c.mu.Unlock()
	go func() {
		defer close(entry.loadDone)
		defer cancel()
		c.load(loadCtx, b, entry)
		var maintenance sync.WaitGroup
		if c.coordinator.referenceLogs {
			maintenance.Add(1)
			go func() { defer maintenance.Done(); c.runGraphMaintenance(loadCtx, entry) }()
		}
		if c.EventDrivenIndexing() {
			c.runRoutedIndex(loadCtx, entry)
		} else if c.coordinator.referenceLogs {
			<-loadCtx.Done()
		}
		cancel()
		maintenance.Wait()
	}()
}
