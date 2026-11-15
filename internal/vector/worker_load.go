package vector

import (
	"strings"
	"time"
)

// Estimated vector/graph storage and query wall-time, not process RSS or CPU.
type ShardLoad struct {
	EstimatedBytes uint64  `json:"estimated_bytes"`
	Busy           float64 `json:"busy"`
}

func (c *ClusterService) sampleLoad() map[string]ShardLoad {
	now := time.Now()
	elapsed := now.Sub(c.lastSample).Seconds()
	c.lastSample = now
	if elapsed <= 0 || elapsed > 3600 {
		elapsed = 1
	}
	result := map[string]ShardLoad{}
	for _, entry := range c.entries() {
		entry.mu.RLock()
		s := entry.service
		if s != nil {
			stats := s.Index.Stats()
			bytes := uint64(stats.Live)*uint64(c.dimension)*8 + uint64(stats.GraphNodes)*uint64(c.dimension*4+256)
			busy := float64(entry.queryNanos.Swap(0)) / 1e9 / elapsed
			id := strings.SplitN(string(entry.owner.Partition), "~", 2)[0]
			prior := c.lastLoad[id]
			result[id] = ShardLoad{EstimatedBytes: bytes, Busy: .25*busy + .75*prior.Busy}
		}
		entry.mu.RUnlock()
	}
	c.lastLoad = result
	return result
}
