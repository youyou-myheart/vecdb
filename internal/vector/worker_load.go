package vector

import (
	"strings"
	"time"
)

// Estimated vector/graph storage and query wall-time, not process RSS or CPU.
type ShardLoad struct {
	LiveRecords    uint64  `json:"live_records"`
	RecordsKnown   bool    `json:"records_known"`
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
			id := strings.SplitN(string(entry.owner.Partition), "~", 2)[0]
			load := result[id]
			busy := float64(entry.queryNanos.Swap(0)) / 1e9 / elapsed
			load.Busy = .25*busy + .75*c.lastLoad[id].Busy
			// Load estimates run on the heartbeat goroutine. A graph write or
			// replacement must not hold up lease renewal for any shard.
			index := s.Index
			if index.mu.TryRLock() {
				live, nodes := uint64(index.live), uint64(len(index.nodes))
				index.mu.RUnlock()
				load.EstimatedBytes = live*uint64(c.dimension)*8 + nodes*uint64(c.dimension*4+256)
				load.LiveRecords = max(live, load.LiveRecords)
				load.RecordsKnown = true
			}
			result[id] = load
		}
		entry.mu.RUnlock()
	}
	for id, load := range result {
		if !load.RecordsKnown {
			prior := c.lastLoad[id]
			prior.Busy = load.Busy
			result[id] = prior
		}
	}
	c.lastLoad = result
	return result
}
