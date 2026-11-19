package vector

import (
	"testing"
	"time"
)

func TestSampleLoadDoesNotWaitForIndexWriter(t *testing.T) {
	const name = "index-0000"
	index := &HNSWIndex{live: 2, nodes: map[uint64]nodeVersion{1: {}, 2: {}, 3: {}}}
	entry := &localPartition{owner: Ownership{Partition: name}, service: &Service{Index: index}}
	c := &ClusterService{PartitionedService: &PartitionedService{dimension: 64}, locals: map[IndexPartitionID]*localPartition{name: entry}}
	blockedSample := func() ShardLoad {
		t.Helper()
		index.mu.Lock()
		defer index.mu.Unlock()
		done := make(chan map[string]ShardLoad, 1)
		go func() { done <- c.sampleLoad() }()
		select {
		case loads := <-done:
			return loads[name]
		case <-time.After(time.Second):
			t.Fatal("heartbeat load sample waited for index write lock")
			return ShardLoad{}
		}
	}
	if first := blockedSample(); first.RecordsKnown {
		t.Fatalf("initial contended sample claims a known count: %+v", first)
	}
	available := c.sampleLoad()[name]
	if !available.RecordsKnown || available.LiveRecords != 2 || available.EstimatedBytes != 2*64*8+3*(64*4+256) {
		t.Fatalf("first available sample: %+v", available)
	}
	prior := available
	prior.Busy = .5
	c.lastLoad[name] = prior
	start := time.Now().Add(-time.Second)
	c.lastSample = start
	index.live = 1
	entry.queryNanos.Store(int64(time.Second))
	busy := blockedSample()
	elapsed := c.lastSample.Sub(start).Seconds()
	if !busy.RecordsKnown || busy.LiveRecords != available.LiveRecords || busy.EstimatedBytes != available.EstimatedBytes || busy.Busy != .25/elapsed+.75*prior.Busy || entry.queryNanos.Load() != 0 {
		t.Fatalf("contended sample lost stats or busy accounting: %+v", busy)
	}
	if fresh := c.sampleLoad()[name]; !fresh.RecordsKnown || fresh.LiveRecords != 1 || fresh.EstimatedBytes == available.EstimatedBytes {
		t.Fatalf("new count was not picked up: %+v", fresh)
	}
}
