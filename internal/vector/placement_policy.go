package vector

import (
	"bytes"
	"crypto/sha256"
	"math"
	"sort"
)

func capacity(n ClusterNode) float64 {
	if n.Capacity <= 0 {
		return 1
	}
	return n.Capacity
}
func shardCost(id IndexPartitionID, workers []ClusterNode) float64 {
	cost := 1.0
	for _, n := range workers {
		if sample, ok := n.Load[string(id)]; ok {
			v := 1 + float64(sample.EstimatedBytes)/(64<<20) + 4*sample.Busy
			if !math.IsNaN(v) && !math.IsInf(v, 0) && v > cost {
				cost = v
			}
		}
	}
	return cost
}

func hasNode(nodes []ClusterNode, id string) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// improvePlacement mutates only the supplied target plan, preserving the
// current tie-breaking order. Persistence and cooldown enforcement are callers' responsibilities.
func improvePlacement(layout PartitionLayout, workers []ClusterNode, targets map[IndexPartitionID][]ClusterNode, costs map[IndexPartitionID]float64, loads map[string]float64) bool {
	return improvePlacementInDomain(layout, workers, targets, costs, loads, "node")
}
func improvePlacementInDomain(layout PartitionLayout, workers []ClusterNode, targets map[IndexPartitionID][]ClusterNode, costs map[IndexPartitionID]float64, loads map[string]float64, domain string) bool {
	bestGain := 0.0
	var chosen IndexPartitionID
	var from int
	var to ClusterNode
	for _, b := range layout.Bindings {
		cost := costs[b.Index]
		for slot, source := range targets[b.Index] {
			for _, dest := range workers {
				if dest.ID == source.ID || !canReplaceReplica(targets[b.Index], dest, domain, slot) || !placementFits(b.Index, dest, workers, targets) {
					continue
				}
				sourceLoad := loads[source.ID] / capacity(source)
				destLoad := loads[dest.ID] / capacity(dest)
				if sourceLoad <= destLoad*1.2 {
					continue
				}
				// Require a meaningful reduction in squared normalized load. Avoid moving
				// an indivisible hot shard back and forth when the destination would be worse.
				before := sourceLoad*sourceLoad + destLoad*destLoad
				after := math.Pow((loads[source.ID]-cost)/capacity(source), 2) + math.Pow((loads[dest.ID]+cost)/capacity(dest), 2)
				gain := before - after
				if gain > cost*cost*.01 && gain > bestGain {
					bestGain = gain
					chosen = b.Index
					from = slot
					to = dest
				}
			}
		}
	}
	if chosen != "" {
		targets[chosen][from] = to
		return true
	}
	return false
}

// rankWorkers evaluates one digest per worker instead of recomputing it in
// every sort comparison. The resulting descending SHA256 order is unchanged.
func rankWorkers(id IndexPartitionID, workers []ClusterNode) []ClusterNode {
	type ranked struct {
		node  ClusterNode
		score [32]byte
	}
	ranks := make([]ranked, len(workers))
	for i, n := range workers {
		ranks[i] = ranked{n, sha256.Sum256([]byte(string(id) + "\x00" + n.ID))}
	}
	sort.Slice(ranks, func(i, j int) bool { return bytes.Compare(ranks[i].score[:], ranks[j].score[:]) > 0 })
	result := make([]ClusterNode, len(ranks))
	for i, r := range ranks {
		result[i] = r.node
	}
	return result
}

// Reserve reported resident memory plus unreported desired replicas. A new
// replica gets 50% restore headroom; this is an admission estimate, not an RSS cap.
func estimatedShardBytes(id IndexPartitionID, workers []ClusterNode) float64 {
	size := float64(1 << 20)
	for _, n := range workers {
		if sample, ok := n.Load[string(id)]; ok {
			size = max(size, float64(sample.EstimatedBytes))
		}
	}
	return size
}
func placementFits(id IndexPartitionID, dest ClusterNode, workers []ClusterNode, targets map[IndexPartitionID][]ClusterNode) bool {
	if dest.MemoryBudgetBytes == 0 {
		return true
	}
	used := float64(0)
	for _, sample := range dest.Load {
		used += float64(sample.EstimatedBytes)
	}
	for shard, nodes := range targets {
		if hasNode(nodes, dest.ID) {
			if _, reported := dest.Load[string(shard)]; !reported {
				used += estimatedShardBytes(shard, workers)
			}
		}
	}
	return used+1.5*estimatedShardBytes(id, workers) <= float64(dest.MemoryBudgetBytes)
}
