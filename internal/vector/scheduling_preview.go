package vector

import (
	"context"
	"errors"
)

type PlacementExplanation struct {
	Partition IndexPartitionID `json:"partition"`
	Version   uint64           `json:"version"`
	TaskID    string           `json:"task_id,omitempty"`
	Current   []Assignment     `json:"current"`
	Proposed  []ClusterNode    `json:"proposed"`
	Cost      float64          `json:"cost"`
	Changed   bool             `json:"changed"`
	Task      *MigrationTask   `json:"task,omitempty"`
}
type BalancePreview struct {
	FailureDomain     string                 `json:"failure_domain"`
	Nodes             []ClusterNode          `json:"nodes"`
	SnapshotTS        uint64                 `json:"snapshot_ts"`
	ObservedAt        int64                  `json:"observed_at_ms"`
	Reason            string                 `json:"reason"`
	CooldownRemaining int64                  `json:"cooldown_remaining_ms"`
	NormalizedLoads   map[string]float64     `json:"normalized_loads"`
	Partitions        []PlacementExplanation `json:"partitions"`
}

// Preview captures registry, plans, leases, tasks and cooldown in ONE read-only
// transaction. It invokes the same policy as Master and never consumes cooldown.
func (c *ClusterService) Preview(ctx context.Context) (BalancePreview, error) {
	out := BalancePreview{NormalizedLoads: map[string]float64{}, Partitions: []PlacementExplanation{}}
	if c.replicas < 2 {
		return out, errors.New("migration tasks require replica mode")
	}
	tx, e := c.root.begin()
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	nodes, e := c.coordinator.nodesIn(ctx, tx)
	if e != nil {
		return out, e
	}
	workers := []ClusterNode{}
	for _, n := range nodes {
		if hasRole(n, RoleWorker) && !n.Draining {
			workers = append(workers, n)
		}
	}
	view := newSchedulingView(c.coordinator, tx)
	snapshot, e := c.coordinator.placementSnapshot(ctx, tx, c.layout, workers, view)
	if e != nil {
		return out, e
	}
	decision := planPlacement(c.layout, snapshot, c.replicas)
	if !c.coordinator.loadBalance {
		decision.Reason = "hash_placement"
		decision.Move = false
		for _, b := range c.layout.Bindings {
			ranked := replicaChoices(b.Index, workers, c.replicas, c.coordinator.failureDomain)
			decision.Targets[b.Index] = ranked[:min(c.replicas, len(ranked))]
		}
	}
	out.FailureDomain = c.coordinator.failureDomain
	out.Nodes = nodes
	out.SnapshotTS = tx.StartTS()
	out.ObservedAt = snapshot.Now
	out.Reason = decision.Reason
	out.CooldownRemaining = max(int64(0), snapshot.Cooldown-(snapshot.Now-snapshot.LastMove))
	for _, n := range workers {
		out.NormalizedLoads[n.ID] = decision.Loads[n.ID] / capacity(n)
	}
	for _, b := range c.layout.Bindings {
		old := snapshot.Plans[b.Index]
		ex := PlacementExplanation{Partition: b.Index, Version: old.Version, TaskID: old.TaskID, Current: old.Desired, Proposed: decision.Targets[b.Index], Cost: decision.Costs[b.Index]}
		target := []Assignment{}
		for _, n := range ex.Proposed {
			target = append(target, Assignment{Partition: replicaID(b.Index, n)})
		}
		ex.Changed = membershipChanged(old.Desired, target)
		if old.TaskID != "" {
			task, e := c.coordinator.taskIn(ctx, tx, old.TaskID)
			if e != nil {
				return out, e
			}
			ex.Task = &task
		}
		out.Partitions = append(out.Partitions, ex)
	}
	return out, nil
}
