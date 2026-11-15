package vector

import (
	"context"
	"encoding/json"
	tikverr "github.com/tikv/client-go/v2/error"
	"math"
	"sort"
)

// PlacementSnapshot is immutable input to the policy: no clocks, I/O or writes.
type PlacementSnapshot struct {
	FailureDomain string
	Workers       []ClusterNode
	Plans         map[IndexPartitionID]ReplicaPlan
	Owners        map[IndexPartitionID]Ownership
	Active        map[IndexPartitionID]bool
	Now           int64
	LastMove      int64
	Cooldown      int64
}
type PlacementDecision struct {
	Targets map[IndexPartitionID][]ClusterNode
	Costs   map[IndexPartitionID]float64
	Loads   map[string]float64
	Reason  string
	Move    bool
}

func planPlacement(layout PartitionLayout, snapshot PlacementSnapshot, count int) PlacementDecision {
	workers := snapshot.Workers
	workers = append([]ClusterNode(nil), workers...)
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	byID := map[string]ClusterNode{}
	for _, n := range workers {
		byID[n.ID+"/"+n.Session] = n
	}
	targets := map[IndexPartitionID][]ClusterNode{}
	costs := map[IndexPartitionID]float64{}
	loads := map[string]float64{}
	stable := true
	repaired := false
	for _, b := range layout.Bindings {
		cost := shardCost(b.Index, workers)
		costs[b.Index] = cost
		old := snapshot.Plans[b.Index]
		for _, a := range old.Desired {
			if n, ok := byID[a.Node+"/"+a.Session]; ok && canPlaceReplica(targets[b.Index], n, snapshot.FailureDomain) {
				targets[b.Index] = append(targets[b.Index], n)
				loads[n.ID] += cost
			}
		}
		if len(old.Serving) > len(old.Desired) || snapshot.Active[b.Index] {
			stable = false
		}
		for _, a := range old.Desired {
			o := snapshot.Owners[a.Partition]
			if !o.Ready || o.Expires <= snapshot.Now || o.Offset < old.Barrier {
				stable = false
			}
		}
	}
	desired := min(count, len(workers))
	for _, b := range layout.Bindings {
		if len(targets[b.Index]) > desired {
			targets[b.Index] = targets[b.Index][:desired]
			repaired = true
		}
		for len(targets[b.Index]) < desired {
			best := -1
			score := math.Inf(1)
			for i, n := range workers {
				if !canPlaceReplica(targets[b.Index], n, snapshot.FailureDomain) || !placementFits(b.Index, n, workers, targets) {
					continue
				}
				s := (loads[n.ID] + costs[b.Index]) / capacity(n)
				if s < score {
					best = i
					score = s
				}
			}
			if best < 0 {
				break
			}
			n := workers[best]
			targets[b.Index] = append(targets[b.Index], n)
			loads[n.ID] += costs[b.Index]
			repaired = true
		}
	}
	for _, b := range layout.Bindings {
		if len(targets[b.Index]) < desired {
			return PlacementDecision{Targets: targets, Costs: costs, Loads: loads, Reason: placementBlockReason(snapshot.FailureDomain)}
		}
	}
	if repaired || !stable || len(workers) <= count {
		reason := "insufficient_workers"
		if repaired {
			reason = "membership_repair"
		} else if !stable {
			reason = "migration_in_progress"
		}
		return PlacementDecision{Targets: targets, Costs: costs, Loads: loads, Reason: reason}
	}

	if snapshot.Now-snapshot.LastMove < snapshot.Cooldown {
		return PlacementDecision{Targets: targets, Costs: costs, Loads: loads, Reason: "cooldown"}
	}
	moved := improvePlacementInDomain(layout, workers, targets, costs, loads, snapshot.FailureDomain)
	reason := "balanced"
	if moved {
		reason = "load_imbalance"
	}
	return PlacementDecision{Targets: targets, Costs: costs, Loads: loads, Reason: reason, Move: moved}
}
func (c *Coordinator) placementSnapshot(ctx context.Context, tx transactionAPI, layout PartitionLayout, workers []ClusterNode, view *schedulingView) (PlacementSnapshot, error) {
	s := PlacementSnapshot{FailureDomain: c.failureDomain, Workers: workers, Plans: map[IndexPartitionID]ReplicaPlan{}, Owners: map[IndexPartitionID]Ownership{}, Active: map[IndexPartitionID]bool{}, Now: txnMillis(tx), Cooldown: c.balanceCooldown.Milliseconds()}
	for _, b := range layout.Bindings {
		p, e := view.plan(ctx, b.Index)
		if e != nil && !tikverr.IsErrNotFound(e) {
			return s, e
		}
		s.Plans[b.Index] = p
		if p.TaskID != "" {
			task, e := c.taskIn(ctx, tx, p.TaskID)
			if e != nil {
				return s, e
			}
			s.Active[b.Index] = !task.terminal()
		}
		for _, a := range p.Desired {
			o, e := view.owner(ctx, a.Partition)
			if e != nil && !tikverr.IsErrNotFound(e) {
				return s, e
			}
			s.Owners[a.Partition] = o
		}
	}
	raw, e := tx.Get(ctx, []byte(c.root.Prefix+"cluster/balance-last-move"))
	if e == nil {
		e = json.Unmarshal(raw, &s.LastMove)
	} else if tikverr.IsErrNotFound(e) {
		e = nil
	}
	return s, e
}
func (c *Coordinator) balancedTargets(ctx context.Context, tx transactionAPI, layout PartitionLayout, workers []ClusterNode, count int) (map[IndexPartitionID][]ClusterNode, error) {
	return c.balancedTargetsIn(ctx, tx, layout, workers, count, newSchedulingView(c, tx))
}
func (c *Coordinator) balancedTargetsIn(ctx context.Context, tx transactionAPI, layout PartitionLayout, workers []ClusterNode, count int, view *schedulingView) (map[IndexPartitionID][]ClusterNode, error) {
	snapshot, e := c.placementSnapshot(ctx, tx, layout, workers, view)
	if e != nil {
		return nil, e
	}
	decision := planPlacement(layout, snapshot, count)
	if decision.Move {
		if e = putJSON(tx, []byte(c.root.Prefix+"cluster/balance-last-move"), snapshot.Now); e != nil {
			return nil, e
		}
	}
	return decision.Targets, nil
}

func placementBlockReason(policy string) string {
	if policy == "host" || policy == "zone" {
		return "insufficient_failure_domains_or_capacity"
	}
	return "insufficient_capacity"
}
