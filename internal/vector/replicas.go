package vector

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
)

type ReplicaPlan struct {
	Version   uint64           `json:"version"`
	TaskID    string           `json:"task_id,omitempty"`
	Desired   []Assignment     `json:"desired"`
	Serving   []Assignment     `json:"serving"`
	Barrier   uint64           `json:"barrier"`
	Publisher IndexPartitionID `json:"publisher"`
}

func replicaID(id IndexPartitionID, n ClusterNode) IndexPartitionID {
	return IndexPartitionID(string(id) + "~" + hex.EncodeToString([]byte(n.ID+"/"+n.Session)))
}
func (c *Coordinator) planKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/replica-plans/" + string(id))
}
func (c *Coordinator) planIn(ctx context.Context, tx transactionAPI, id IndexPartitionID) (ReplicaPlan, error) {
	var p ReplicaPlan
	b, e := tx.Get(ctx, c.planKey(id))
	if e == nil {
		e = json.Unmarshal(b, &p)
	}
	return p, e
}
func (c *Coordinator) Plan(ctx context.Context, id IndexPartitionID) (ReplicaPlan, error) {
	tx, e := c.root.begin()
	if e != nil {
		return ReplicaPlan{}, e
	}
	defer tx.Rollback()
	return c.planIn(ctx, tx, id)
}
func containsReplica(list []Assignment, id IndexPartitionID) bool {
	for _, a := range list {
		if a.Partition == id {
			return true
		}
	}
	return false
}
func (c *Coordinator) ScheduleReplicas(ctx context.Context, leader Ownership, layout PartitionLayout, nodes []ClusterNode, count int) error {
	if leader.Partition != masterLeaseID {
		return ErrFenced
	}
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = c.Fence(leader)(ctx, tx); e != nil {
		return e
	}
	if nodes == nil {
		nodes, e = c.nodesIn(ctx, tx)
		if e != nil {
			return e
		}
	}
	workers := []ClusterNode{}
	alive := map[string]bool{}
	for _, n := range nodes {
		if hasRole(n, RoleWorker) {
			alive[n.ID+"/"+n.Session] = true
			if !n.Draining {
				workers = append(workers, n)
			}
		}
	}
	view := newSchedulingView(c, tx)
	var balanced map[IndexPartitionID][]ClusterNode
	if c.loadBalance {
		balanced, e = c.balancedTargetsIn(ctx, tx, layout, workers, count, view)
		if e != nil {
			return e
		}
	}
	for _, b := range layout.Bindings {
		old, err := view.plan(ctx, b.Index)
		if err != nil && !tikverr.IsErrNotFound(err) {
			return err
		}
		next := ReplicaPlan{Version: old.Version, TaskID: old.TaskID, Barrier: old.Barrier, Publisher: old.Publisher}
		choices := balanced[b.Index]
		if !c.loadBalance {
			choices = replicaChoices(b.Index, workers, count, c.failureDomain)
		}
		seenTargets := map[string]bool{}
		selected := []ClusterNode{}
		for i, n := range choices {
			if i >= count {
				break
			}
			if seenTargets[n.ID] || !canPlaceReplica(selected, n, c.failureDomain) {
				return errors.New("placement violates replica anti-affinity")
			}
			seenTargets[n.ID] = true
			selected = append(selected, n)
			next.Desired = append(next.Desired, Assignment{Partition: replicaID(b.Index, n), Node: n.ID, Session: n.Session, URL: n.URL, LeaderEpoch: leader.Epoch})
		}
		changed := len(next.Desired) != len(old.Desired)
		for _, a := range next.Desired {
			if !containsReplica(old.Desired, a.Partition) {
				changed = true
			}
		}
		if changed {
			data := *c.root
			if layout.DataPartitions > 1 {
				data.Prefix = fmt.Sprintf("%sdata/%04d/", c.root.Prefix, b.Data)
			}
			if layout.Routing != nil {
				h, err := (graphJournal{c: c, id: b.Index}).headIn(ctx, tx)
				e = err
				next.Barrier = h.Committed.DataOffset
			} else {
				next.Barrier, e = data.readCounter(ctx, tx, "log-head")
			}
			if e != nil {
				return e
			}
		}
		next.Serving = append(next.Serving, next.Desired...)
		ready := len(next.Desired) == count
		for _, a := range next.Desired {
			o, err := view.owner(ctx, a.Partition)
			if err != nil && !tikverr.IsErrNotFound(err) {
				return err
			}
			if err != nil || !o.Ready || o.Expires <= txnMillis(tx) || o.Offset < next.Barrier {
				ready = false
			}
		}
		if !ready {
			for _, a := range old.Serving {
				if alive[a.Node+"/"+a.Session] && !containsReplica(next.Serving, a.Partition) {
					next.Serving = append(next.Serving, a)
				}
			}
		}
		// Keep the previous publisher while it is still eligible and alive.
		publisher := IndexPartitionID("")
		for _, a := range next.Serving {
			o, err := view.owner(ctx, a.Partition)
			if err != nil && !tikverr.IsErrNotFound(err) {
				return err
			}
			if err == nil && o.Ready && o.Expires > txnMillis(tx) {
				if publisher == "" || a.Partition == old.Publisher {
					publisher = a.Partition
				}
				if a.Partition == old.Publisher {
					break
				}
			}
		}
		next.Publisher = publisher
		if e = c.reconcileTask(ctx, tx, view, leader, b.Index, old, &next, ready); e != nil {
			return e
		}
		for _, a := range next.Serving {
			if e = putJSON(tx, c.assignmentKey(a.Partition), a); e != nil {
				return e
			}
		}
		for _, a := range old.Serving {
			if !containsReplica(next.Serving, a.Partition) {
				if e = putJSON(tx, c.assignmentKey(a.Partition), Assignment{Partition: a.Partition, LeaderEpoch: leader.Epoch}); e != nil {
					return e
				}
			}
		}
		oldJSON, _ := json.Marshal(old)
		nextJSON, _ := json.Marshal(next)
		if string(oldJSON) != string(nextJSON) {
			if e = putJSON(tx, c.taskFenceKey(b.Index), next.Version); e != nil {
				return e
			}
			if e = tx.Set(c.planKey(b.Index), nextJSON); e != nil {
				return e
			}
		}
	}
	return tx.Commit(ctx)
}
func (c *Coordinator) Progress(ctx context.Context, expected Ownership, offset uint64) error {
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	o, e := c.readOwner(ctx, tx, expected.Partition)
	if e != nil {
		return e
	}
	if !sameOwner(o, expected) || o.Expires <= txnMillis(tx) {
		return ErrFenced
	}
	if offset < o.Offset {
		return errors.New("replica offset regression")
	}
	o.Offset = offset
	if e = putJSON(tx, c.ownerKey(o.Partition), o); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (c *Coordinator) ReplicaFence(id IndexPartitionID, owner Ownership) func(context.Context, transactionAPI) error {
	return func(ctx context.Context, tx transactionAPI) error {
		if e := c.Fence(owner)(ctx, tx); e != nil {
			return e
		}
		p, e := c.planIn(ctx, tx, id)
		if e != nil {
			return e
		}
		if p.Publisher != owner.Partition {
			return ErrFenced
		}
		// Serialize publisher changes and membership changes with publication/pruning.
		return putJSON(tx, c.planKey(id), p)
	}
}
func (c *Coordinator) RetainReplicas(id IndexPartitionID) func(context.Context, transactionAPI, uint64) (uint64, error) {
	return func(ctx context.Context, tx transactionAPI, end uint64) (uint64, error) {
		p, e := c.planIn(ctx, tx, id)
		if e != nil {
			return 0, e
		}
		for _, a := range p.Serving {
			o, err := c.readOwner(ctx, tx, a.Partition)
			if tikverr.IsErrNotFound(err) {
				return 0, nil
			}
			if err != nil {
				return 0, err
			}
			if o.Offset < end {
				end = o.Offset
			}
		}
		return end, putJSON(tx, c.planKey(id), p)
	}
}
func (c *ClusterService) Drain(context.Context) error {
	if c.replicas < 2 {
		return errors.New("drain requires replica mode")
	}
	c.draining.Store(true)
	return nil
}
