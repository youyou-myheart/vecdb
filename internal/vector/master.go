package vector

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	tikverr "github.com/tikv/client-go/v2/error"
)

const masterLeaseID IndexPartitionID = "_master"
const RoleMaster = "master"
const RoleGateway = "gateway"
const RoleWorker = "worker"

func parseRoles(value string) ([]string, error) {
	if value == "" {
		value = "master,gateway,worker"
	}
	seen := map[string]bool{}
	roles := []string{}
	for _, r := range strings.Split(value, ",") {
		r = strings.TrimSpace(r)
		if (r != RoleMaster && r != RoleGateway && r != RoleWorker) || seen[r] {
			return nil, errors.New("ROLES must be unique master,gateway,worker entries")
		}
		seen[r] = true
		roles = append(roles, r)
	}
	return roles, nil
}
func hasRole(node ClusterNode, role string) bool {
	for _, r := range node.Roles {
		if r == role {
			return true
		}
	}
	return false
}

type Assignment struct {
	Partition   IndexPartitionID `json:"partition"`
	Node        string           `json:"node"`
	Session     string           `json:"session"`
	URL         string           `json:"url"`
	LeaderEpoch uint64           `json:"leader_epoch"`
}

func (c *Coordinator) assignmentKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/assignments/" + string(id))
}
func (c *Coordinator) assignmentIn(ctx context.Context, tx transactionAPI, id IndexPartitionID) (Assignment, error) {
	var a Assignment
	b, e := tx.Get(ctx, c.assignmentKey(id))
	if e != nil {
		return a, e
	}
	e = json.Unmarshal(b, &a)
	if e == nil && a.Partition != id {
		e = errors.New("assignment key mismatch")
	}
	return a, e
}
func (c *Coordinator) Assignment(ctx context.Context, id IndexPartitionID) (Assignment, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Assignment{}, e
	}
	defer tx.Rollback()
	return c.assignmentIn(ctx, tx, id)
}

// Only the elected master can publish plans. Its fencing write is in the SAME
// transaction as all assignment changes; heartbeat renewal uses a separate key.
func (c *Coordinator) Schedule(ctx context.Context, leader Ownership, layout PartitionLayout, nodes []ClusterNode) error {
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
	workers := []ClusterNode{}
	byID := map[string]ClusterNode{}
	for _, n := range nodes {
		if hasRole(n, RoleWorker) {
			workers = append(workers, n)
			byID[n.ID] = n
		}
	}
	for _, binding := range layout.Bindings {
		target := byID[preferredOwner(binding.Index, workers)]
		next := Assignment{Partition: binding.Index, Node: target.ID, Session: target.Session, URL: target.URL, LeaderEpoch: leader.Epoch}
		old, err := c.assignmentIn(ctx, tx, binding.Index)
		if err != nil && !tikverr.IsErrNotFound(err) {
			return err
		}
		if err == nil && old == next {
			continue
		}
		if e = putJSON(tx, c.assignmentKey(binding.Index), next); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

type Master struct {
	replicas    int
	coordinator *Coordinator
	node        ClusterNode
	layout      PartitionLayout
}

func (m *Master) Tick(ctx context.Context, nodes []ClusterNode) error {
	leader, e := m.coordinator.Resolve(ctx, masterLeaseID)
	if e != nil {
		if !errors.Is(e, ErrFenced) && !tikverr.IsErrNotFound(e) {
			return e
		}
		leader, e = m.coordinator.Acquire(ctx, masterLeaseID, m.node)
		if e != nil {
			return e
		}
	}
	if leader.Node != m.node.ID || leader.Session != m.node.Session {
		return nil
	}
	leader, e = m.coordinator.Update(ctx, leader, true, false)
	if e != nil {
		return e
	}
	if m.replicas > 1 {
		if e = m.coordinator.ScheduleReplicas(ctx, leader, m.layout, nodes, m.replicas); e != nil {
			return e
		}
		if m.layout.Routing != nil {
			nodes, e = m.coordinator.Nodes(ctx)
			if e != nil {
				return e
			}
			eligible := []ClusterNode{}
			for _, n := range nodes {
				if !n.Draining {
					eligible = append(eligible, n)
				}
			}
			return m.coordinator.Schedule(ctx, leader, sourceLayout(m.layout.DataPartitions), eligible)
		}
		return nil
	}
	return m.coordinator.Schedule(ctx, leader, m.layout, nodes)
}
