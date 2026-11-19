package vector

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"example.com/vector-storage/internal/lease"
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
	metrics     *pipelineMetrics
	replicas    int
	coordinator *Coordinator
	node        ClusterNode
	layout      PartitionLayout
	owner       Ownership // Accessed by the control loop and released after it stops.
	leader      atomic.Pointer[lease.Handle]
}

func (m *Master) InLeaderLease(epoch *uint64) bool {
	state := m.leader.Load()
	return state != nil && state.InLeaderLease(epoch)
}

func (m *Master) retireLeader() {
	if state := m.leader.Load(); state != nil {
		state.Retire()
		m.leader.Store(nil)
	}
}

func (m *Master) releaseLease(ctx context.Context) {
	m.retireLeader()
	_ = m.coordinator.releaseOwner(ctx, m.owner, nil)
	m.owner = Ownership{}
}

func (m *Master) Tick(ctx context.Context, nodes []ClusterNode) error {
	leader, started, err := m.coordinator.campaignOwner(ctx, m.node, masterLeaseID, m.owner, nil, nil)
	if m.owner.Epoch != 0 && (errors.Is(err, ErrFenced) || tikverr.IsErrNotFound(err)) {
		m.retireLeader()
		m.owner = Ownership{}
		leader, started, err = m.coordinator.campaignOwner(ctx, m.node, masterLeaseID, m.owner, nil, nil)
	}
	if errors.Is(err, ErrConflict) {
		return nil
	}
	if err != nil {
		return err
	}
	m.owner = leader
	state := m.leader.Load()
	if state == nil {
		state = &lease.Handle{}
		m.leader.Store(state)
	}
	state.Confirm(started, lease.Grant{
		Epoch: leader.Epoch, Expires: leader.Expires, Remaining: time.Duration(m.coordinator.ttl) * time.Millisecond,
	})
	var epoch uint64
	if !m.InLeaderLease(&epoch) || epoch != leader.Epoch {
		return ErrFenced
	}
	if err := m.schedule(ctx, leader, nodes); err != nil {
		return err
	}
	var after uint64
	if !m.InLeaderLease(&after) || after != epoch {
		return ErrFenced
	}
	return nil
}

func (m *Master) schedule(ctx context.Context, leader Ownership, nodes []ClusterNode) error {
	if m.replicas > 1 {
		if e := retryMasterSchedule(ctx, func(attempt int) (err error) {
			if m.metrics != nil {
				defer m.metrics.track("master_schedule_attempt", "")(&err)
			}
			// Refresh nodes as well as fences/plans after a definite conflict.
			current := nodes
			if attempt > 0 {
				current = nil
			}
			return m.coordinator.ScheduleReplicas(ctx, leader, m.layout, current, m.replicas)
		}); e != nil {
			return e
		}
		if m.layout.Routing != nil {
			nodes, e := m.coordinator.Nodes(ctx)
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

// Master and Worker heartbeats can remain phase-aligned for many ticks. A
// scheduling write conflict must get a fresh bounded chance within this tick;
// waiting only for the next identical heartbeat can starve a drain forever.
func retryMasterSchedule(ctx context.Context, schedule func(int) error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := schedule(attempt)
		if !tikverr.IsErrWriteConflict(err) || attempt >= 3 {
			return err
		}
		if !waitIndexRetry(ctx, time.Duration(5+rand.Intn(16))*time.Millisecond) {
			return ctx.Err()
		}
	}
}
