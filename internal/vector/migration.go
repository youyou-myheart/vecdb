package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	tikverr "github.com/tikv/client-go/v2/error"
)

type MigrationStage string

const (
	MigrationPending    MigrationStage = "Pending"
	MigrationRestoring  MigrationStage = "Restoring"
	MigrationCatchingUp MigrationStage = "CatchingUp"
	MigrationReady      MigrationStage = "Ready"
	MigrationSwitching  MigrationStage = "Switching"
	MigrationRetiring   MigrationStage = "Retiring"
	MigrationCompleted  MigrationStage = "Completed"
	MigrationSuperseded MigrationStage = "Superseded"
)

type MigrationEvent struct {
	Stage MigrationStage `json:"stage"`
	At    int64          `json:"at_ms"`
}
type ReplicaProgress struct {
	Replica IndexPartitionID `json:"replica"`
	Offset  uint64           `json:"offset"`
	Lag     uint64           `json:"lag"`
	Ready   bool             `json:"ready"`
	Phase   string           `json:"phase,omitempty"`
	Error   string           `json:"error,omitempty"`
}
type MigrationTask struct {
	DroppedEvents  uint64                      `json:"dropped_events,omitempty"`
	SourceEpochs   map[IndexPartitionID]uint64 `json:"source_epochs"`
	LastError      string                      `json:"last_error,omitempty"`
	ID             string                      `json:"id"`
	Partition      IndexPartitionID            `json:"partition"`
	Version        uint64                      `json:"version"`
	LeaderEpoch    uint64                      `json:"leader_epoch"`
	Source         []Assignment                `json:"source"`
	Target         []Assignment                `json:"target"`
	Barrier        uint64                      `json:"barrier"`
	Stage          MigrationStage              `json:"stage"`
	Reason         string                      `json:"reason"`
	Blocked        string                      `json:"blocked,omitempty"`
	CreatedAt      int64                       `json:"created_at_ms"`
	UpdatedAt      int64                       `json:"updated_at_ms"`
	StageStartedAt int64                       `json:"stage_started_at_ms"`
	TimedOut       bool                        `json:"timed_out"`
	Progress       []ReplicaProgress           `json:"progress"`
	History        []MigrationEvent            `json:"history"`
}

func (t MigrationTask) terminal() bool {
	return t.Stage == MigrationCompleted || t.Stage == MigrationSuperseded
}
func (t *MigrationTask) transition(stage MigrationStage, now int64) {
	if t.Stage == stage {
		return
	}
	t.Stage = stage
	t.StageStartedAt = now
	t.UpdatedAt = now
	t.History = append(t.History, MigrationEvent{stage, now})
	if len(t.History) > 64 {
		t.History = append([]MigrationEvent(nil), t.History[len(t.History)-64:]...)
		t.DroppedEvents++
	}
}
func (c *Coordinator) taskFenceKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/task-fences/" + string(id))
}
func (c *Coordinator) taskKey(id string) []byte { return []byte(c.root.Prefix + "cluster/tasks/" + id) }
func (c *Coordinator) taskIn(ctx context.Context, tx transactionAPI, id string) (MigrationTask, error) {
	var t MigrationTask
	b, e := tx.Get(ctx, c.taskKey(id))
	if e != nil {
		return t, e
	}
	e = json.Unmarshal(b, &t)
	if e == nil && t.ID != id {
		e = errors.New("migration task key mismatch")
	}
	return t, e
}
func membershipChanged(a, b []Assignment) bool {
	if len(a) != len(b) {
		return true
	}
	for _, n := range a {
		if !containsReplica(b, n.Partition) {
			return true
		}
	}
	return false
}

// reconcileTask shares the master's fenced transaction with plan/assignment writes.
// Ready and Switching are durable events in the atomic switch, not extra polling delays.
func (c *Coordinator) reconcileTask(ctx context.Context, tx transactionAPI, view *schedulingView, leader Ownership, id IndexPartitionID, old ReplicaPlan, next *ReplicaPlan, ready bool) error {
	now := txnMillis(tx)
	var task MigrationTask
	var err error
	if old.TaskID != "" {
		task, err = c.taskIn(ctx, tx, old.TaskID)
		if err == nil && (task.Version != old.Version || task.Partition != id) {
			return errors.New("task/plan generation mismatch")
		}
		if err != nil {
			return err
		}
	}
	changed := membershipChanged(old.Desired, next.Desired)
	if old.TaskID == "" || changed {
		if old.TaskID != "" && !task.terminal() {
			task.transition(MigrationSuperseded, now)
			task.Blocked = "membership changed; replacement task continues from current serving plan"
			if err = putJSON(tx, c.taskKey(task.ID), task); err != nil {
				return err
			}
		}
		next.Version = old.Version + 1
		next.TaskID = fmt.Sprintf("%s-%020d", id, next.Version)
		reason := "membership_repair"
		if len(old.Desired) == 0 {
			reason = "initial_placement"
		} else if c.loadBalance && len(old.Serving) == len(old.Desired) {
			reason = "load_imbalance"
		}
		for _, a := range old.Desired {
			raw, e := tx.Get(ctx, c.nodeKey(a.Node))
			if e != nil && !tikverr.IsErrNotFound(e) {
				return e
			}
			var node ClusterNode
			if e == nil {
				if e = json.Unmarshal(raw, &node); e != nil {
					return e
				}
			}
			if node.Expires <= now || node.Session != a.Session {
				reason = "worker_unavailable"
				break
			}
			if node.Draining {
				reason = "worker_draining"
			}
		}
		task = MigrationTask{ID: next.TaskID, Partition: id, Version: next.Version, LeaderEpoch: leader.Epoch, Source: old.Serving, Target: next.Desired, Barrier: next.Barrier, CreatedAt: now, Reason: reason}
		task.SourceEpochs = map[IndexPartitionID]uint64{}
		for _, a := range task.Source {
			o, e := view.owner(ctx, a.Partition)
			if e != nil && !tikverr.IsErrNotFound(e) {
				return e
			}
			task.SourceEpochs[a.Partition] = o.Epoch
		}
		task.transition(MigrationPending, now)
	}
	if !changed && old.TaskID != "" && task.terminal() {
		return nil
	}
	before, _ := json.Marshal(task)
	task.LeaderEpoch = leader.Epoch
	task.Progress = []ReplicaProgress{}
	task.Blocked = ""
	task.TimedOut = false
	anyOwner := false
	allLoaded := len(next.Desired) > 0
	for _, a := range next.Desired {
		o, e := view.owner(ctx, a.Partition)
		if e != nil && !tikverr.IsErrNotFound(e) {
			return e
		}
		live := e == nil && o.Expires > now
		p := ReplicaProgress{Replica: a.Partition, Offset: o.Offset, Ready: live && o.Ready, Phase: o.Phase, Error: o.LastError}
		if o.Offset < task.Barrier {
			p.Lag = task.Barrier - o.Offset
		}
		task.Progress = append(task.Progress, p)
		anyOwner = anyOwner || (live && !containsReplica(task.Source, a.Partition))
		allLoaded = allLoaded && live && (o.Ready || o.Phase == "CatchingUp")
		if p.Error != "" {
			task.Blocked = "restore_failed"
		}
	}
	if !task.terminal() {
		switched := task.Stage == MigrationRetiring
		if !switched {
			if !ready {
				if allLoaded {
					task.transition(MigrationCatchingUp, now)
					task.Blocked = "waiting_for_log_barrier"
				} else if anyOwner {
					task.transition(MigrationRestoring, now)
					if task.Blocked == "" {
						task.Blocked = "waiting_for_snapshot_restore"
					}
				} else {
					task.Blocked = "waiting_for_worker"
				}
				if len(next.Desired) < 2 {
					task.Blocked = "insufficient_eligible_workers_or_capacity"
				}
			} else {
				// Fast restores may finish between polls. Record the verified milestones.
				if task.Stage == MigrationPending {
					task.transition(MigrationRestoring, now)
				}
				if task.Stage == MigrationRestoring {
					task.transition(MigrationCatchingUp, now)
				}
				// A source can reacquire a new epoch while the target restores.
				// Capture the epoch at cutover, not just at task creation, so an
				// old resource-release acknowledgement cannot retire its new graph.
				for _, a := range task.Source {
					if containsReplica(next.Serving, a.Partition) {
						continue
					}
					o, e := view.owner(ctx, a.Partition)
					if e != nil && !tikverr.IsErrNotFound(e) {
						return e
					}
					if e == nil {
						task.SourceEpochs[a.Partition] = o.Epoch
					}
				}
				task.transition(MigrationReady, now)
				task.transition(MigrationSwitching, now)
				task.transition(MigrationRetiring, now)
			}
		}
		if task.Stage == MigrationRetiring {
			retired := true
			for _, a := range task.Source {
				if containsReplica(next.Serving, a.Partition) {
					continue
				}
				epoch := task.SourceEpochs[a.Partition]
				if epoch == 0 {
					continue
				}
				_, e := tx.Get(ctx, c.retirementKey(a.Partition, epoch))
				if e == nil {
					continue
				}
				if !tikverr.IsErrNotFound(e) {
					return e
				}
				raw, e := tx.Get(ctx, c.nodeKey(a.Node))
				if e != nil && !tikverr.IsErrNotFound(e) {
					return e
				}
				if e == nil {
					var node ClusterNode
					if e = json.Unmarshal(raw, &node); e != nil {
						return e
					}
					if node.Session == a.Session && node.Expires > now {
						retired = false
					}
				}
			}
			if retired {
				task.transition(MigrationCompleted, now)
			} else {
				task.Blocked = "waiting_for_source_resource_release"
			}
		}
		if !task.terminal() && now-task.StageStartedAt >= 120000 {
			task.TimedOut = true
			task.Blocked = "stage_timeout: " + task.Blocked
		}
	}
	after, _ := json.Marshal(task)
	if old.TaskID != next.TaskID || string(before) != string(after) {
		task.UpdatedAt = now
		return putJSON(tx, c.taskKey(task.ID), task)
	}
	return nil
}

// Reports are fenced by both plan generation and owner epoch. A separate
// task fence conflicts with replacements/switches without contending with snapshot publication.
func (c *Coordinator) ReportReplica(ctx context.Context, id IndexPartitionID, version uint64, expected Ownership, phase, message string, offset uint64) error {
	if phase != "Restoring" && phase != "CatchingUp" && phase != "Ready" && phase != "Error" {
		return errors.New("invalid replica phase")
	}
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = c.reportReplicaIn(ctx, tx, id, version, expected, phase, message, offset); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (c *Coordinator) reportReplicaIn(ctx context.Context, tx transactionAPI, id IndexPartitionID, version uint64, expected Ownership, phase, message string, offset uint64) error {
	plan, e := c.planIn(ctx, tx, id)
	if e != nil {
		return e
	}
	if plan.Version != version || !containsReplica(plan.Serving, expected.Partition) {
		return ErrFenced
	}

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
	o.Phase = phase
	if len(message) > 1024 {
		message = message[:1024]
	}
	o.LastError = message
	if phase == "Error" && plan.TaskID != "" {
		task, e := c.taskIn(ctx, tx, plan.TaskID)
		if e != nil {
			return e
		}
		if !task.terminal() {
			task.LastError = message
			task.UpdatedAt = txnMillis(tx)
			if e = putJSON(tx, c.taskKey(task.ID), task); e != nil {
				return e
			}
		}
	}
	if e = putJSON(tx, c.ownerKey(o.Partition), o); e != nil {
		return e
	}
	guard, e := tx.Get(ctx, c.taskFenceKey(id))
	if e != nil {
		return e
	}
	if e = tx.Set(c.taskFenceKey(id), guard); e != nil {
		return e
	}
	return nil
}

var ErrTaskQuery = errors.New("invalid task query")

type TaskPage struct {
	Tasks []MigrationTask `json:"tasks"`
	Next  string          `json:"next,omitempty"`
}

func validTaskID(id string) bool {
	return id != "" && len(id) <= 128 && !strings.ContainsAny(id, "/\\\x00")
}
func (c *ClusterService) Task(ctx context.Context, id string) (MigrationTask, error) {
	if !validTaskID(id) {
		return MigrationTask{}, ErrTaskQuery
	}
	tx, e := c.root.begin()
	if e != nil {
		return MigrationTask{}, e
	}
	defer tx.Rollback()
	return c.coordinator.taskIn(ctx, tx, id)
}
func (c *ClusterService) Tasks(ctx context.Context, after string, limit int) (TaskPage, error) {
	out := TaskPage{Tasks: []MigrationTask{}}
	if limit < 1 || limit > 200 || (after != "" && !validTaskID(after)) {
		return out, ErrTaskQuery
	}
	tx, e := c.root.begin()
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	start := c.coordinator.taskKey("")
	if after != "" {
		start = append(c.coordinator.taskKey(after), 0)
	}
	it, e := tx.Iter(start, []byte(c.root.Prefix+"cluster/tasks0"))
	if e != nil {
		return out, e
	}
	defer it.Close()
	for it.Valid() {
		if e = ctx.Err(); e != nil {
			return out, e
		}
		if len(out.Tasks) == limit {
			out.Next = out.Tasks[len(out.Tasks)-1].ID
			break
		}
		var t MigrationTask
		if e = json.Unmarshal(it.Value(), &t); e != nil {
			return out, e
		}
		out.Tasks = append(out.Tasks, t)
		if e = it.Next(); e != nil {
			return out, e
		}
	}
	return out, nil
}
