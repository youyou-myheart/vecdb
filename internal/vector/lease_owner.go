package vector

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"example.com/vector-storage/internal/lease"
	tikverr "github.com/tikv/client-go/v2/error"
)

// campaignOwner runs an election step inside a background control loop. A new
// owner prepares the component before advertising it as ready. An existing
// owner only renews; the caller must retire its component before campaigning
// for a new epoch. Only a confirmed storage result can be published locally.
func (c *Coordinator) campaignOwner(ctx context.Context, node ClusterNode, id IndexPartitionID, current Ownership, prepare func(Ownership) error, report func(*Ownership)) (Ownership, time.Time, error) {
	var held *Ownership
	if current.Epoch != 0 {
		if current.Partition != id || current.Node != node.ID || current.Session != node.Session {
			return Ownership{}, time.Time{}, ErrFenced
		}
		held = &current
	}
	election := lease.Election[Ownership]{
		Acquire: func(ctx context.Context) (Ownership, bool, error) {
			owner, err := c.Acquire(ctx, id, node)
			return owner, owner.Ready && report == nil, err
		},
		Renew: func(ctx context.Context, owner Ownership) (Ownership, error) {
			return c.update(ctx, owner, true, false, report)
		},
		Release: func(ctx context.Context, owner Ownership, err error) {
			_ = c.releaseOwner(ctx, owner, func(o *Ownership) { o.LastError = err.Error() })
		},
	}
	return election.Step(ctx, held, prepare)
}

func (c *Coordinator) renewOwner(ctx context.Context, owner Ownership, ready bool) (Ownership, time.Time, error) {
	return lease.Refresh(ctx, func(ctx context.Context) (Ownership, error) {
		return c.update(ctx, owner, ready, false, nil)
	})
}

func (c *Coordinator) releaseOwner(ctx context.Context, owner Ownership, report func(*Ownership)) error {
	if owner.Epoch == 0 {
		return nil
	}
	_, err := c.update(ctx, owner, false, true, report)
	return err
}

func (c *Coordinator) ownerKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/owners/" + string(id))
}

func (c *Coordinator) fenceKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/fences/" + string(id))
}

func (c *Coordinator) writeGuardKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/write-epochs/" + string(id))
}

func epochToken(epoch uint64) []byte { return strconv.AppendUint(nil, epoch, 10) }

func (c *Coordinator) readOwner(ctx context.Context, tx transactionAPI, id IndexPartitionID) (Ownership, error) {
	b, e := tx.Get(ctx, c.ownerKey(id))
	if e != nil {
		return Ownership{}, e
	}
	var o Ownership
	e = json.Unmarshal(b, &o)
	if e == nil && o.Partition != id {
		e = errors.New("ownership key mismatch")
	}
	return o, e
}
func (c *Coordinator) Acquire(ctx context.Context, id IndexPartitionID, node ClusterNode) (Ownership, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Ownership{}, e
	}
	defer tx.Rollback()
	now := txnMillis(tx)
	b, e := tx.Get(ctx, c.nodeKey(node.ID))
	if e != nil {
		return Ownership{}, e
	}
	var registered ClusterNode
	if e = json.Unmarshal(b, &registered); e != nil {
		return Ownership{}, e
	}
	if registered.Session != node.Session || registered.Expires <= now {
		return Ownership{}, ErrFenced
	}
	if id == masterLeaseID {
		if !hasRole(registered, RoleMaster) {
			return Ownership{}, ErrFenced
		}
	} else {
		if !hasRole(registered, RoleWorker) {
			return Ownership{}, ErrFenced
		}
		assignment, err := c.assignmentIn(ctx, tx, id)
		if err != nil {
			return Ownership{}, err
		}
		if assignment.Node != node.ID || assignment.Session != node.Session {
			return Ownership{}, ErrFenced
		}
		// Conflict with a concurrent reassignment rather than claiming from stale intent.
		if err = putJSON(tx, c.assignmentKey(id), assignment); err != nil {
			return Ownership{}, err
		}
	}
	old, e := c.readOwner(ctx, tx, id)
	if e != nil && !tikverr.IsErrNotFound(e) {
		return old, e
	}
	if e == nil && old.Expires > now {
		return old, ErrConflict
	}
	if old.Epoch == ^uint64(0) {
		return old, errors.New("ownership epoch exhausted")
	}
	o := Ownership{WriteEndpoint: registered.WriteEndpoint, Ready: id == masterLeaseID, Partition: id, Node: node.ID, Session: node.Session, URL: strings.TrimRight(node.URL, "/"), Epoch: old.Epoch + 1, Expires: now + c.ttl}
	if c.root.packed != nil && strings.HasPrefix(string(id), "data-") {
		// This prefix was confirmed by every destination and reported in
		// an existing lease renewal. It is safe to resume after takeover.
		o.Offset = old.Offset
	}
	if e = putJSON(tx, c.ownerKey(id), o); e != nil {
		return o, e
	}
	if e = putJSON(tx, c.fenceKey(id), o); e != nil {
		return o, e
	}
	if c.root.raw == nil && c.root.packed == nil && strings.HasPrefix(string(id), "data-") {
		if e = tx.Set(c.writeGuardKey(id), epochToken(o.Epoch)); e != nil {
			return o, e
		}
	}
	return o, tx.Commit(ctx)
}
func sameOwner(a, b Ownership) bool {
	return a.Partition == b.Partition && a.Node == b.Node && a.Session == b.Session && a.Epoch == b.Epoch
}
func (c *Coordinator) Update(ctx context.Context, expected Ownership, ready, release bool) (Ownership, error) {
	return c.update(ctx, expected, ready, release, nil)
}
func (c *Coordinator) update(ctx context.Context, expected Ownership, ready, release bool, report func(*Ownership)) (Ownership, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Ownership{}, e
	}
	defer tx.Rollback()
	o, e := c.readOwner(ctx, tx, expected.Partition)
	if e != nil {
		return o, e
	}
	now := txnMillis(tx)
	if !sameOwner(o, expected) || o.Released || o.Expires <= now {
		return o, ErrFenced
	}
	if expected.Partition != masterLeaseID && !release {
		assignment, err := c.assignmentIn(ctx, tx, expected.Partition)
		if err != nil {
			return o, err
		}
		if assignment.Node != expected.Node || assignment.Session != expected.Session {
			return o, ErrFenced
		}
	}
	if report != nil {
		report(&o)
	}
	o.Ready = o.Ready || ready
	if release {
		o.Ready = false
		o.Released = true
		// Publishing an unavailable route must not make an issued lease
		// expire early. A successor still checks the old expiry in Acquire.
		if !c.holdReleasedLeases {
			o.Expires = 0
		}
	} else {
		o.Expires = now + c.ttl
	}
	if e = putJSON(tx, c.ownerKey(o.Partition), o); e != nil {
		return o, e
	}
	if release {
		if c.root.raw == nil && c.root.packed == nil && strings.HasPrefix(string(o.Partition), "data-") {
			if e = tx.Set(c.writeGuardKey(o.Partition), epochToken(0)); e != nil {
				return o, e
			}
		}
		if e = putJSON(tx, c.fenceKey(o.Partition), o); e != nil {
			return o, e
		}
	}
	return o, tx.Commit(ctx)
}
func (c *Coordinator) Resolve(ctx context.Context, id IndexPartitionID) (Ownership, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Ownership{}, e
	}
	defer tx.Rollback()
	return c.resolveIn(ctx, tx, id)
}

func (c *Coordinator) resolveIn(ctx context.Context, tx transactionAPI, id IndexPartitionID) (Ownership, error) {
	o, e := c.readOwner(ctx, tx, id)
	if e != nil {
		return o, e
	}
	if o.Expires <= txnMillis(tx) || !o.Ready {
		return o, ErrFenced
	}
	return o, nil
}
func (c *Coordinator) Check(ctx context.Context, expected Ownership) error {
	current, e := c.Resolve(ctx, expected.Partition)
	if e != nil {
		return e
	}
	if !sameOwner(current, expected) {
		return ErrFenced
	}
	return nil
}

// Fence changes on acquisition/release, independently of renewal. Mutations
// write it transactionally so takeover conflicts with in-flight publications.
func (c *Coordinator) Fence(expected Ownership) func(context.Context, transactionAPI) error {
	return func(ctx context.Context, tx transactionAPI) error {
		_, err := c.fenceOwner(ctx, tx, expected)
		return err
	}
}

// Return the grant verified at this transaction's timestamp so a dependent
// lease can use its expiry without another KV read.
func (c *Coordinator) liveOwner(ctx context.Context, tx transactionAPI, expected Ownership) (Ownership, error) {
	o, err := c.readOwner(ctx, tx, expected.Partition)
	if err != nil {
		return o, err
	}
	if !sameOwner(o, expected) || !o.Ready || o.Expires <= txnMillis(tx) {
		return o, ErrFenced
	}
	return o, nil
}

func (c *Coordinator) fenceOwner(ctx context.Context, tx transactionAPI, expected Ownership) (Ownership, error) {
	o, err := c.liveOwner(ctx, tx, expected)
	if err != nil {
		return o, err
	}
	b, err := tx.Get(ctx, c.fenceKey(o.Partition))
	if err != nil {
		return o, err
	}
	var token Ownership
	if err = json.Unmarshal(b, &token); err != nil {
		return o, err
	}
	if !sameOwner(token, expected) {
		return o, ErrFenced
	}
	return o, tx.Set(c.fenceKey(o.Partition), b)
}
