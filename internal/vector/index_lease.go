package vector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"example.com/vector-storage/internal/lease"
	tikverr "github.com/tikv/client-go/v2/error"
)

// The publisher lease is distinct from a replica's liveness lease. A ready
// replica may remain available for queries while its publisher lease drains.
type indexLease struct {
	Owner      Ownership     `json:"owner"`
	Generation uint64        `json:"generation"`
	Expires    int64         `json:"expires_ms"`
	remaining  time.Duration // Grant snapshot duration; never persisted.
}

func (c *Coordinator) indexLeaseKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/index-leases/" + string(id))
}
func (c *Coordinator) indexGuardKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/index-guards/" + string(id))
}
func (l indexLease) token() []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%d/%d", l.Owner.Partition, l.Owner.Node, l.Owner.Session, l.Owner.Epoch, l.Generation))
}

// A failed/unknown grant does not extend the local lease. The deadline starts
// BEFORE requesting the storage timestamp, not when the reply arrives.
func (c *Coordinator) grantIndexLease(ctx context.Context, id IndexPartitionID, owner Ownership) (indexLease, error) {
	tx, err := c.root.begin()
	if err != nil {
		return indexLease{}, err
	}
	defer tx.Rollback()
	current, err := c.fenceOwner(ctx, tx, owner)
	if err != nil {
		return indexLease{}, err
	}
	plan, err := c.planIn(ctx, tx, id)
	if err != nil {
		return indexLease{}, err
	}
	if plan.Publisher != owner.Partition || plan.LeaseDraining {
		return indexLease{}, ErrFenced
	}
	// Serialize renewal with the Master's decision to drain/switch the publisher.
	if err = putJSON(tx, c.planKey(id), plan); err != nil {
		return indexLease{}, err
	}
	var lease indexLease
	err = readJSON(ctx, tx, c.indexLeaseKey(id), &lease)
	if err != nil && !tikverr.IsErrNotFound(err) {
		return lease, err
	}
	now := txnMillis(tx)
	fresh := lease.Expires <= now || !sameOwner(lease.Owner, owner)
	if lease.Expires > now && !sameOwner(lease.Owner, owner) {
		return lease, ErrFenced
	}
	if fresh {
		if lease.Generation == ^uint64(0) {
			return lease, errors.New("index lease generation exhausted")
		}
		lease.Generation++
		lease.Owner = owner
		if err = tx.Set(c.indexGuardKey(id), lease.token()); err != nil {
			return lease, err
		}
	}
	lease.Expires = min(current.Expires, now+c.ttl)
	lease.remaining = time.Duration(max(int64(0), lease.Expires-now)) * time.Millisecond
	if err = putJSON(tx, c.indexLeaseKey(id), lease); err != nil {
		return lease, err
	}
	return lease, tx.Commit(ctx)
}

// Called in the plan transaction. Even if an owner is released or its node
// disappears, an issued publisher lease cannot be revoked early. A switch also
// changes the storage guard, fencing requests sent before expiry but committed
// after handoff. Reading a lease alone would not serialize a concurrent renewal.
func (c *Coordinator) protectPublisherLease(ctx context.Context, tx transactionAPI, id IndexPartitionID, old ReplicaPlan, next *ReplicaPlan) (bool, error) {
	if !c.leaseReads {
		return false, nil
	}
	var lease indexLease
	err := readJSON(ctx, tx, c.indexLeaseKey(id), &lease)
	if tikverr.IsErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if lease.Expires > txnMillis(tx) {
		if lease.Owner.Partition != old.Publisher {
			return false, errors.New("publisher lease/plan mismatch")
		}
		draining := next.Publisher != old.Publisher
		next.Publisher = old.Publisher
		next.LeaseDraining = draining
		if !containsReplica(next.Serving, old.Publisher) {
			for _, a := range old.Serving {
				if a.Partition == old.Publisher {
					next.Serving = append(next.Serving, a)
					break
				}
			}
		}
		if !containsReplica(next.Serving, old.Publisher) {
			return false, errors.New("leased publisher absent from serving plan")
		}
		return draining, nil
	}
	if next.Publisher != old.Publisher {
		// Touch the lease too: an expired snapshot cannot switch past a concurrent
		// successful renewal, nor can an old renewal restore the revoked guard.
		lease.Expires = 0
		if err = putJSON(tx, c.indexLeaseKey(id), lease); err != nil {
			return false, err
		}
		if err = tx.Set(c.indexGuardKey(id), []byte("revoked")); err != nil {
			return false, err
		}
	}
	return false, nil
}

// Cached authority is an optimization; the write guard remains a storage-side
// check. It prevents late writes after process suspension or clock anomalies.
// It is NOT the owner, plan or graph head, and renewals do not touch it.
func (c *Coordinator) fenceIndexGuard(ctx context.Context, tx transactionAPI, id IndexPartitionID, token []byte) error {
	b, err := tx.Get(ctx, c.indexGuardKey(id))
	if err != nil {
		return err
	}
	if string(b) != string(token) {
		return ErrFenced
	}
	return tx.Set(c.indexGuardKey(id), b)
}

func (w *indexWriter) adoptGrantLocked(grant indexLease) {
	if w.lease.Generation != grant.Generation {
		w.known = false
		w.controls = nil
	}
	w.lease = grant
}

// The replica control tick campaigns for a publisher lease when the durable
// plan designates this replica. The plan transaction still enforces handoff.
func (g *graphReplica) refreshLease(ctx context.Context, plan ReplicaPlan) (err error) {
	w := g.writer
	if w == nil {
		return nil
	}
	owner := g.journal.owner
	w.mu.Lock()
	oldPublisher, oldRoute := w.publisher, w.publisherRoute
	w.publisher = plan.Publisher
	w.publisherRoute = Assignment{}
	for _, a := range plan.Serving {
		if a.Partition == plan.Publisher {
			w.publisherRoute = a
			break
		}
	}
	generation := w.lease.Generation
	changed := oldPublisher != w.publisher || oldRoute != w.publisherRoute
	w.mu.Unlock()
	if changed {
		g.input.notify()
	}
	if !w.replicaLive() || plan.Publisher != owner.Partition || plan.LeaseDraining {
		return nil
	}
	if grant, live := w.leaderLease.Snapshot(); live && grant.Epoch == generation && time.Until(grant.Deadline) > time.Duration(g.journal.c.ttl)*time.Millisecond/2 {
		return nil
	}
	defer g.track("index_lease_renew")(&err)
	var grant indexLease
	group := g.journal.log(&w.leaderLease, nil)
	err = group.Campaign(ctx, func(ctx context.Context) (lease.Grant, time.Time, error) {
		var start time.Time
		var err error
		grant, start, err = lease.Refresh(ctx, func(ctx context.Context) (indexLease, error) {
			return g.journal.c.grantIndexLease(ctx, g.journal.id, owner)
		})
		return lease.Grant{Epoch: grant.Generation, Expires: grant.Expires, Remaining: grant.remaining}, start, err
	})
	if err != nil {
		return logStorageError(err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lease.Generation != grant.Generation {
		// Old-token operations reject the newly confirmed generation until
		// the old append exits and recovery can install its base.
		if !w.op.TryLock() {
			return ErrConflict
		}
		defer w.op.Unlock()
		defer g.input.notify()
	}
	w.adoptGrantLocked(grant)
	return nil
}

// Recovery must establish a serialization point even when the owner/term did
// not change. An earlier unknown commit may still be in flight. Merely reading
// the head could install a stale cache; writing the guard in this transaction
// makes either that earlier append or this recovery retry from durable state.
func (j graphJournal) claimLeased(ctx context.Context, token []byte) (GraphHead, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return GraphHead{}, err
	}
	defer tx.Rollback()
	if err = j.c.fenceIndexGuard(ctx, tx, j.id, token); err != nil {
		return GraphHead{}, err
	}
	if err = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); err != nil {
		return GraphHead{}, err
	}
	h, err := j.headIn(ctx, tx)
	if err != nil {
		return h, err
	}
	if !sameOwner(h.Leader, j.owner) {
		if h.Term == ^uint64(0) {
			return h, errors.New("graph term exhausted")
		}
		h.Term++
		h.Leader = j.owner
		if err = putJSON(tx, j.key("head"), h); err != nil {
			return h, err
		}
	}
	if _, bounded := j.nativeAppend(tx); bounded && h.Committed.Sequence != ^uint64(0) {
		// Conflict with any unconfirmed append from this same generation.
		// The next slot is absent at this recovery snapshot; its tombstone
		// fences an older prewrite even when the publisher lease did not change.
		if err = tx.Delete(j.stream().Key(h.Committed.Sequence + 1)); err != nil {
			return h, err
		}
	}
	return h, tx.Commit(ctx)
}
