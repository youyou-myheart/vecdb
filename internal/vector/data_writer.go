package vector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// Recovery happens during acquisition, before the new owner accepts requests.
func (c *ClusterService) recoverWriter(ctx context.Context, id DataPartitionID, o Ownership) (_ *mutationGroup, err error) {
	defer c.observations.pipeline.track("data_writer_recover", string(o.Partition))(&err)
	repo := c.data[id]
	lease := &appendLease{owner: o, guard: c.coordinator.writeGuardKey(o.Partition), token: epochToken(o.Epoch),
		boundCommitTS: c.coordinator.holdReleasedLeases}
	group := newMutationGroup(c.runCtx, repo, &c.observations.pipeline, string(o.Partition))
	group.lease = lease
	group.onCommit = func() { c.signalSource(id, "write", true) }
	if repo.raw != nil {
		writer, err := repo.raw.recover(ctx, repo.Prefix, o)
		if err != nil {
			return nil, err
		}
		lease.head.Store(writer.history.end())
		group.rawWriter = writer
		return group, nil
	}
	// A TxnKV snapshot after acquisition observes earlier commits and locks.
	tx, err := repo.begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	head, err := repo.logHead(ctx, tx)
	if err != nil {
		return nil, err
	}
	lease.head.Store(head)
	return group, nil
}
func (c *ClusterService) retireWriter(id DataPartitionID) {
	c.writerMu.Lock()
	group := c.writers[id]
	delete(c.writers, id)
	c.writerMu.Unlock()
	if group != nil {
		group.close()
		if group.rawWriter != nil {
			group.repo.raw.local.CompareAndDelete(group.repo.Prefix, group)
		}
	}
}

// ResolveWriteOwner returns the authoritative ready incarnation. SDKs cache
// this by data partition and discard it when the endpoint or epoch changes.
func (c *ClusterService) ResolveWriteOwner(ctx context.Context, id string) (DataPartitionID, Ownership, error) {
	d := c.router.DataForID(id)
	if c.layout.Routing == nil {
		return d, Ownership{}, nil
	}
	o, err := c.coordinator.Resolve(ctx, sourceID(d))
	if err != nil {
		return d, o, fmt.Errorf("%w: %w", ErrPartitionUnavailable, err)
	}
	return d, o, nil
}
func (c *ClusterService) MutateOwned(ctx context.Context, namespace string, d DataPartitionID, epoch uint64, session, id string, m Mutation, deleted bool) (_ Record, err error) {
	if namespace != c.root.Prefix || d != c.router.DataForID(id) {
		return Record{}, ErrFenced
	}
	defer c.observations.pipeline.track("data_owner_mutate", string(sourceID(d)))(&err)
	group, err := c.localDataWriter(d, epoch, session)
	if err != nil {
		return Record{}, err
	}
	record, err := group.mutate(ctx, id, m, deleted)
	if err != nil {
		return Record{}, err
	}
	var after uint64
	if !group.lease.InLeaderLease(&after) || after != epoch {
		// The write may have persisted. A retry on the new owner uses the
		// same request ID and its receipt.
		return Record{}, ErrFenced
	}
	return record, nil
}

func (c *ClusterService) localDataWriter(d DataPartitionID, epoch uint64, session string) (*mutationGroup, error) {
	c.writerMu.RLock()
	group := c.writers[d]
	c.writerMu.RUnlock()
	var current uint64
	if group == nil || group.lease.owner.Session != session || !group.lease.InLeaderLease(&current) || current != epoch {
		return nil, ErrFenced
	}
	return group, nil
}
func (c *ClusterService) WriteNamespace() string { return c.root.Prefix }
func (c *ClusterService) routeMutation(ctx context.Context, d DataPartitionID, id string, m Mutation, deleted bool) (Record, error) {
	c.writerMu.RLock()
	o := c.writeRoutes[d]
	c.writerMu.RUnlock()
	// A bounded wait covers normal ownership renewal or failover.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		if o.Epoch == 0 {
			_, owner, err := c.ResolveWriteOwner(ctx, id)
			if err != nil {
				if !waitIndexRetry(ctx, 50*time.Millisecond) {
					return Record{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, ctx.Err())
				}
				continue
			}
			o = owner
			c.writerMu.Lock()
			c.writeRoutes[d] = o
			c.writerMu.Unlock()
		}
		reply, err := c.sendPartition(ctx, o, partitionRPC{Namespace: c.root.Prefix, Partition: o.Partition, Source: &d, Epoch: o.Epoch, Session: o.Session, Operation: "mutate", ID: id, Mutation: &m, Deleted: deleted})
		if err == nil {
			if reply.Record == nil {
				return Record{}, errors.New("missing record in data writer ACK")
			}
			return *reply.Record, nil
		}
		if errors.Is(err, ErrConflict) {
			return Record{}, err
		}
		c.writerMu.Lock()
		if sameOwner(c.writeRoutes[d], o) {
			delete(c.writeRoutes, d)
		}
		c.writerMu.Unlock()
		// Dial failures occur before a request can reach the previous owner.
		var networkErr *net.OpError
		dialFailed := errors.As(err, &networkErr) && networkErr.Op == "dial"
		if !errors.Is(err, ErrFenced) && !dialFailed {
			return Record{}, err
		}
		o = Ownership{}
		if !waitIndexRetry(ctx, 10*time.Millisecond) {
			return Record{}, ctx.Err()
		}
	}
}
