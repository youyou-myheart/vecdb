package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// Progress reads never open a data snapshot or reconstruct WAL history.
// The counters are meaningful only while this recovered writer holds its lease.
func (b *mutationGroup) dataProgress() (DataProgress, error) {
	l := b.lease
	var before, after uint64
	if l == nil || !l.InLeaderLease(&before) || before != l.owner.Epoch {
		return DataProgress{}, ErrFenced
	}
	// A floor is published after completion, and completion after the head.
	// Read in that order so concurrent appends cannot produce an inverted span.
	d := DataProgress{Floor: l.floor.Load(), Owner: l.owner}
	d.Completed = max(l.indexed.Load(), l.owner.Offset)
	d.Head = l.head.Load()
	if !l.InLeaderLease(&after) || after != before {
		return DataProgress{}, ErrFenced
	}
	if d.Floor > d.Completed || d.Completed > d.Head {
		return DataProgress{}, ErrInvalidLogCursor
	}
	d.Known, d.Fresh, d.OwnerAvailable = true, true, true
	d.ObservedEpoch, d.ObservedAtMS = before, time.Now().UnixMilli()
	return d, nil
}

func (c *ClusterService) dataProgress(ctx context.Context) (map[string]DataProgress, error) {
	ids := make([]DataPartitionID, 0, len(c.data))
	keys := make([][]byte, 0, len(c.data))
	for id := range c.data {
		ids = append(ids, id)
		keys = append(keys, c.coordinator.ownerKey(sourceID(id)))
	}
	// Only small ownership records are read from TiKV. One metadata snapshot
	// avoids a timestamp request and log scan for every data partition.
	tx, err := c.root.begin()
	if err != nil {
		return nil, err
	}
	values, err := batchGet(ctx, tx, keys)
	now := txnMillis(tx)
	_ = tx.Rollback()
	if err != nil {
		return nil, err
	}
	owners := make([]Ownership, len(ids))
	for i, key := range keys {
		if value := values[string(key)]; value != nil {
			if err := json.Unmarshal(value, &owners[i]); err != nil {
				return nil, err
			}
			if owners[i].Partition != sourceID(ids[i]) {
				return nil, fmt.Errorf("data progress ownership key mismatch")
			}
		}
	}
	results := make([]DataProgress, len(ids))
	// A stopped Worker must not hold the entire status request for its lease
	// duration. Bound the whole probe phase, including queued RPCs.
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var tasks errgroup.Group
	tasks.SetLimit(8)
	for i, id := range ids {
		tasks.Go(func() error {
			owner := owners[i]
			c.progressMu.Lock()
			d := c.progressCache[id]
			c.progressMu.Unlock()
			d.Owner, d.Fresh, d.OwnerAvailable = owner, false, false
			if owner.Ready && !owner.Released && owner.Expires > now {
				reply, err := c.sendPartition(probeCtx, owner, partitionRPC{
					Namespace: c.root.Prefix, Partition: owner.Partition, Source: &id,
					Session: owner.Session, Epoch: owner.Epoch, Operation: "data_progress",
				})
				if p := reply.DataProgress; err == nil && p != nil && p.Known && p.Fresh &&
					p.OwnerAvailable && sameOwner(p.Owner, owner) && p.ObservedEpoch == owner.Epoch &&
					p.ObservedAtMS > 0 && p.Floor <= p.Completed && p.Completed <= p.Head {
					d = *p
					// The route has the latest renewal/error metadata; the counters
					// above were sampled from this same owner's live memory.
					d.Owner = owner
					c.progressMu.Lock()
					if c.progressCache == nil {
						c.progressCache = make(map[DataPartitionID]DataProgress)
					}
					old := c.progressCache[id]
					if !old.Known || d.ObservedEpoch > old.ObservedEpoch ||
						d.ObservedEpoch == old.ObservedEpoch && d.Head >= old.Head && d.Completed >= old.Completed && d.Floor >= old.Floor {
						c.progressCache[id] = d
					}
					c.progressMu.Unlock()
				}
			}
			results[i] = d
			return nil
		})
	}
	_ = tasks.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make(map[string]DataProgress, len(ids))
	for i, id := range ids {
		out[string(sourceID(id))] = results[i]
	}
	return out, nil
}
