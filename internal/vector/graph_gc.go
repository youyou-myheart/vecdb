package vector

import (
	"context"
	"errors"
	"fmt"
)

// Deletion is bounded, snapshot-certified and serialized against consumer lease
// acquisition and leader replacement. Objects are retired in the SAME commit.
func (j graphJournal) Prune(ctx context.Context, r *Repository, limit int) (GraphPosition, error) {
	if limit < 1 || limit > LogBatchSize {
		return GraphPosition{}, ErrInvalidSubscription
	}
	tx, e := r.begin()
	if e != nil {
		return GraphPosition{}, e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return GraphPosition{}, e
	}
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return h.Floor, e
	}
	if h.Checkpoint == nil {
		return h.Floor, nil
	}
	end := h.CheckpointPosition.Sequence
	if end > h.Visible.Sequence || end < h.Floor.Sequence {
		return h.Floor, errors.New("invalid graph checkpoint retention")
	}
	end, e = r.retainConsumers(ctx, tx, "graph", end)
	if e != nil {
		return h.Floor, e
	}
	if end < h.Floor.Sequence {
		return h.Floor, errors.New("consumer below graph floor")
	}
	if end-h.Floor.Sequence > uint64(limit) {
		end = h.Floor.Sequence + uint64(limit)
	}
	pos := h.Floor
	for n := pos.Sequence; n < end; {
		n++
		key := j.key(fmt.Sprintf("entries/%020d", n))
		var entry GraphEntry
		if e = readJSON(ctx, tx, key, &entry); e != nil {
			return h.Floor, e
		}
		if entry.Previous != pos || entry.Position.Sequence != n || entryHash(entry) != entry.Position.Hash {
			return h.Floor, errors.New("graph GC chain mismatch")
		}
		if e = j.retireObject(ctx, tx, r, entry.Object); e != nil {
			return h.Floor, e
		}
		if e = tx.Delete(key); e != nil {
			return h.Floor, e
		}
		pos = entry.Position
	}
	if pos.Sequence == h.CheckpointPosition.Sequence && pos != h.CheckpointPosition {
		return h.Floor, errors.New("checkpoint hash differs from compacted prefix")
	}
	if pos == h.Floor {
		return pos, nil
	}
	h.Floor = pos
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return pos, e
	}
	return pos, tx.Commit(ctx)
}
