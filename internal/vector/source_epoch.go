package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
	"maps"
)

const sourceEpochEntry = "source_epoch"

// A rejection reports durable Index-local state, never a process-local guess.
type SourceEpochError struct {
	Source    DataPartitionID `json:"source"`
	Epoch     uint64          `json:"epoch"`
	Committed uint64          `json:"commit_lsn"`
}

func (e *SourceEpochError) Error() string {
	return fmt.Sprintf("source %d epoch fenced: accepted=%d commit_lsn=%d", e.Source, e.Epoch, e.Committed)
}
func (e *SourceEpochError) Unwrap() error { return ErrFenced }

func checkSourceEpoch(h GraphHead, q Delivery) error {
	key := fmt.Sprint(q.Source)
	if q.Owner.Epoch == 0 || q.Owner.Epoch != h.SourceEpochs[key] {
		return &SourceEpochError{q.Source, h.SourceEpochs[key], h.Sources[key]}
	}
	return nil
}

// Admission may advance an epoch even for an already committed batch. Only an
// issued, currently live owner can introduce a higher epoch. Once accepted,
// ordinary deliveries depend on this Index's epoch, not the source lease.
// A leased writer orders epoch barriers and deltas in the same journal.
func (j graphJournal) admitSource(ctx context.Context, c *ClusterService, q Delivery) (head GraphHead, err error) {
	defer c.observations.pipeline.track("source_epoch_admission", string(j.id))(&err)
	err = retryIndexCommit(ctx, func() error {
		var e error
		head, e = j.admitSourceOnce(ctx, c, q)
		return e
	}, func() {})
	return head, err
}

func (j graphJournal) admitSourceOnce(ctx context.Context, c *ClusterService, q Delivery) (GraphHead, error) {
	return j.admitSourceTransaction(ctx, c, q, nil, nil, nil)
}
func (j graphJournal) admitSourceCached(ctx context.Context, c *ClusterService, q Delivery, h GraphHead, token []byte, writer *indexWriter) (GraphHead, error) {
	return j.admitSourceTransaction(ctx, c, q, &h, token, writer)
}
func (j graphJournal) admitSourceTransaction(ctx context.Context, c *ClusterService, q Delivery, cached *GraphHead, token []byte, writer *indexWriter) (GraphHead, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return GraphHead{}, err
	}
	defer tx.Rollback()
	if native, ok := tx.(interface{ SetEnable1PC(bool) }); ok {
		native.SetEnable1PC(true)
	}
	native, oneSlot := j.nativeAppend(tx)
	oneSlot = oneSlot && cached != nil && writer != nil
	var h GraphHead
	if cached == nil {
		if err = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); err != nil {
			return h, err
		}
		h, err = j.headIn(ctx, tx)
	} else {
		h = cloneGraphHead(*cached)
		err = writer.authority(token)
		if err == nil && !oneSlot {
			err = tx.Set(j.c.indexGuardKey(j.id), token)
		}
	}
	if err != nil {
		return h, err
	}
	key := fmt.Sprint(q.Source)
	if q.Owner.Epoch < h.SourceEpochs[key] {
		return h, &SourceEpochError{q.Source, h.SourceEpochs[key], h.Sources[key]}
	}
	if err = c.validateDeliveryBounds(ctx, tx, j.id, q, h.Sources[key]); err != nil {
		return h, err
	}
	if q.Through > h.Sources[key] {
		if err = c.validateDeliveryEvents(ctx, tx, j.id, q); err != nil {
			return h, err
		}
	}
	if q.Owner.Epoch == h.SourceEpochs[key] {
		return h, nil
	}
	// Read-only authorization. A later global takeover does not invalidate this
	// barrier: each Index rejects old delivery only after observing a new epoch.
	owner, err := j.c.readOwner(ctx, tx, q.Owner.Partition)
	if err != nil {
		return h, err
	}
	if !sameOwner(owner, q.Owner) || !owner.Ready || owner.Expires <= txnMillis(tx) {
		return h, ErrFenced
	}
	if !sameOwner(h.Leader, j.owner) {
		if oneSlot {
			return h, ErrFenced
		}
		if h.Term == ^uint64(0) {
			return h, errors.New("graph term exhausted")
		}
		h.Term++
		h.Leader = j.owner
		if err = putJSON(tx, j.key("head"), h); err != nil {
			return h, err
		}
	}
	if h.Committed.Sequence == ^uint64(0) {
		return h, errors.New("graph sequence exhausted")
	}
	epochs := maps.Clone(h.SourceEpochs)
	if epochs == nil {
		epochs = map[string]uint64{}
	}
	epochs[key] = q.Owner.Epoch
	encoded, err := json.Marshal(epochs)
	if err != nil {
		return h, err
	}
	if uint64(len(encoded)) > ^uint64(0)-h.LogBytes {
		return h, errors.New("graph byte counter exhausted")
	}
	e := GraphEntry{Kind: sourceEpochEntry, Term: h.Term, Previous: h.Committed,
		Position: GraphPosition{Sequence: h.Committed.Sequence + 1, DataOffset: h.Committed.DataOffset},
		Sources:  maps.Clone(h.Sources), SourceEpochs: epochs, LogBytes: h.LogBytes + uint64(len(encoded)), CreatedMS: txnMillis(tx)}
	e.Position.Hash = entryHash(e)
	keyBytes := j.stream().Key(e.Position.Sequence)
	if oneSlot {
		raw, err := json.Marshal(e)
		if err != nil {
			return h, err
		}
		if err = j.proposeSlot(ctx, writer, token, native, e.Position.Sequence, raw); err != nil {
			return h, err
		}
	} else {
		if _, err = tx.Get(ctx, keyBytes); err == nil {
			return h, ErrConflict
		} else if !tikverr.IsErrNotFound(err) {
			return h, err
		}
		if err = putJSON(tx, keyBytes, e); err != nil {
			return h, err
		}
		if writer != nil {
			if err = writer.authority(token); err != nil {
				return h, err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return h, err
		}
	}
	h.Committed, h.Visible, h.SourceEpochs, h.LogBytes = e.Position, e.Position, epochs, e.LogBytes
	c.observations.pipeline.init()
	c.observations.pipeline.sourceEpochAdvances.WithLabelValues(string(j.id), key).Inc()
	return h, nil
}

func (g *graphReplica) observeSourceEpochRejection(phase string, err error) {
	var rejected *SourceEpochError
	if g.input != nil && errors.As(err, &rejected) {
		p := &g.input.cluster.observations.pipeline
		p.init()
		p.sourceEpochRejections.WithLabelValues(string(g.journal.id), phase).Inc()
	}
}
