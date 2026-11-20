package vector

import (
	"bytes"
	"context"
	"fmt"

	"example.com/vector-storage/internal/shardlog"
)

func (j graphJournal) stream() shardlog.Stream { return shardlog.Stream(j.key("entries/")) }

// Preserve a single MVCC snapshot across the log, its GC floor and data references.
func logReader(tx transactionAPI) shardlog.ReadBatch {
	return func(ctx context.Context, keys [][]byte) ([][]byte, error) {
		values, err := batchGet(ctx, tx, keys)
		if err != nil {
			return nil, err
		}
		rows := make([][]byte, len(keys))
		for i, key := range keys {
			rows[i] = values[string(key)]
		}
		return rows, nil
	}
}

// A dispatcher recovers both the source ACK and its WAL boundary in the same
// snapshot. In steady state the live owner already knows the committed head.
type dataLogReader struct {
	repo        *Repository
	tx          dataReader
	head        *uint64
	verifyFloor bool
}

func (r *Repository) dataLog(node *shardlog.Node, tx dataReader, head *uint64, verifyFloor bool) shardlog.Group[shardlog.Slot, LogBatch] {
	return shardlog.Open(node, slotWriter(dataWAL(r.Prefix), tx), dataLogReader{r, tx, head, verifyFloor})
}

func (reader dataLogReader) ReadCommitted(ctx context.Context, w shardlog.Window) (LogBatch, error) {
	if w.Limit < 1 || w.Limit > dispatchLookahead || w.ByteLimit < 0 {
		return LogBatch{}, fmt.Errorf("invalid log read window")
	}
	r, tx := reader.repo, reader.tx
	if tx == nil {
		var err error
		tx, err = r.beginDataRead(ctx)
		if err != nil {
			return LogBatch{}, err
		}
		defer tx.Rollback()
	}
	var result LogBatch
	var err error
	if reader.head == nil {
		result, err = r.readEventsIn(ctx, tx, w.After, w.Through, w.Limit, w.ByteLimit)
	} else {
		result, err = r.readEventsAtHead(ctx, tx, w.After, w.Through, *reader.head, w.Limit, w.ByteLimit)
	}
	if err != nil || !reader.verifyFloor || result.Head <= result.Floor {
		return result, err
	}
	first, err := tx.FirstKey(ctx, []byte(r.Prefix+"log/"), []byte(r.Prefix+"log0"))
	if err == nil && !bytes.Equal(first, r.logKey(result.Floor+1)) {
		err = fmt.Errorf("data log gap at retained lower bound %d", result.Floor+1)
	}
	return result, err
}

type graphLogReader struct {
	journal graphJournal
	tx      transactionAPI
}

func (r graphLogReader) ReadCommitted(ctx context.Context, w shardlog.Window) ([][]byte, error) {
	tx := r.tx
	if tx == nil {
		var err error
		tx, err = r.journal.c.root.begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	head, err := r.journal.headIn(ctx, tx)
	if err != nil {
		return nil, err
	}
	if w.After < head.Floor.Sequence {
		return nil, ErrLogCompacted
	}
	through := w.Through
	if through == 0 {
		through = head.Visible.Sequence
	}
	if through > head.Visible.Sequence || w.After > through {
		return nil, ErrInvalidLogCursor
	}
	return r.journal.stream().Tail(ctx, logReader(tx), w.After, through, w.Limit, true)
}
func (j graphJournal) log(node *shardlog.Node, tx transactionAPI) shardlog.Group[shardlog.Slot, [][]byte] {
	return shardlog.Open(node, slotWriter(j.stream(), tx), graphLogReader{j, tx})
}

// Token binds the graph base prepared by the caller to the publisher grant.
func (j graphJournal) proposeSlot(ctx context.Context, w *indexWriter, token []byte, native tikvTransaction, seq uint64, value []byte) error {
	w.mu.Lock()
	generation := w.lease.Generation
	matches := bytes.Equal(token, w.lease.token())
	w.mu.Unlock()
	if !matches || w.replicaLease == nil || !w.replicaLease.ActiveTerm(j.owner.Epoch) {
		return ErrFenced
	}
	return logStorageError(j.log(&w.leaderLease, native).Propose(ctx, generation, shardlog.Slot{Sequence: seq, Value: value}))
}

func slotWriter(stream shardlog.Stream, view dataReader) func(context.Context, shardlog.Slot) error {
	tx, ok := view.(transactionAPI)
	if !ok {
		return nil
	}
	native, ok := nativeTiKV(tx)
	if !ok {
		return nil
	}
	return stream.Writer(native.KVTxn)
}
