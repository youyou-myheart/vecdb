package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

type rawShardWriter struct {
	store rawShardStore
	owner Ownership
	mutationBatchState

	history *rawShardHistory
}

func newRawShardWriter(store rawShardStore, owner Ownership, history *rawShardHistory, count uint64) *rawShardWriter {
	w := &rawShardWriter{
		store: store, owner: owner, history: history,
		mutationBatchState: mutationBatchState{confirmed: history, number: count, end: history.end(), records: make(map[string]uint64), receipts: make(map[string]receipt)},
	}
	return w
}

// Each slot is an immutable KV pair. BatchPut may persist only some of
// its keys, so an error can confirm at most the contiguous readable prefix.
func (w *rawShardWriter) write(ctx context.Context, batches ...mutationLogBatch) (int, error) {
	keys := make([][]byte, len(batches))
	values := make([][]byte, len(batches))
	for i, batch := range batches {
		keys[i] = w.store.batchKey(w.owner.Epoch, batch.Number)
		encoded, err := json.Marshal(batch)
		if err != nil {
			return 0, err
		}
		values[i] = encoded
	}
	if err := w.store.client.BatchPut(ctx, keys, values); err != nil {
		stored, readErr := w.store.client.BatchGet(ctx, keys)
		if readErr != nil {
			return 0, fmt.Errorf("raw batch persistence not confirmed; retry same request_id: %w", err)
		}
		confirmed := 0
		for confirmed < len(stored) && confirmed < len(values) && bytes.Equal(stored[confirmed], values[confirmed]) {
			confirmed++
		}
		if confirmed != len(values) {
			return confirmed, fmt.Errorf("raw batch persistence not confirmed; retry same request_id: %w", err)
		}
	}
	return len(batches), nil
}

func (w *rawShardWriter) accept(batches ...mutationLogBatch) error {
	if err := w.history.extend(batches); err != nil {
		return err
	}
	// The drainer calls prepare and accept in order. A later pending write
	// may already have advanced the same record; preserve that projection.
	for _, batch := range batches {
		for _, e := range batch.Entries {
			delete(w.receipts, e.RequestID)
			if w.records[e.Record.ID] == e.Record.Revision {
				delete(w.records, e.Record.ID)
			}
		}
	}
	return nil
}
