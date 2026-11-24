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
		mutationBatchState: mutationBatchState{number: count, end: history.end(), records: make(map[string]uint64, len(history.records)), receipts: make(map[string]receipt, len(history.receipts))},
	}
	for _, entry := range history.entries {
		w.records[entry.Record.ID] = entry.Record.Revision
		w.receipts[entry.RequestID] = receipt{Digest: entry.Digest, Record: entry.Record}
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
	return w.history.extend(batches)
}
