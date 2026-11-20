package vector

import (
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

func (w *rawShardWriter) write(ctx context.Context, batch mutationLogBatch) error {
	encoded, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	// All final revisions and receipts in this logical batch share one value.
	// BatchPut cannot split a single KV pair across TiKV Regions.
	if err = w.store.put(ctx, w.store.batchKey(w.owner.Epoch, batch.Number), encoded); err != nil {
		return fmt.Errorf("raw batch persistence not confirmed; retry same request_id: %w", err)
	}
	return nil
}

func (w *rawShardWriter) accept(batch mutationLogBatch) error {
	return w.history.extend([]mutationLogBatch{batch})
}
