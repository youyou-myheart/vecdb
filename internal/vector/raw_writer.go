package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type rawShardWriter struct {
	store    rawShardStore
	owner    Ownership
	records  map[string]uint64
	receipts map[string]receipt
	number   uint64
	end      uint64

	history *rawShardHistory
}

func newRawShardWriter(store rawShardStore, owner Ownership, history *rawShardHistory, count uint64) *rawShardWriter {
	w := &rawShardWriter{
		store: store, owner: owner, history: history,
		number: count, end: history.end(),
		records: make(map[string]uint64, len(history.records)), receipts: make(map[string]receipt, len(history.receipts)),
	}
	for _, entry := range history.entries {
		w.records[entry.Record.ID] = entry.Record.Revision
		w.receipts[entry.RequestID] = receipt{Digest: entry.Digest, Record: entry.Record}
	}
	return w
}

// The drainer prepares operations in queue order. Projected revisions let the
// following slots prepare independently while earlier slots write to disk.
// A failed slot retires this projection and all unacknowledged successors.
func (w *rawShardWriter) prepare(requests []*mutationRequest) ([]mutationResult, rawAcceptedBatch, error) {
	results := make([]mutationResult, len(requests))
	var batch rawAcceptedBatch
	for i, request := range requests {
		q := request.preparedMutation
		if previous, ok := w.receipts[q.mutation.RequestID]; ok {
			if previous.Digest != q.digest {
				results[i].err = ErrConflict
			} else {
				results[i].record = previous.Record
			}
			continue
		}
		record, err := q.nextRecord(w.records[q.id])
		if errors.Is(err, ErrConflict) {
			results[i].err = err
			continue
		}
		if err != nil {
			return nil, rawAcceptedBatch{}, err
		}
		if w.end == ^uint64(0) || w.number == ^uint64(0) {
			return nil, rawAcceptedBatch{}, errors.New("raw data log sequence exhausted")
		}
		if len(batch.Entries) == 0 {
			batch.Number, batch.First = w.number+1, w.end+1
		}
		w.end++
		w.records[q.id] = record.Revision
		w.receipts[q.mutation.RequestID] = receipt{Digest: q.digest, Record: record}
		batch.Entries = append(batch.Entries, rawEntry{RequestID: q.mutation.RequestID, Digest: q.digest, Record: record})
		results[i].record = record
	}
	if batch.Number != 0 {
		w.number = batch.Number
	}
	return results, batch, nil
}

func (w *rawShardWriter) write(ctx context.Context, batch rawAcceptedBatch) error {
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

func (w *rawShardWriter) accept(batch rawAcceptedBatch) error {
	return w.history.extend([]rawAcceptedBatch{batch})
}
