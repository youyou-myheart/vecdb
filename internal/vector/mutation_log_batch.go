package vector

import "errors"

type mutationLogEntry struct {
	RequestID string `json:"request_id"`
	Digest    string `json:"digest"`
	Record    Record `json:"record"`
}

// A single atomic KV value contains the complete accepted decision for its
// batch. Replaying a contiguous prefix needs no volatile writer state.
type mutationLogBatch struct {
	Number  uint64             `json:"number"`
	WALOnly bool               `json:"wal_only,omitempty"`
	First   uint64             `json:"first"`
	Entries []mutationLogEntry `json:"entries"`
}

func (b mutationLogBatch) last() uint64 { return b.First + uint64(len(b.Entries)) - 1 }

// The partition owner retains revisions and receipts in memory across appends.
type mutationBatchState struct {
	records  map[string]uint64
	receipts map[string]receipt
	number   uint64
	end      uint64
}

// The drainer prepares operations in queue order. Projected revisions let the
// following slots prepare independently while earlier slots write to disk.
// A failed slot retires this projection and all unacknowledged successors.
func (w *mutationBatchState) prepare(requests []*mutationRequest) ([]mutationResult, mutationLogBatch, error) {
	results := make([]mutationResult, len(requests))
	var batch mutationLogBatch
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
			return nil, mutationLogBatch{}, err
		}
		if w.end == ^uint64(0) || w.number == ^uint64(0) {
			return nil, mutationLogBatch{}, errors.New("data log sequence exhausted")
		}
		if len(batch.Entries) == 0 {
			batch.Number, batch.First = w.number+1, w.end+1
		}
		w.end++
		w.records[q.id] = record.Revision
		w.receipts[q.mutation.RequestID] = receipt{Digest: q.digest, Record: record}
		batch.Entries = append(batch.Entries, mutationLogEntry{RequestID: q.mutation.RequestID, Digest: q.digest, Record: record})
		results[i].record = record
	}
	if batch.Number != 0 {
		w.number = batch.Number
	}
	return results, batch, nil
}
