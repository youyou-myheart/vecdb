package vector

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"example.com/vector-storage/internal/shardlog"
	tikverr "github.com/tikv/client-go/v2/error"
)

// One entry is the durable decision for a whole mutation group. Current
// records, subscription events and receipts are replayable materialized views.
// The WAL is the only durable mutation made by the writer.
func dataWAL(prefix string) shardlog.Stream { return shardlog.Stream(prefix + "wal/") }

func lastDataBatch(ctx context.Context, tx transactionAPI, prefix string) (mutationLogBatch, error) {
	var b mutationLogBatch
	k, v, err := tx.Last(ctx, []byte(prefix+"wal/"), []byte(prefix+"wal0"))
	if tikverr.IsErrNotFound(err) {
		return b, nil
	}
	if err != nil {
		return b, err
	}
	if err = json.Unmarshal(v, &b); err != nil {
		return b, err
	}
	if b.Number == 0 || b.First == 0 || len(b.Entries) == 0 || len(b.Entries) > mutationGroupRecords || b.last() < b.First || !bytes.Equal(k, dataWAL(prefix).Key(b.Number)) {
		return b, errors.New("invalid data WAL batch")
	}
	return b, nil
}

// One legacy event per logical LSN remains available to subscriptions and graph
// references. New receipts are scoped to the data partition.
func batchRows(prefix string, b mutationLogBatch) (map[string][]byte, error) {
	r := Repository{Prefix: prefix}
	rows := make(map[string][]byte, 3*len(b.Entries))
	for i, e := range b.Entries {
		if e.Record.ID == "" || e.RequestID == "" || e.Digest == "" {
			return nil, errors.New("invalid data WAL event")
		}
		rec, err := json.Marshal(e.Record)
		if err != nil {
			return nil, err
		}
		ack, err := json.Marshal(receipt{Digest: e.Digest, Record: e.Record})
		if err != nil {
			return nil, err
		}
		seq := b.First + uint64(i)
		event, err := json.Marshal(LogEvent{seq, e.Record})
		if err != nil {
			return nil, err
		}
		rows[string(r.key("record", e.Record.ID))] = rec
		rows[string(r.key("request", e.RequestID))] = ack
		rows[string(r.logKey(seq))] = event
	}
	return rows, nil
}

// Scan backwards to the last completed projection from the old storage
// format. The remaining suffix is authoritative for records and receipts.
func unprojectedDataBatches(ctx context.Context, tx tikvTransaction, prefix string) ([]mutationLogBatch, uint64, error) {
	r := Repository{Prefix: prefix}
	floor, err := r.readCounter(ctx, tx, "log-floor")
	if err != nil {
		return nil, 0, err
	}
	it, err := tx.IterReverse([]byte(prefix + "wal0"))
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()
	var batches []mutationLogBatch
	for it.Valid() && bytes.HasPrefix(it.Key(), []byte(prefix+"wal/")) {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		var b mutationLogBatch
		if err = json.Unmarshal(it.Value(), &b); err != nil {
			return nil, 0, err
		}
		if b.Number == 0 || b.First == 0 || len(b.Entries) == 0 || len(b.Entries) > mutationGroupRecords ||
			b.last() < b.First || !bytes.Equal(it.Key(), dataWAL(prefix).Key(b.Number)) {
			return nil, 0, errors.New("invalid data WAL batch")
		}
		if len(batches) > 0 {
			next := batches[len(batches)-1]
			if next.Number != b.Number+1 || next.First != b.last()+1 {
				return nil, 0, errors.New("data WAL gap")
			}
		}
		if b.last() <= floor {
			break
		}
		if !b.WALOnly {
			_, err = tx.Get(ctx, r.logKey(b.last()))
			if err == nil {
				break
			}
			if !tikverr.IsErrNotFound(err) {
				return nil, 0, err
			}
		}
		batches = append(batches, b)
		if err = it.Next(); err != nil {
			return nil, 0, err
		}
	}
	slices.Reverse(batches)
	return batches, floor, nil
}

type dataWALMemory struct {
	state  mutationBatchState // owned by the mutation drainer
	mu     sync.Mutex
	base   uint64
	events []LogEvent
	latest map[string]Record
}

func (m *dataWALMemory) apply(b mutationLogBatch) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, entry := range b.Entries {
		m.latest[entry.Record.ID] = entry.Record
		m.events = append(m.events, LogEvent{Sequence: b.First + uint64(i), Record: entry.Record})
	}
}

func (m *dataWALMemory) record(id string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, found := m.latest[id]
	return cloneRecord(record), found
}

func (m *dataWALMemory) window(after, head uint64, limit int) ([]LogEvent, error) {
	m.mu.Lock()
	if after < m.base || after > head || head-m.base > uint64(len(m.events)) {
		m.mu.Unlock()
		return nil, ErrInvalidLogCursor
	}
	drop := after - m.base
	clear(m.events[:drop])
	m.events = m.events[drop:]
	m.base = after
	events := slices.Clone(m.events[:min(head-after, uint64(limit))])
	m.mu.Unlock()
	size := 0
	for i, event := range events {
		raw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		size += len(raw)
		if size > dispatchBytes {
			if i == 0 {
				return nil, errors.New("event exceeds dispatch byte budget")
			}
			return events[:i], nil
		}
	}
	return events, nil
}

func (r *Repository) recoverDataWAL(ctx context.Context, state *appendLease) error {
	tx, err := r.Client.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	native := tikvTransaction{tx}
	b, err := lastDataBatch(ctx, native, r.Prefix)
	if err != nil {
		return err
	}
	tail, err := r.logHead(ctx, native)
	if err != nil {
		return err
	}
	if b.Number == ^uint64(0) {
		return errors.New("data WAL sequence exhausted")
	}
	// A late append to the next slot must conflict with recovery even when
	// a previous writer lost the reply and the new owner has no data to write.
	key := dataWAL(r.Prefix).Key(b.Number + 1)
	if err = tx.Delete(key); err != nil {
		return err
	}
	if err = shardlog.Commit(ctx, tx); err != nil {
		return err
	}
	if err = r.loadDataMemory(ctx, state); err != nil {
		return err
	}
	if state.wal.state.end != max(tail, b.lastLSN()) {
		return errors.New("data WAL changed during recovery")
	}
	state.head.Store(state.wal.state.end)
	state.batch = b.Number
	state.wal.state.number = b.Number
	return nil
}

func (r *Repository) loadDataMemory(ctx context.Context, state *appendLease) error {
	tx, err := r.begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	head, err := r.logHead(ctx, tx)
	if err != nil {
		return err
	}
	floor, err := r.readCounter(ctx, tx, "log-floor")
	if err != nil {
		return err
	}
	completed, err := r.readCounter(ctx, tx, "dispatch-completed")
	if err != nil {
		return err
	}
	base := max(floor, completed, state.owner.Offset)
	if base > head {
		return ErrInvalidLogCursor
	}
	m := &dataWALMemory{
		base:   base,
		latest: make(map[string]Record),
		state:  mutationBatchState{end: head, records: map[string]uint64{}, receipts: map[string]receipt{}},
	}
	reader := *r
	reader.scanLimit = int(^uint(0) >> 1)
	records, err := reader.scanTxn(ctx, tx, "record")
	if err != nil {
		return err
	}
	for _, record := range records {
		m.state.records[record.ID] = record.Revision
		m.latest[record.ID] = record
	}
	router := IdentityPartitionRouter{layout: PartitionLayout{DataPartitions: r.packed.count}}
	legacy := Repository{Prefix: r.collectionPrefix}
	if r.collectionPrefix != "" {
		if err = loadDataReceipts(ctx, tx, legacy.Prefix, m.state.receipts, func(ack receipt) bool {
			return r.Prefix == fmt.Sprintf("%sdata/%04d/", legacy.Prefix, router.DataForID(ack.Record.ID))
		}); err != nil {
			return err
		}
	}
	if err = loadDataReceipts(ctx, tx, r.Prefix, m.state.receipts, func(receipt) bool { return true }); err != nil {
		return err
	}
	for after := base; after < head; {
		page, err := r.readEventsAtHead(ctx, tx, after, head, head, dispatchLookahead, dispatchBytes)
		if err != nil {
			return err
		}
		m.events = append(m.events, page.Events...)
		after = page.Events[len(page.Events)-1].Sequence
	}
	state.wal = m
	return nil
}

func loadDataReceipts(ctx context.Context, tx transactionAPI, prefix string, receipts map[string]receipt, include func(receipt) bool) error {
	start := prefix + "request/"
	it, err := tx.Iter([]byte(start), []byte(prefix+"request0"))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var ack receipt
		if err := json.Unmarshal(it.Value(), &ack); err != nil {
			return err
		}
		if include(ack) {
			id, err := hex.DecodeString(string(it.Key()[len(start):]))
			if err != nil {
				return err
			}
			receipts[string(id)] = ack
		}
		if err := it.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (b mutationLogBatch) lastLSN() uint64 {
	if len(b.Entries) == 0 {
		return 0
	}
	return b.last()
}

// Revision checks and idempotency use the owner’s recovered in-memory state.
func (r *Repository) proposeDataBatch(ctx context.Context, requests []*mutationRequest, state *appendLease) ([]mutationResult, error) {
	var results []mutationResult
	err := state.WithLeader(ctx, state.owner.Epoch, func(ctx context.Context) error {
		// Production writers recover before admission.
		if state.wal == nil {
			return ErrFenced
		}
		current := &state.wal.state
		staged := mutationBatchState{number: current.number, end: current.end, records: make(map[string]uint64, len(requests)), receipts: make(map[string]receipt, len(requests))}
		for _, q := range requests {
			staged.records[q.id] = current.records[q.id]
			if ack, ok := current.receipts[q.mutation.RequestID]; ok {
				staged.receipts[q.mutation.RequestID] = ack
			}
		}
		var b mutationLogBatch
		var err error
		results, b, err = staged.prepare(requests)
		if err != nil || len(b.Entries) == 0 {
			return err
		}
		b.WALOnly = true
		raw, err := json.Marshal(b)
		if err != nil {
			return err
		}
		// Propose rechecks the local lease after pinning the transaction.
		// A delayed append admitted by Propose conflicts with the successor's
		// recovery tombstone.
		tx, err := r.Client.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err = r.dataLog(&state.Node, tikvTransaction{tx}, nil, false).Propose(ctx, state.owner.Epoch, shardlog.Slot{Sequence: b.Number, Value: raw}); err != nil {
			return fmt.Errorf("data WAL append not confirmed: %w", err)
		}
		maps.Copy(current.records, staged.records)
		maps.Copy(current.receipts, staged.receipts)
		current.end, current.number = staged.end, staged.number
		state.wal.apply(b)
		state.batch = b.Number
		state.head.Store(b.last())
		return nil
	})

	if err != nil {
		state.StepDown()
		state.uncertain.Store(true)
		// Occupation fences this writer's cached batch number. Returning a
		// revision conflict here would make the SDK discard a retryable write.
		if errors.Is(err, shardlog.ErrOccupied) {
			err = fmt.Errorf("%w: %w", ErrFenced, err)
		} else {
			err = logStorageError(err)
		}
		results = make([]mutationResult, len(requests))
		for i := range results {
			results[i] = mutationResult{err: err}
		}
	} else {
		for i := range results {
			results[i].record = cloneRecord(results[i].record)
		}
	}
	return results, err
}
