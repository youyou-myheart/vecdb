package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"example.com/vector-storage/internal/shardlog"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
)

// One entry is the durable decision for a whole mutation group. Current
// records, subscription events and receipts are replayable materialized views.
// The owner finishes projecting one entry before it can propose the next.
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

// The last logical event is the completion witness. A successor cannot start
// a later batch until this one is projected. A delayed helper therefore sees
// either this witness (or its GC floor), or a write conflict on newer rows.
func (r *Repository) projectDataBatch(ctx context.Context, b mutationLogBatch) error {
	if b.Number == 0 {
		return nil
	}
	rows, err := batchRows(r.Prefix, b)
	if err != nil {
		return err
	}
	last := string(r.logKey(b.last()))
	keys := make([]string, 0, len(rows))
	for k := range rows {
		if k != last {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	keys = append(keys, last)
	return r.writeProjection(ctx, rows, keys, last, b.last())
}

func (r *Repository) writeProjection(ctx context.Context, rows map[string][]byte, keys []string, witness string, lastLSN uint64) error {
	for ctx.Err() == nil {
		err := r.writeProjectionOnce(ctx, rows, keys, witness, lastLSN)
		if tikverr.IsErrWriteConflict(err) {
			continue
		}
		if len(keys) < 2 || !errors.Is(err, shardlog.ErrSplitRequired) {
			return err
		}
		// Region boundaries or the TiKV request-size limit can split a batch.
		// Use the SDK's current Region cache. Persist the group which contains
		// the witness last, even if the region splits again during a retry.
		raw := make([][]byte, len(keys))
		for i, key := range keys {
			raw[i] = []byte(key)
		}
		groups, _, err := r.Client.GetRegionCache().GroupKeysByRegion(tikv.NewBackoffer(ctx, 1000), raw, nil)
		if err != nil {
			return err
		}
		if len(groups) == 1 {
			mid := len(keys) / 2
			if err = r.writeProjection(ctx, rows, keys[:mid], witness, lastLSN); err != nil {
				return err
			}
			return r.writeProjection(ctx, rows, keys[mid:], witness, lastLSN)
		}
		var final []string
		for _, group := range groups {
			batch := make([]string, len(group))
			for i, key := range group {
				batch[i] = string(key)
			}
			if batch[len(batch)-1] == witness {
				final = batch
				continue
			}
			if err = r.writeProjection(ctx, rows, batch, witness, lastLSN); err != nil {
				return err
			}
		}
		if len(final) != 0 {
			return r.writeProjection(ctx, rows, final, witness, lastLSN)
		}
		return nil
	}
	return ctx.Err()
}

func (r *Repository) writeProjectionOnce(ctx context.Context, rows map[string][]byte, keys []string, witness string, lastLSN uint64) error {
	tx, err := r.Client.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	value, err := tx.Get(ctx, []byte(witness))
	if err == nil {
		if !bytes.Equal(value, rows[witness]) {
			return errors.New("data WAL projection differs from committed event")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(err) {
		return err
	}
	floor, err := r.readCounter(ctx, tikvTransaction{tx}, "log-floor")
	if err != nil || floor >= lastLSN {
		return err
	}
	through := string(r.logKey(floor))
	for _, k := range keys {
		if strings.HasPrefix(k, r.Prefix+"log/") && k <= through {
			continue
		}
		if err = tx.Set([]byte(k), rows[k]); err != nil {
			return err
		}
	}
	if tx.Len() == 0 {
		return nil
	}
	return shardlog.Commit(ctx, tx)
}

// Stage deletion of immutable batches covered by this certified logical floor.
// Keep the tail slot as the durable batch-number anchor after complete GC.
func (r *Repository) pruneDataBatches(ctx context.Context, tx transactionAPI, end uint64, limit int) (uint64, error) {
	native, ok := nativeTiKV(tx)
	if !ok {
		return 0, errors.New("data WAL requires a TiKV snapshot")
	}
	tail, err := lastDataBatch(ctx, native, r.Prefix)
	if err != nil || tail.Number == 0 {
		return end, err
	}
	if _, err := native.Get(ctx, r.logKey(tail.last())); tikverr.IsErrNotFound(err) {
		// GC must never certify entries whose records/receipts could still be
		// missing. A previously compacted tail already has durable projections.
		floor, err := r.readCounter(ctx, native, "log-floor")
		if err != nil {
			return 0, err
		}
		if floor < tail.last() {
			end = min(end, tail.First-1)
		}
	} else if err != nil {
		return 0, err
	}
	it, err := native.Iter([]byte(r.Prefix+"wal/"), dataWAL(r.Prefix).Key(tail.Number))
	if err != nil {
		return 0, err
	}
	defer it.Close()
	for n := 0; n < limit && it.Valid(); n++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		var b mutationLogBatch
		if err := json.Unmarshal(it.Value(), &b); err != nil {
			return 0, err
		}
		if len(b.Entries) == 0 || b.First == 0 || b.last() < b.First || !bytes.Equal(it.Key(), dataWAL(r.Prefix).Key(b.Number)) {
			return 0, errors.New("invalid GC batch")
		}
		if b.last() > end {
			break
		}
		if err := tx.Delete(bytes.Clone(it.Key())); err != nil {
			return 0, err
		}
		if err := it.Next(); err != nil {
			return 0, err
		}
	}
	return end, nil
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
	if err = r.projectDataBatch(ctx, b); err != nil {
		return err
	}
	state.head.Store(max(tail, b.lastLSN()))
	state.batch = b.Number
	return nil
}

func (b mutationLogBatch) lastLSN() uint64 {
	if len(b.Entries) == 0 {
		return 0
	}
	return b.last()
}

// Semantic preparation uses the materialized state of the previous batch.
// All operations in the proposed batch share one WAL slot, including receipts.
func (r *Repository) proposeDataBatch(ctx context.Context, requests []*mutationRequest, state *appendLease) ([]mutationResult, error) {
	results := make([]mutationResult, len(requests))
	// Pin before local admission. An old snapshot conflicts with the successor's
	// recovery tombstone even if this goroutine pauses before its prewrite.
	tx, err := r.Client.Begin()
	if err == nil {
		defer tx.Rollback()
		err = state.WithLeader(ctx, state.owner.Epoch, func(ctx context.Context) error {
			keys := make([][]byte, 0, 3*len(requests))
			for _, q := range requests {
				keys = append(keys, r.key("record", q.id), r.key("request", q.mutation.RequestID))
				if r.collectionPrefix != "" {
					keys = append(keys, (&Repository{Prefix: r.collectionPrefix}).key("request", q.mutation.RequestID))
				}
			}
			values, err := tx.BatchGet(ctx, keys)
			if err != nil {
				return err
			}
			prepared := mutationBatchState{number: state.batch, end: state.head.Load(), records: make(map[string]uint64, len(requests)), receipts: make(map[string]receipt, len(requests))}
			legacyRepo := Repository{Prefix: r.collectionPrefix}
			router := IdentityPartitionRouter{layout: PartitionLayout{DataPartitions: r.packed.count}}
			for _, q := range requests {
				if raw := values[string(r.key("record", q.id))]; raw != nil {
					var record Record
					if err := json.Unmarshal(raw, &record); err != nil {
						return err
					}
					prepared.records[q.id] = record.Revision
				}
				id := q.mutation.RequestID
				raw := values[string(r.key("request", id))]
				legacy := raw == nil && r.collectionPrefix != ""
				if legacy {
					raw = values[string(legacyRepo.key("request", id))]
				}
				if raw != nil {
					var ack receipt
					if err := json.Unmarshal(raw, &ack); err != nil {
						return err
					}
					if !legacy || router.DataForID(ack.Record.ID) == router.DataForID(q.id) {
						prepared.receipts[id] = ack
					}
				}
			}
			var b mutationLogBatch
			results, b, err = prepared.prepare(requests)
			if err != nil {
				return err
			}
			if len(b.Entries) == 0 {
				return nil
			}
			raw, err := json.Marshal(b)
			if err != nil {
				return err
			}
			if err = r.dataLog(&state.Node, tikvTransaction{tx}, nil, false).Propose(ctx, state.owner.Epoch, shardlog.Slot{Sequence: b.Number, Value: raw}); err != nil {
				return fmt.Errorf("data WAL append not confirmed: %w", err)
			}
			state.batch = b.Number
			if err = r.projectDataBatch(ctx, b); err != nil {
				return fmt.Errorf("data WAL committed; projection pending: %w", err)
			}
			state.head.Store(b.last())
			return nil
		})
	}
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
	}
	return results, err
}
