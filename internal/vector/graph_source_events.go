package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Use the same pinned data view as BatchGet, but do not encode vectors as JSON
// just to decode them again. The caller still verifies every reference's
// sequence, identity, revision and hash before publishing the graph delta.
func readGraphSourceEvents(ctx context.Context, tx transactionAPI, keys [][]byte) ([]LogEvent, error) {
	if raw, ok := tx.(*rawSnapshotTransaction); ok {
		return raw.graphSourceEvents(ctx, keys)
	}
	return readJSONGraphSourceEvents(ctx, tx, keys)
}

func readJSONGraphSourceEvents(ctx context.Context, tx transactionAPI, keys [][]byte) ([]LogEvent, error) {
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	events := make([]LogEvent, len(keys))
	for i, key := range keys {
		if err := json.Unmarshal(values[string(key)], &events[i]); err != nil {
			return nil, fmt.Errorf("missing or invalid data log reference: %w", err)
		}
	}
	return events, nil
}

func (t *rawSnapshotTransaction) graphSourceEvents(ctx context.Context, keys [][]byte) ([]LogEvent, error) {
	// Keep the generic reader for non-data or noncanonical keys. In particular,
	// a mixed metadata reader must retain its original transaction snapshot.
	for _, key := range keys {
		_, relative, ok := splitRawShardKey(t.raw.namespace, string(key))
		suffix, log := strings.CutPrefix(relative, "log/")
		seq, err := strconv.ParseUint(suffix, 10, 64)
		if !ok || !log || err != nil || formatRawLogSequence(seq) != suffix {
			return readJSONGraphSourceEvents(ctx, t, keys)
		}
	}
	events := make([]LogEvent, len(keys))
	for i, key := range keys {
		prefix, relative, _ := splitRawShardKey(t.raw.namespace, string(key))
		seq, _ := strconv.ParseUint(relative[4:], 10, 64)
		snap, err := t.snapshot(ctx, prefix)
		if err != nil {
			return nil, err
		}
		entry, found, err := snap.entry(seq)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("missing data log reference")
		}
		record, err := copyRawGraphRecord(entry.Record)
		if err != nil {
			return nil, err
		}
		events[i] = LogEvent{Sequence: seq, Record: record}
	}
	return events, nil
}

// The old JSON round trip detached maps/slices, omitted empty fields and
// normalized invalid UTF-8. Preserve all three properties, including JSON's
// handling of keys that collide after normalization and non-finite numbers.
func copyRawGraphRecord(record Record) (Record, error) {
	plain := utf8.ValidString(record.ID)
	for key, value := range record.Payload {
		plain = plain && utf8.ValidString(key) && utf8.ValidString(value)
	}
	for _, value := range record.Vector {
		plain = plain && !math.IsNaN(value) && !math.IsInf(value, 0)
	}
	if !plain {
		data, err := json.Marshal(record)
		if err != nil {
			return Record{}, err
		}
		var copied Record
		err = json.Unmarshal(data, &copied)
		return copied, err
	}
	copied := cloneRecord(record)
	if len(copied.Payload) == 0 {
		copied.Payload = nil
	}
	return copied, nil
}
