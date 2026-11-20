package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
	"strconv"
)

var ErrInvalidLogCursor = errors.New("log cursor exceeds head")

var ErrLogCompacted = errors.New("consumer behind retained log")

const LogVersion = 1
const LogBatchSize = 256

type LogEvent struct {
	Sequence uint64 `json:"sequence"`
	Record   Record `json:"record"`
}
type LogBatch struct {
	Events      []LogEvent
	Head, Floor uint64
}
type IncrementalRepository interface {
	Bootstrap(context.Context) ([]Record, uint64, error)
	ReadEvents(context.Context, uint64, uint64, int) (LogBatch, error)
	PruneEvents(context.Context, int) (uint64, error)
}

func (r *Repository) logKey(n uint64) []byte { return []byte(fmt.Sprintf("%slog/%020d", r.Prefix, n)) }

// logHead looks up one tail key at the caller's snapshot. GC atomically
// records the last removed LSN, including when the entire log is compacted.
func (r *Repository) logHead(ctx context.Context, tx dataReader) (uint64, error) {
	prefix := r.Prefix + "log/"
	key, err := tx.LastKey(ctx, []byte(prefix), []byte(r.Prefix+"log0"))
	if tikverr.IsErrNotFound(err) {
		return r.readCounter(ctx, tx, "log-floor")
	}
	if err != nil {
		return 0, err
	}
	if len(key) != len(prefix)+20 {
		return 0, fmt.Errorf("invalid data log tail key %q", key)
	}
	n, err := strconv.ParseUint(string(key[len(prefix):]), 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid data log tail key %q", key)
	}
	return n, nil
}

func (r *Repository) readCounter(ctx context.Context, tx dataReader, name string) (uint64, error) {
	b, e := tx.Get(ctx, []byte(r.Prefix+name))
	if tikverr.IsErrNotFound(e) {
		return 0, nil
	}
	if e != nil {
		return 0, e
	}
	var n uint64
	e = json.Unmarshal(b, &n)
	return n, e
}
func (r *Repository) Bootstrap(ctx context.Context) ([]Record, uint64, error) {
	tx, e := r.beginDataRead(ctx)
	if e != nil {
		return nil, 0, e
	}
	defer tx.Rollback()
	head, e := r.logHead(ctx, tx)
	if e != nil {
		return nil, 0, e
	}
	// The head and every record are read at the same data snapshot.
	rows, e := r.scanTxn(ctx, tx, "record")
	return rows, head, e
}
func (r *Repository) ReadEvents(ctx context.Context, after, through uint64, limit int) (LogBatch, error) {
	return r.readEvents(ctx, after, through, limit, 0)
}
func (r *Repository) readEvents(ctx context.Context, after, through uint64, limit, byteLimit int) (LogBatch, error) {
	var out LogBatch
	if limit < 1 || limit > LogBatchSize {
		return out, errors.New("invalid log batch size")
	}
	tx, e := r.beginDataRead(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	return r.readEventsIn(ctx, tx, after, through, limit, byteLimit)
}

func (r *Repository) readEventsIn(ctx context.Context, tx dataReader, after, through uint64, limit, byteLimit int) (LogBatch, error) {
	head, e := r.logHead(ctx, tx)
	if e != nil {
		return LogBatch{}, e
	}
	return r.readEventsAtHead(ctx, tx, after, through, head, limit, byteLimit)
}
func (r *Repository) readEventsAtHead(ctx context.Context, tx dataReader, after, through, head uint64, limit, byteLimit int) (LogBatch, error) {
	out := LogBatch{Head: head}
	var e error
	out.Floor, e = r.readCounter(ctx, tx, "log-floor")
	if e != nil {
		return out, e
	}
	if after > out.Head {
		return out, ErrInvalidLogCursor
	}
	if out.Floor > out.Head {
		return out, errors.New("invalid log cursor")
	}
	if after < out.Floor {
		return out, ErrLogCompacted
	}
	end := out.Head
	if through != 0 && through < end {
		end = through
	}
	if after >= end {
		return out, nil
	}
	// Bound TiKV's scan too. A short projection must not prefetch hundreds
	// of subsequent WAL values that the caller will never inspect.
	end = after + min(end-after, uint64(limit))
	it, e := tx.Iter(r.logKey(after+1), append(r.logKey(end), 0))
	if e != nil {
		return out, e
	}
	defer it.Close()
	bytes := 0
	for next := after + 1; next <= end; next++ {
		if e = ctx.Err(); e != nil {
			return LogBatch{}, e
		}
		if !it.Valid() || string(it.Key()) != string(r.logKey(next)) {
			return LogBatch{}, errors.New("log gap")
		}
		if byteLimit > 0 && bytes+len(it.Value()) > byteLimit {
			if len(out.Events) == 0 {
				return LogBatch{}, errors.New("event exceeds dispatch byte budget")
			}
			break
		}
		var event LogEvent
		if e = json.Unmarshal(it.Value(), &event); e != nil {
			return LogBatch{}, e
		}
		if event.Sequence != next {
			return LogBatch{}, errors.New("log sequence mismatch")
		}
		if e = validateBatch([]Record{event.Record}, r.Dimension); e != nil {
			return LogBatch{}, e
		}
		bytes += len(it.Value())
		out.Events = append(out.Events, event)
		if len(out.Events) == limit || bytes >= 8<<20 || next == end {
			break
		}
		if e = it.Next(); e != nil {
			return LogBatch{}, e
		}
	}
	return out, nil
}

// Prune only a bounded prefix certified by the current durable graph snapshot.
// Publish forbids checkpoint regression; a concurrent newer manifest stays safe.
func (r *Repository) PruneEvents(ctx context.Context, limit int) (uint64, error) {
	if limit < 1 || limit > LogBatchSize {
		return 0, errors.New("invalid prune batch size")
	}
	if r.usesRawShardData() {
		// Raw WAL entries also back record and receipt recovery. Keep the
		// entire history until a durable checkpoint can replace it.
		return 0, ctx.Err()
	}
	tx, e := r.begin()
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	if r.Fence != nil {
		if e = r.Fence(ctx, tx); e != nil {
			return 0, e
		}
	}
	floor, e := r.readCounter(ctx, tx, "log-floor")
	if e != nil {
		return 0, e
	}
	b, e := tx.Get(ctx, []byte(r.Namespace()+"manifest"))
	if tikverr.IsErrNotFound(e) {
		return floor, nil
	}
	if e != nil {
		return floor, e
	}
	var m Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		return floor, e
	}
	if m.LogVersion != LogVersion {
		return floor, nil
	}
	head, e := r.logHead(ctx, tx)
	if e != nil {
		return floor, e
	}
	if m.Offset > head || m.Offset < floor {
		return floor, errors.New("invalid durable log cursor")
	}
	end := m.Offset
	if r.Retain != nil {
		end, e = r.Retain(ctx, tx, end)
		if e != nil {
			return floor, e
		}
		if end < floor {
			return floor, nil
		}
	}
	end, e = r.retainConsumers(ctx, tx, "data", end)
	if e != nil {
		return floor, e
	}
	if end < floor {
		return floor, errors.New("consumer retention below existing floor")
	}
	if end-floor > uint64(limit) {
		end = floor + uint64(limit)
	}
	if r.packed != nil && end > floor {
		end, e = r.pruneDataBatches(ctx, tx, end, limit)
		if e != nil {
			return floor, e
		}
	}
	if end == floor {
		return floor, nil
	}
	for n := floor + 1; ; n++ {
		if e = ctx.Err(); e != nil {
			return floor, e
		}
		if e = tx.Delete(r.logKey(n)); e != nil {
			return floor, e
		}
		if n == end {
			break
		}
	}
	b, _ = json.Marshal(end)
	if e = tx.Set([]byte(r.Prefix+"log-floor"), b); e != nil {
		return floor, e
	}
	if e = tx.Commit(ctx); e != nil {
		return floor, e
	}
	return end, nil
}
