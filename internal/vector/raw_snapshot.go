package vector

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

// A metadata transaction can read data as well. Pin the immutable WAL view
// on first access to each shard, even when the transaction began on the root
// repository or another shard. Control keys still use the TiKV snapshot.
type rawSnapshotTransaction struct {
	transactionAPI
	raw     *rawDataStore
	readCtx context.Context
	pinned  map[string]*rawShardSnapshot
}

func (r *Repository) usesRawShardData() bool {
	if r.raw == nil {
		return false
	}
	_, relative, ok := splitRawShardKey(r.raw.namespace, r.Prefix)
	return ok && relative == ""
}

func (r *Repository) wrapRawSnapshot(tx transactionAPI) transactionAPI {
	if r.raw == nil {
		return tx
	}
	return &rawSnapshotTransaction{transactionAPI: tx, raw: r.raw}
}

// Pure data readers access only WAL-backed ranges. A mixed/control reader
// must begin its TiKV transaction first so its metadata snapshot stays pinned.
func (r *Repository) beginDataRead(ctx context.Context) (dataReader, error) {
	if r.usesRawShardData() {
		return &rawSnapshotTransaction{raw: r.raw, readCtx: ctx}, nil
	}
	return r.begin()
}
func (t *rawSnapshotTransaction) Rollback() error {
	if t.transactionAPI == nil {
		return nil
	}
	return t.transactionAPI.Rollback()
}

func (t *rawSnapshotTransaction) snapshot(ctx context.Context, prefix string) (*rawShardSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if snap := t.pinned[prefix]; snap != nil {
		return snap, nil
	}
	snap, err := t.raw.snapshot(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, errors.New("missing raw data snapshot")
	}
	if t.pinned == nil {
		t.pinned = make(map[string]*rawShardSnapshot)
	}
	t.pinned[prefix] = snap
	return snap, nil
}

func splitRawShardKey(namespace, key string) (prefix, relative string, ok bool) {
	tail, ok := strings.CutPrefix(key, namespace)
	if !ok {
		return "", "", false
	}
	tail, ok = strings.CutPrefix(tail, "data/")
	if !ok || len(tail) < 5 || tail[4] != '/' {
		return "", "", false
	}
	for _, digit := range tail[:4] {
		if digit < '0' || digit > '9' {
			return "", "", false
		}
	}
	return key[:len(key)-len(tail)+5], tail[5:], true
}

func rawDataKey(namespace, key string) (prefix, relative string, ok bool) {
	prefix, relative, ok = splitRawShardKey(namespace, key)
	if !ok || !(relative == "log-floor" || strings.HasPrefix(relative, "record/") || strings.HasPrefix(relative, "log/")) {
		return "", "", false
	}
	return prefix, relative, true
}

func (t *rawSnapshotTransaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	prefix, relative, ok := rawDataKey(t.raw.namespace, string(key))
	if !ok {
		if t.transactionAPI == nil {
			return nil, errors.New("metadata key in raw data read")
		}
		return t.transactionAPI.Get(ctx, key)
	}
	snap, err := t.snapshot(ctx, prefix)
	if err != nil {
		return nil, err
	}
	switch {
	case relative == "log-floor":
		return strconv.AppendUint(nil, snap.floor, 10), nil
	case strings.HasPrefix(relative, "record/"):
		id, err := hex.DecodeString(strings.TrimPrefix(relative, "record/"))
		if err == nil {
			return marshalRawRecord(snap, string(id))
		}
	default:
		suffix := strings.TrimPrefix(relative, "log/")
		if seq, err := strconv.ParseUint(suffix, 10, 64); err == nil && formatRawLogSequence(seq) == suffix {
			return marshalRawEvent(snap, seq)
		}
	}
	return nil, tikverr.ErrNotExist
}

func (t *rawSnapshotTransaction) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	var out map[string][]byte
	var kvKeys [][]byte
	for _, key := range keys {
		if _, _, ok := rawDataKey(t.raw.namespace, string(key)); !ok {
			kvKeys = append(kvKeys, key)
			continue
		}
		value, err := t.Get(ctx, key)
		if tikverr.IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = make(map[string][]byte, len(keys))
		}
		out[string(key)] = value
	}
	if len(kvKeys) != 0 {
		kvValues, err := batchGet(ctx, t.transactionAPI, kvKeys)
		if err != nil || out == nil {
			return kvValues, err
		}
		for key, value := range kvValues {
			out[key] = value
		}
	}
	if out == nil {
		out = map[string][]byte{}
	}
	return out, nil
}

func (t *rawSnapshotTransaction) Set(key, value []byte) error {
	if _, _, ok := rawDataKey(t.raw.namespace, string(key)); ok {
		return errors.New("cannot write raw data through a metadata transaction")
	}
	return t.transactionAPI.Set(key, value)
}

func (t *rawSnapshotTransaction) Delete(key []byte) error {
	if _, _, ok := rawDataKey(t.raw.namespace, string(key)); ok {
		return errors.New("cannot delete raw data through a metadata transaction")
	}
	return t.transactionAPI.Delete(key)
}

func (t *rawSnapshotTransaction) SetEnable1PC(enabled bool) {
	if native, ok := t.transactionAPI.(interface{ SetEnable1PC(bool) }); ok {
		native.SetEnable1PC(enabled)
	}
}

func (t *rawSnapshotTransaction) commitBeforeLeaseExpiry(ctx context.Context, expires int64) error {
	if native, ok := t.transactionAPI.(interface {
		commitBeforeLeaseExpiry(context.Context, int64) error
	}); ok {
		return native.commitBeforeLeaseExpiry(ctx, expires)
	}
	return errors.New("transaction does not support lease-bounded commit")
}

func marshalRawRecord(snap *rawShardSnapshot, id string) ([]byte, error) {
	r, ok, err := snap.record(id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return json.Marshal(r)
}

func marshalRawEvent(snap *rawShardSnapshot, seq uint64) ([]byte, error) {
	entry, ok, err := snap.entry(seq)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return json.Marshal(LogEvent{Sequence: seq, Record: entry.Record})
}

func formatRawLogSequence(seq uint64) string { return fmt.Sprintf("%020d", seq) }

// Inclusive iterator bounds allow a final sequence of MaxUint64 without
// overflowing. For records the bounds index the byte-sorted ID array.
type rawSnapshotSpan struct {
	snap           *rawShardSnapshot
	prefix         string
	ids            []string
	low, high      uint64
	records, valid bool
}

func (s rawSnapshotSpan) key(i uint64) []byte {
	if s.records {
		return []byte(s.prefix + hex.EncodeToString([]byte(s.ids[i])))
	}
	return []byte(s.prefix + formatRawLogSequence(i))
}

func (s rawSnapshotSpan) value(i uint64) ([]byte, error) {
	if s.records {
		return marshalRawRecord(s.snap, s.ids[i])
	}
	return marshalRawEvent(s.snap, i)
}

func rawLogLowerBound(low, high uint64, key string) (uint64, bool) {
	if formatRawLogSequence(high) < key {
		return 0, false
	}
	for low < high {
		mid := low + (high-low)/2
		if formatRawLogSequence(mid) < key {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, true
}

func (t *rawSnapshotTransaction) span(ctx context.Context, start, end []byte) (rawSnapshotSpan, bool, error) {
	shard, relative, ok := rawDataKey(t.raw.namespace, string(start))
	family, lower, hasFamily := strings.Cut(relative, "/")
	if !ok || !hasFamily {
		return rawSnapshotSpan{}, false, nil
	}
	prefix := shard + family + "/"
	upper, within := strings.CutPrefix(string(end), prefix)
	unbounded := string(end) == shard+family+"0"
	if !unbounded && !within {
		return rawSnapshotSpan{}, true, errors.New("raw data iterator must stay within one shard key family")
	}
	snap, err := t.snapshot(ctx, shard)
	if err != nil {
		return rawSnapshotSpan{}, true, err
	}
	s := rawSnapshotSpan{snap: snap, prefix: prefix, records: family == "record"}
	if s.records {
		s.ids = snap.recordIDs()
		bound := func(key string) int {
			return sort.Search(len(s.ids), func(i int) bool {
				return hex.EncodeToString([]byte(s.ids[i])) >= key
			})
		}
		lo, hi := bound(lower), len(s.ids)
		if !unbounded {
			hi = bound(upper)
		}
		if lo < hi {
			s.low, s.high, s.valid = uint64(lo), uint64(hi-1), true
		}
		return s, true, nil
	}
	if snap.end > snap.floor {
		s.low, s.valid = rawLogLowerBound(snap.floor+1, snap.end, lower)
		s.high = snap.end
		if !unbounded {
			if hi, ok := rawLogLowerBound(snap.floor+1, snap.end, upper); ok {
				s.high = hi - 1
			}
		}
		s.valid = s.valid && s.low <= s.high
	}
	return s, true, nil
}

type rawSnapshotIterator struct {
	span  rawSnapshotSpan
	next  uint64
	more  bool
	key   []byte
	value []byte
}

func (i *rawSnapshotIterator) Valid() bool   { return i.key != nil }
func (i *rawSnapshotIterator) Key() []byte   { return i.key }
func (i *rawSnapshotIterator) Value() []byte { return i.value }
func (i *rawSnapshotIterator) Close() {
	i.more = false
	i.key, i.value = nil, nil
	i.span = rawSnapshotSpan{}
}

func (i *rawSnapshotIterator) Next() error {
	i.key, i.value = nil, nil
	if !i.more {
		return nil
	}
	value, err := i.span.value(i.next)
	if err != nil {
		i.Close()
		return err
	}
	i.key, i.value = i.span.key(i.next), value
	i.more = i.next < i.span.high
	if i.more {
		i.next++
	}
	return nil
}

func (t *rawSnapshotTransaction) Iter(start, end []byte) (iteratorAPI, error) {
	if _, _, ok := rawDataKey(t.raw.namespace, string(start)); !ok {
		if t.transactionAPI == nil {
			return nil, errors.New("metadata range in raw data read")
		}
		return t.transactionAPI.Iter(start, end)
	}
	// transactionAPI.Iter has no context. Acquiring the view can read TiKV;
	// advancing reads one immutable item from memory or the local cache file.
	parent := t.readCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	span, _, err := t.span(ctx, start, end)
	if err != nil {
		return nil, err
	}
	it := &rawSnapshotIterator{span: span, next: span.low, more: span.valid}
	if err := it.Next(); err != nil {
		return nil, err
	}
	return it, nil
}

func (t *rawSnapshotTransaction) Last(ctx context.Context, start, end []byte) ([]byte, []byte, error) {
	span, raw, err := t.span(ctx, start, end)
	if !raw {
		return t.transactionAPI.Last(ctx, start, end)
	}
	if err != nil {
		return nil, nil, err
	}
	if !span.valid {
		return nil, nil, tikverr.ErrNotExist
	}
	value, err := span.value(span.high)
	if err != nil {
		return nil, nil, err
	}
	return span.key(span.high), value, nil
}

func (t *rawSnapshotTransaction) FirstKey(ctx context.Context, start, end []byte) ([]byte, error) {
	return t.edge(ctx, start, end, false)
}

func (t *rawSnapshotTransaction) LastKey(ctx context.Context, start, end []byte) ([]byte, error) {
	return t.edge(ctx, start, end, true)
}

func (t *rawSnapshotTransaction) edge(ctx context.Context, start, end []byte, last bool) ([]byte, error) {
	span, raw, err := t.span(ctx, start, end)
	if !raw {
		if t.transactionAPI == nil {
			return nil, errors.New("metadata range in raw data read")
		}
		if last {
			return t.transactionAPI.LastKey(ctx, start, end)
		}
		return t.transactionAPI.FirstKey(ctx, start, end)
	}
	if err != nil {
		return nil, err
	}
	if !span.valid {
		return nil, tikverr.ErrNotExist
	}
	if last {
		return span.key(span.high), nil
	}
	return span.key(span.low), nil
}
