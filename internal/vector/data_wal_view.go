package vector

import (
	"bytes"
	"context"
	"errors"
	"strings"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

type packedStore struct {
	namespace string
	count     int
	client    *txnkv.Client
}

// Reconstruct logical data rows at the caller's MVCC timestamp. Physical
// rows from earlier versions are the base; newer WAL batches overlay them.
type packedSnapshot struct {
	tikvTransaction
	store  *packedStore
	loaded map[string]bool // value says this prefix needs the overlay
	view   *transaction.KVTxn
}

func (t *packedSnapshot) dataPrefix(key []byte) string {
	prefix, rel, ok := splitRawShardKey(t.store.namespace, string(key))
	if ok && (strings.HasPrefix(rel, "log/") || strings.HasPrefix(rel, "record/") || strings.HasPrefix(rel, "request/")) {
		return prefix
	}
	return ""
}

func (t *packedSnapshot) load(ctx context.Context, key []byte) error {
	prefix := t.dataPrefix(key)
	if _, checked := t.loaded[prefix]; prefix == "" || checked {
		return nil
	}
	batches, floor, err := unprojectedDataBatches(ctx, t.tikvTransaction, prefix)
	if err != nil {
		return err
	}
	pending := len(batches) != 0
	if pending {
		if t.view == nil {
			t.view, err = t.store.client.Begin(tikv.WithStartTS(t.StartTS()))
			if err != nil {
				return err
			}
		}
		r := Repository{Prefix: prefix}
		for _, batch := range batches {
			rows, err := batchRows(prefix, batch)
			if err != nil {
				return err
			}
			for k, v := range rows {
				if strings.HasPrefix(k, prefix+"log/") && k <= string(r.logKey(floor)) {
					continue
				}
				if err = t.view.Set([]byte(k), v); err != nil {
					return err
				}
			}
		}
	}

	if t.loaded == nil {
		t.loaded = map[string]bool{}
	}
	t.loaded[prefix] = pending
	return nil
}

func (t *packedSnapshot) Get(ctx context.Context, k []byte) ([]byte, error) {
	if err := t.load(ctx, k); err != nil {
		return nil, err
	}
	if t.loaded[t.dataPrefix(k)] {
		return t.view.Get(ctx, k)
	}
	return t.tikvTransaction.Get(ctx, k)
}
func (t *packedSnapshot) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	for _, k := range keys {
		if err := t.load(ctx, k); err != nil {
			return nil, err
		}
	}
	if t.view == nil {
		return t.tikvTransaction.BatchGet(ctx, keys)
	}
	// A mixed read must continue to see its own staged metadata mutations.
	var data, meta [][]byte
	for _, k := range keys {
		if t.loaded[t.dataPrefix(k)] {
			data = append(data, k)
		} else {
			meta = append(meta, k)
		}
	}
	if len(data) == 0 {
		return t.tikvTransaction.BatchGet(ctx, meta)
	}
	out, err := t.view.BatchGet(ctx, data)
	if err != nil || len(meta) == 0 {
		return out, err
	}
	m, err := t.tikvTransaction.BatchGet(ctx, meta)
	if out == nil {
		out = make(map[string][]byte, len(m))
	}
	for k, v := range m {
		out[k] = v
	}
	return out, err
}
func (t *packedSnapshot) Iter(start, end []byte) (iteratorAPI, error) {
	if err := t.load(context.Background(), start); err != nil {
		return nil, err
	}
	if t.loaded[t.dataPrefix(start)] {
		return t.view.Iter(start, end)
	}
	return t.tikvTransaction.Iter(start, end)
}

func (t *packedSnapshot) dataEdge(ctx context.Context, start, end []byte, last, keysOnly bool) ([]byte, []byte, error) {
	if err := t.load(ctx, start); err != nil {
		return nil, nil, err
	}
	if !t.loaded[t.dataPrefix(start)] {
		return t.tikvTransaction.edge(ctx, start, end, last, keysOnly)
	}
	var it iteratorAPI
	var err error
	if last {
		it, err = t.view.IterReverse(end)
	} else {
		it, err = t.view.Iter(start, end)
	}
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	if !it.Valid() || bytes.Compare(it.Key(), start) < 0 {
		return nil, nil, tikverr.ErrNotExist
	}
	k := bytes.Clone(it.Key())
	if keysOnly {
		return k, nil, nil
	}
	return k, bytes.Clone(it.Value()), nil
}
func (t *packedSnapshot) Last(ctx context.Context, start, end []byte) ([]byte, []byte, error) {
	return t.dataEdge(ctx, start, end, true, false)
}
func (t *packedSnapshot) LastKey(ctx context.Context, start, end []byte) ([]byte, error) {
	k, _, e := t.dataEdge(ctx, start, end, true, true)
	return k, e
}
func (t *packedSnapshot) FirstKey(ctx context.Context, start, end []byte) ([]byte, error) {
	k, _, e := t.dataEdge(ctx, start, end, false, true)
	return k, e
}
func (t *packedSnapshot) Delete(k []byte) error {
	if err := t.load(context.Background(), k); err != nil {
		return err
	}
	if t.loaded[t.dataPrefix(k)] {
		if err := t.view.Delete(k); err != nil {
			return err
		}
	}
	return t.tikvTransaction.Delete(k)
}
func (t *packedSnapshot) Set(k, v []byte) error {
	if err := t.load(context.Background(), k); err != nil {
		return err
	}
	if t.loaded[t.dataPrefix(k)] {
		if err := t.view.Set(k, v); err != nil {
			return err
		}
	}
	return t.tikvTransaction.Set(k, v)
}
func (t *packedSnapshot) Rollback() error {
	if t.view != nil {
		return errors.Join(t.view.Rollback(), t.tikvTransaction.Rollback())
	}
	return t.tikvTransaction.Rollback()
}
