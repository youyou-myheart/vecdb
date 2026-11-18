package vector

import (
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
)

// Fetch related metadata at one transaction snapshot. TiKV groups keys by
// Region internally; never call one transaction concurrently from goroutines.
func batchGet(ctx context.Context, tx transactionAPI, keys [][]byte) (map[string][]byte, error) {
	if native, ok := tx.(interface {
		BatchGet(context.Context, [][]byte) (map[string][]byte, error)
	}); ok {
		return native.BatchGet(ctx, keys)
	}
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := tx.Get(ctx, key)
		if tikverr.IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[string(key)] = value
	}
	return out, nil
}

// A batch's prefetched values plus its own writes. This preserves sequential
// revisions and request-ID checks even when several operations target one ID.
type prefetchedTransaction struct {
	transactionAPI
	values map[string][]byte
}

func (t prefetchedTransaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, ok := t.values[string(key)]
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return value, nil
}

func (t prefetchedTransaction) Set(key, value []byte) error {
	if err := t.transactionAPI.Set(key, value); err != nil {
		return err
	}
	t.values[string(key)] = value
	return nil
}
