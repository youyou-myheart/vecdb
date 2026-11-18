package vector

import (
	"bytes"
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func (t tikvTransaction) Last(ctx context.Context, start, end []byte) ([]byte, []byte, error) {
	return t.edge(ctx, start, end, true, false)
}

func (t tikvTransaction) LastKey(ctx context.Context, start, end []byte) ([]byte, error) {
	key, _, err := t.edge(ctx, start, end, true, true)
	return key, err
}

func (t tikvTransaction) FirstKey(ctx context.Context, start, end []byte) ([]byte, error) {
	key, _, err := t.edge(ctx, start, end, false, true)
	return key, err
}

func (t tikvTransaction) edge(ctx context.Context, start, end []byte, reverse, keyOnly bool) ([]byte, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	snapshot := t.GetSnapshot()
	if keyOnly {
		snapshot.SetKeyOnly(true)
		defer snapshot.SetKeyOnly(false)
	}
	// The SDK treats a size <= 1 as the default (256). Two is the smallest
	// supported scan batch; consume only the first pair, then close.
	snapshot.SetScanBatchSize(2)
	defer snapshot.SetScanBatchSize(txnsnapshot.DefaultScanBatchSize)
	var it iteratorAPI
	var err error
	if reverse {
		it, err = snapshot.IterReverse(end)
	} else {
		it, err = snapshot.Iter(start, end)
	}
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	if err = ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !it.Valid() || bytes.Compare(it.Key(), start) < 0 {
		return nil, nil, tikverr.ErrNotExist
	}
	return bytes.Clone(it.Key()), bytes.Clone(it.Value()), nil
}
