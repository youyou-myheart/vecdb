package vector

import (
	"bytes"
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func (t tikvTransaction) Last(ctx context.Context, start, end []byte) ([]byte, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	snapshot := t.GetSnapshot()
	// The SDK treats a size <= 1 as the default (256). Two is the smallest
	// supported reverse-scan batch; consume only the first pair, then close.
	snapshot.SetScanBatchSize(2)
	defer snapshot.SetScanBatchSize(txnsnapshot.DefaultScanBatchSize)
	it, err := snapshot.IterReverse(end)
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
