package vector

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

var errSourceLockBusy = errors.New("source commit lock busy before prewrite")

type sourceFenceLocker interface {
	SetPessimistic(bool)
	LockKeys(context.Context, *kv.LockCtx, ...[]byte) error
}

// Serialize graph commits touching a shared source before prewriting any graph
// metadata. Otherwise a losing optimistic 2PC transaction can leave secondary
// locks that delay readers until cleanup. Objects are already persisted here;
// construction and S3 IO never run while these locks are held.
//
// Use the transaction's original snapshot. A newer fence version produces a
// definite write conflict and the caller retries with a fresh transaction; do
// not read an old snapshot after locking at an unrelated newer forUpdateTS.
func lockSourceFences(ctx context.Context, tx transactionAPI, c *Coordinator, inputs []Delivery) error {
	native, ok := tx.(sourceFenceLocker)
	if !ok || len(inputs) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(inputs))
	for _, input := range inputs {
		keys = append(keys, c.fenceKey(input.Owner.Partition))
	}
	slices.SortFunc(keys, bytes.Compare)
	keys = slices.CompactFunc(keys, bytes.Equal)
	native.SetPessimistic(true)
	// NOWAIT prevents wait cycles for overlapping multi-region batches. A loser
	// rolls back any acquired locks and retries with a fresh snapshot and jitter;
	// no graph metadata has been prewritten. One batched RPC per Region also
	// avoids extending the stale-snapshot window with one RPC per source.
	err := native.LockKeys(ctx, kv.NewLockCtx(tx.StartTS(), kv.LockNoWait, time.Now()), keys...)
	if errors.Is(err, tikverr.ErrLockAcquireFailAndNoWaitSet) {
		return errSourceLockBusy
	}
	return err
}
