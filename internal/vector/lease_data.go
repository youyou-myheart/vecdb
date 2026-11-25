package vector

import (
	"context"
	"fmt"
	"sync/atomic"

	"example.com/vector-storage/internal/lease"
	"example.com/vector-storage/internal/shardlog"
	tikverr "github.com/tikv/client-go/v2/error"
)

const guardWriteEvery = 100

// appendLease ties one data writer and its log cursor to a confirmed owner.
type appendLease struct {
	shardlog.Node
	owner         Ownership
	guard         []byte
	token         []byte
	boundCommitTS bool
	head          atomic.Uint64
	floor         atomic.Uint64
	indexed       atomic.Uint64 // contiguous source prefix confirmed by every target
	// Only the partition's drainer counts confirmed, nonempty commits.
	wal               *dataWALMemory
	batch             uint64 // committed single-key WAL batch number; serialized by the drainer
	batchesSinceGuard int
	// An ambiguous commit stops admission until the dispatcher recovers a new epoch.
	uncertain atomic.Bool
}

func (l *appendLease) grantForTransaction(tx transactionAPI, epoch uint64) (lease.Snapshot, error) {
	grant, live := l.Snapshot()
	if !live || grant.Epoch != epoch || txnMillis(tx) >= grant.Expires {
		return lease.Snapshot{}, ErrFenced
	}
	return grant, nil
}

// Every append is fenced, including TiKV retries. Only a confirmed commit can
// advance the guard cadence. A nil lease is the legacy unowned write path.
func (l *appendLease) commit(ctx context.Context, tx transactionAPI, epoch uint64) error {
	commit := tx.Commit
	writeGuard := false
	if l != nil {
		grant, err := l.grantForTransaction(tx, epoch)
		if err != nil {
			return err
		}
		// Coordinators permitting early takeover and transactions without a
		// commit timestamp limit require a guard write at every commit.
		writeGuard = true
		if bounded, ok := tx.(interface {
			commitBeforeLeaseExpiry(context.Context, int64) error
		}); l.boundCommitTS && ok {
			commit = func(ctx context.Context) error {
				return bounded.commitBeforeLeaseExpiry(ctx, grant.Expires)
			}
			writeGuard = l.batchesSinceGuard+1 >= guardWriteEvery
		}
		if writeGuard {
			if err = tx.Set(l.guard, l.token); err != nil {
				return err
			}
		}
	}
	if err := commit(ctx); err != nil {
		if l != nil && !tikverr.IsErrWriteConflict(err) {
			l.StepDown()
			l.uncertain.Store(true)
		}
		return fmt.Errorf("commit not confirmed; retry same request_id: %w", err)
	}
	if l != nil {
		if writeGuard {
			l.batchesSinceGuard = 0
		} else {
			l.batchesSinceGuard++
		}
	}
	return nil
}
