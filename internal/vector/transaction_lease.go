package vector

import (
	"context"
	"errors"

	"example.com/vector-storage/internal/shardlog"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

// Select the native immutable-slot path for leased, reference-backed journals.
func (j graphJournal) nativeAppend(tx transactionAPI) (tikvTransaction, bool) {
	if !j.c.holdReleasedLeases || !j.c.referenceLogs {
		return tikvTransaction{}, false
	}
	return nativeTiKV(tx)
}

func nativeTiKV(tx transactionAPI) (tikvTransaction, bool) {
	if mixed, ok := tx.(*rawSnapshotTransaction); ok {
		tx = mixed.transactionAPI
	}
	if view, ok := tx.(*packedSnapshot); ok {
		return view.tikvTransaction, true
	}
	native, ok := tx.(tikvTransaction)
	return native, ok
}
func (t tikvTransaction) insert(key, value []byte) error {
	return t.GetMemBuffer().SetWithFlags(key, value, kv.SetPresumeKeyNotExists)
}

func logStorageError(err error) error {
	switch {
	case errors.Is(err, shardlog.ErrOccupied):
		return ErrConflict
	case errors.Is(err, shardlog.ErrFenced):
		return ErrFenced
	default:
		return err
	}
}

func (t tikvTransaction) commitGraphSlot(ctx context.Context, key, value []byte) error {
	return logStorageError(shardlog.Insert(ctx, t.KVTxn, key, value))
}

func (t tikvTransaction) Commit(ctx context.Context) error {
	if t.Len() == 1 {
		return shardlog.Commit(ctx, t.KVTxn)
	}
	return t.KVTxn.Commit(ctx)
}

func (t tikvTransaction) commitOnePhaseBefore(ctx context.Context, expires int64) error {
	return logStorageError(shardlog.CommitBefore(ctx, t.KVTxn, expires))
}

// A successor starts its recovery snapshot at or after expires. Use 2PC so
// all keys, including every log entry, are prewritten before choosing commitTS.
// A recovery read then sees the old result or resolves its locks at a snapshot
// newer than any commit timestamp allowed for the old writer.
func (t tikvTransaction) commitBeforeLeaseExpiry(ctx context.Context, expires int64) error {
	allowed := func(ts uint64) bool { return int64(ts>>18) < expires }
	// This also disables 1PC and async commit in client-go.
	t.SetCommitTSUpperBoundCheck(allowed)
	limit := func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if req.Type == tikvrpc.CmdCommit {
				commit := req.Commit()
				// client-go can pick a new commitTS after CommitTsExpired
				// without calling its first commitTS checker again.
				if commit.StartVersion == t.StartTS() && !allowed(commit.CommitVersion) {
					return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{
						Error: &kvrpcpb.KeyError{Abort: "data writer lease expired before commit"},
					}}, nil
				}
			}
			return next(target, req)
		}
	}
	if existing := interceptor.GetRPCInterceptorFromCtx(ctx); existing != nil {
		limit = interceptor.ChainRPCInterceptors(existing, limit)
	}
	t.SetRPCInterceptor(limit)
	return t.KVTxn.Commit(ctx)
}
