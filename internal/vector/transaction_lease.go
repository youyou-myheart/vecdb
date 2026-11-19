package vector

import (
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

// Bound the commit timestamp and use TiKV Insert for immutable journal slots.
func (j graphJournal) nativeAppend(tx transactionAPI) (tikvTransaction, bool) {
	if !j.c.holdReleasedLeases || !j.c.referenceLogs {
		return tikvTransaction{}, false
	}
	if mixed, ok := tx.(*rawSnapshotTransaction); ok {
		tx = mixed.transactionAPI
	}
	native, ok := tx.(tikvTransaction)
	return native, ok
}
func (t tikvTransaction) insert(key, value []byte) error {
	return t.GetMemBuffer().SetWithFlags(key, value, kv.SetPresumeKeyNotExists)
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
		t.SetRPCInterceptor(interceptor.ChainRPCInterceptors(existing, limit))
	} else {
		t.SetRPCInterceptor(limit)
	}
	return t.Commit(ctx)
}
