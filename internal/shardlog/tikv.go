package shardlog

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// Insert a single immutable log slot without imposing a lease timestamp on
// TiKV. Let client-go see a fallback
// reply so it knows the store left a lock. Reject the subsequent commit locally:
// the SDK then rolls back the lock. RPC failures retain their unknown outcome.
func Insert(ctx context.Context, t *transaction.KVTxn, key, value []byte) error {
	if t.Len() != 0 {
		return errors.New("unexpected mutations before log slot insert")
	}
	if err := t.GetMemBuffer().SetWithFlags(key, value, kv.SetPresumeKeyNotExists); err != nil {
		return err
	}
	err := commitOnePhase(ctx, t, key)
	var exists *tikverr.ErrKeyExist
	if errors.As(err, &exists) {
		return ErrOccupied
	}
	return err
}

func commitOnePhase(ctx context.Context, t *transaction.KVTxn, insertKey []byte) error {
	var split atomic.Bool
	start := t.StartTS()
	t.SetEnable1PC(true)
	t.SetEnableAsyncCommit(false)
	limit := func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if req.Type == tikvrpc.CmdCommit && req.Commit().StartVersion == start {
				return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{Abort: "TiKV declined one-phase commit"},
				}}, nil
			}
			if req.Type != tikvrpc.CmdPrewrite || req.Prewrite().StartVersion != start {
				return next(target, req)
			}
			p := req.Prewrite()
			if !p.TryOnePc || p.UseAsyncCommit {
				split.Store(!p.TryOnePc && !p.UseAsyncCommit)
				return rejectPrewrite("expected TiKV one-phase commit"), nil
			}
			if insertKey != nil && (len(p.Mutations) != 1 || p.Mutations[0].Op != kvrpcpb.Op_Insert ||
				!bytes.Equal(p.Mutations[0].Key, insertKey) || !bytes.Equal(p.PrimaryLock, insertKey)) {
				return rejectPrewrite("expected one immutable log slot"), nil
			}
			return next(target, req)
		}
	}
	if existing := interceptor.GetRPCInterceptorFromCtx(ctx); existing != nil {
		limit = interceptor.ChainRPCInterceptors(limit, existing)
	}
	t.SetRPCInterceptor(limit)
	err := t.Commit(ctx)
	if err != nil && split.Load() {
		return errors.Join(ErrSplitRequired, err)
	}
	return err
}

func rejectPrewrite(reason string) *tikvrpc.Response {
	return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Abort: reason}}}}
}

// Commit uses one 1PC request for an already buffered set of mutations.
// The caller must ensure all keys fit in one request and one Region.
func Commit(ctx context.Context, t *transaction.KVTxn) error { return commitOnePhase(ctx, t, nil) }

func (s Stream) Append(ctx context.Context, t *transaction.KVTxn, lsn uint64, payload []byte) error {
	if len(s) == 0 || lsn == 0 || len(payload) == 0 {
		return errors.New("invalid log entry")
	}
	return Insert(ctx, t, s.Key(lsn), payload)
}

// Slot is a single immutable TiKV proposal.
type Slot struct {
	Sequence uint64
	Value    []byte
}

// Writer uses the snapshot obtained before local writer admission. Opening a
// fresh transaction on retry could bypass the successor's next-slot tombstone.
func (s Stream) Writer(tx *transaction.KVTxn) func(context.Context, Slot) error {
	if tx == nil {
		return nil
	}
	return func(ctx context.Context, p Slot) error {
		return s.Append(ctx, tx, p.Sequence, p.Value)
	}
}
