package vector

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

// Inspect the store's actual response for each successful mutation. Enabling
// TryOnePc at the SDK alone would also pass when the store falls back to 2PC.
func TestTiKVSingleKeyMetadataUsesOnePhase(t *testing.T) {
	f := newEpochTiKVFixture(t, 0)
	key := f.c.coordinator.nodeKey(f.node.ID)
	var onePhase, separate atomic.Int32
	ctx := interceptor.WithRPCInterceptor(f.ctx, func(send interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			resp, err := send(target, req)
			switch req.Type {
			case tikvrpc.CmdCommit:
				separate.Add(1)
			case tikvrpc.CmdPrewrite:
				p := req.Prewrite()
				if len(p.Mutations) == 1 && bytes.Equal(p.PrimaryLock, key) && err == nil && resp != nil {
					if reply, ok := resp.Resp.(*kvrpcpb.PrewriteResponse); ok && reply.OnePcCommitTs > p.StartVersion {
						onePhase.Add(1)
					}
				}
			}
			return resp, err
		}
	})
	old, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback()
	if err := f.c.coordinator.Heartbeat(ctx, f.node); err != nil {
		t.Fatal(err)
	}
	if err := old.Set(key, []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if err := old.Commit(ctx); !tikverr.IsErrWriteConflict(err) {
		t.Fatal("concurrent single-key write escaped its snapshot", err)
	}
	tx, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if onePhase.Load() != 2 || separate.Load() != 0 {
		t.Fatal("unexpected TiKV wire protocol", onePhase.Load(), separate.Load())
	}
	if nodes, err := f.c.coordinator.Nodes(context.Background()); err != nil || len(nodes) != 0 {
		t.Fatal("deleted node still visible", nodes, err)
	}
}
