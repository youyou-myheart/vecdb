package vector

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// Split at the physical RawKV key, as TiKV's automatic size-based split does.
// Splitting through a TxnKV client would test a different physical key range.
func splitRawRegion(t *testing.T, ctx context.Context, client tikvRawDataClient, key []byte) {
	t.Helper()
	pd := client.GetPDClient().(*tikv.CodecPDClient)
	boundary := pd.GetCodec().EncodeRegionKey(key)
	cache := (rawkv.ClientProbe{Client: client.Client}).GetRegionCache()
	bo := tikv.NewBackoffer(ctx, 5000)
	rpc := tikv.NewRPCClient(tikv.WithCodec(pd.GetCodec()))
	defer rpc.Close()
	sender := tikv.NewRegionRequestSender(cache, rpc)
	req := tikvrpc.NewRequest(tikvrpc.CmdSplitRegion, &kvrpcpb.SplitRegionRequest{SplitKeys: [][]byte{key}})
	deadline := time.Now().Add(5 * time.Second)
	for {
		region, err := cache.LocateKey(bo, key)
		if err != nil {
			t.Fatal(err)
		}
		result, _, err := sender.SendReq(bo, req, region.Region, 5*time.Second)
		if err != nil {
			t.Fatal("split RawKV region:", err)
		}
		regionErr, err := result.GetRegionError()
		if err != nil {
			t.Fatal(err)
		}
		if regionErr != nil {
			if time.Now().After(deadline) || ctx.Err() != nil {
				t.Fatal("split RawKV region:", regionErr, ctx.Err())
			}
			// An earlier split can leave this client's region cache stale.
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if split := result.Resp.(*kvrpcpb.SplitRegionResponse); len(split.Regions) != 2 {
			t.Fatal("incomplete RawKV region split:", split)
		}
		break
	}
	// Ensure subsequent fresh clients see the boundary, not stale PD metadata.
	deadline = time.Now().Add(5 * time.Second)
	for {
		region, err := pd.Client.GetRegion(ctx, boundary)
		if err == nil && region != nil && bytes.Equal(region.Meta.GetStartKey(), boundary) {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatal("PD did not publish the RawKV split:", region, err, ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRawTiKVRegionSplitsPreserveLogsAndOwnership(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	store, err := openRawDataStore(f.ctx, f.r.pd, f.r.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	raw := store.client.(tikvRawDataClient)
	shard := store.shard(f.r.Prefix)
	stream := f.services[0].graphReplica.journal.rawStream("split-test")
	keys := [][]byte{shard.batchKey(1, 1), shard.batchKey(1, 2), stream.Key(1), stream.Key(2)}
	values := [][]byte{[]byte("wal-1"), []byte("wal-2"), []byte("graph-1"), []byte("graph-2")}
	if err = raw.BatchPut(f.ctx, keys, values); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{keys[1], keys[3]} {
		splitRawRegion(t, f.ctx, raw, key)
	}
	// Neither fresh metadata readers nor existing clients with cached regions
	// may fail when ownership and heartbeat requests follow a RawKV split.
	namespace, err := hex.DecodeString(strings.TrimSuffix(strings.TrimPrefix(f.r.Prefix, "vector-v0/"), "/"))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewRepository(f.r.pd, string(namespace), 2)
	if err != nil {
		t.Fatal("open metadata after RawKV splits:", err)
	}
	t.Cleanup(func() { _ = reopened.Client.Close() })
	if err = f.c.coordinator.Heartbeat(f.ctx, f.node); err != nil {
		t.Fatal("heartbeat after RawKV splits:", err)
	}
	owner, err := f.c.coordinator.Update(f.ctx, f.source, true, false)
	if err != nil || owner.Epoch != f.source.Epoch {
		t.Fatal("renew ownership after RawKV splits:", owner, err)
	}
	if _, err = f.r.Mutate(f.ctx, "after-split", Mutation{RequestID: "after-split", Vector: []float64{1, 0}}, false); err != nil {
		t.Fatal("transactional write after RawKV splits:", err)
	}
	// Exercise cross-region batches, point reads, reverse scans and takeover
	// CAS through the same adapter used by the data and graph journals.
	if err = raw.BatchPut(f.ctx, keys, values); err != nil {
		t.Fatal("RawKV batch write after split:", err)
	}
	got, err := raw.BatchGet(f.ctx, keys)
	if err != nil || len(got) != len(values) {
		t.Fatal(got, err)
	}
	for i, want := range values {
		if !bytes.Equal(got[i], want) {
			t.Fatalf("batch value %d = %q, want %q", i, got[i], want)
		}
	}
	if got, err := raw.Get(f.ctx, keys[1]); err != nil || !bytes.Equal(got, values[1]) {
		t.Fatal(got, err)
	}
	if key, value, err := raw.last(f.ctx, stream.Key(1), stream.Key(3)); err != nil || !bytes.Equal(key, keys[3]) || !bytes.Equal(value, values[3]) {
		t.Fatal(key, value, err)
	}
	if _, ok, err := raw.CompareAndSwap(f.ctx, shard.headKey(), nil, []byte("owner-1")); err != nil || !ok {
		t.Fatal("takeover CAS:", ok, err)
	}
	if old, ok, err := raw.CompareAndSwap(f.ctx, shard.headKey(), nil, []byte("stale-owner")); err != nil || ok || string(old) != "owner-1" {
		t.Fatal("stale takeover CAS:", string(old), ok, err)
	}
}
