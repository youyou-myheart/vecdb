package vector

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRawRoutedLayoutBackendIsolation(t *testing.T) {
	ctx := context.Background()
	route := &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}
	open := func(r *Repository, raw bool) (PartitionLayout, error) {
		return r.ensureRoutedLayoutWithBackend(ctx, 2, route, "hash", "node", false, raw)
	}
	const txnMode = "v14-cluster-routing-lease-hash-node-retention=false-owner-append-v1"

	t.Run("raw restart", func(t *testing.T) {
		r, db := newFakeRepo()
		first, err := open(r, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = r.ensureRoutedLayout(ctx, 2, route, "hash", "node", false); err == nil {
			t.Fatal("transactional routed startup accepted a raw namespace")
		}
		if _, err = r.ensureLayout(ctx, 2); err == nil {
			t.Fatal("non-routed cluster startup accepted a raw namespace")
		}
		if err = r.ensureMode(ctx, false); err == nil {
			t.Fatal("standalone startup accepted a raw namespace")
		}
		second, err := open(r, true)
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatalf("raw restart returned layout %+v and error %v", second, err)
		}
		if mode := string(db.data[r.Prefix+"cluster-mode"]); mode != txnMode+"-data-raw-v1" {
			t.Fatalf("persisted raw mode is %q", mode)
		}
	})

	t.Run("existing routed layout", func(t *testing.T) {
		for _, existing := range []string{txnMode, "v14-cluster-routing-lease-hash-node-retention=false", ""} {
			r, db := newFakeRepo()
			if _, err := open(r, false); err != nil {
				t.Fatal(err)
			}
			key := r.Prefix + "cluster-mode"
			if existing == "" {
				delete(db.data, key)
			} else {
				db.data[key] = []byte(existing)
			}
			if _, err := open(r, true); err == nil {
				t.Fatalf("raw startup accepted an existing routed layout with mode %q", existing)
			}
			if mode := string(db.data[key]); mode != existing {
				t.Fatalf("startup changed persisted mode %q to %q", existing, mode)
			}
		}
	})
}

func TestRawTiKVFallbackReadsContiguousBatchKeys(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ns := "raw-fallback/" + uuid.NewString() + "/"
	store, err := openRawDataStore(ctx, []string{"127.0.0.1:12379"}, ns)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	repo := &Repository{Prefix: ns + "data/0000/", Dimension: 2}
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	probe := &rawSnapshotProbe{rawDataClient: store.client, headKey: string(store.shard(repo.Prefix).headKey())}
	reader := newRawDataStore(probe, ns)
	if empty, err := reader.snapshot(ctx, repo.Prefix); err != nil || empty.end != 0 {
		t.Fatal(empty, err)
	}
	var batches []rawAcceptedBatch
	for _, id := range []string{"a", "b", "c"} {
		_, b, err := writer.prepare([]*mutationRequest{rawTestRequest(t, repo, id, id)})
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, b)
	}
	for _, n := range []int{0, 2} {
		if err := writer.write(ctx, batches[n]); err != nil {
			t.Fatal(err)
		}
	}
	probe.heads = 0
	if prefix, err := reader.snapshot(ctx, repo.Prefix); err != nil || prefix.end != 1 || probe.heads != 1 {
		t.Fatal(prefix, err, probe.heads)
	}
	if err := writer.write(ctx, batches[1]); err != nil {
		t.Fatal(err)
	}
	if prefix, err := reader.snapshot(ctx, repo.Prefix); err != nil || prefix.end != 3 {
		t.Fatal(prefix, err)
	}
	probe.heads, probe.batches = 0, 0
	if prefix, err := reader.snapshot(ctx, repo.Prefix); err != nil || prefix.end != 3 || probe.heads != 1 || probe.batches != 1 {
		t.Fatal(prefix, err, probe.heads, probe.batches)
	}
}
