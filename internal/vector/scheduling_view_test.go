package vector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

type countedScheduleTx struct {
	transactionAPI
	reads map[string]int
}

func (t *countedScheduleTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	t.reads[string(key)]++
	return t.transactionAPI.Get(ctx, key)
}

func TestSchedulingViewTransactionScope(t *testing.T) {
	ctx := context.Background()
	r, db := newFakeRepo()
	c := &Coordinator{root: r}
	tx, _ := r.begin()
	defer tx.Rollback()
	counted := &countedScheduleTx{tx, map[string]int{}}
	v := newSchedulingView(c, counted)
	id := IndexPartitionID("index-0000")
	for i := 0; i < 3; i++ {
		if _, err := v.plan(ctx, id); err == nil {
			t.Fatal("missing plan accepted")
		}
		if _, err := v.owner(ctx, id); err == nil {
			t.Fatal("missing owner accepted")
		}
	}
	if counted.reads[string(c.planKey(id))] != 1 || counted.reads[string(c.ownerKey(id))] != 1 {
		t.Fatal(counted.reads)
	}
	db.data[string(c.planKey(id))] = []byte(`{"barrier":9}`)
	next := newSchedulingView(c, counted)
	plan, err := next.plan(ctx, id)
	if err != nil || plan.Barrier != 9 {
		t.Fatalf("%+v %v", plan, err)
	}
	db.getFail = string(c.ownerKey(id))
	next = newSchedulingView(c, counted)
	for i := 0; i < 2; i++ {
		if _, err = next.owner(ctx, id); !errors.Is(err, injected) {
			t.Fatal(err)
		}
	}
	if counted.reads[string(c.ownerKey(id))] != 2 {
		t.Fatal(counted.reads)
	}
}

func legacyRankWorkers(id IndexPartitionID, workers []ClusterNode) []ClusterNode {
	result := append([]ClusterNode(nil), workers...)
	score := func(n ClusterNode) string {
		sum := sha256.Sum256([]byte(string(id) + "\x00" + n.ID))
		return hex.EncodeToString(sum[:])
	}
	sort.Slice(result, func(i, j int) bool { return score(result[i]) > score(result[j]) })
	return result
}
func TestRankWorkersEquivalent(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 32, 128} {
		workers := make([]ClusterNode, n)
		for i := range workers {
			workers[i] = ClusterNode{ID: fmt.Sprintf("worker-%d", n-i), Session: "session"}
		}
		before := append([]ClusterNode{}, workers...)
		for shard := 0; shard < 64; shard++ {
			id := IndexPartitionID(fmt.Sprintf("index-%04d", shard))
			got, want := rankWorkers(id, workers), legacyRankWorkers(id, workers)
			for i := range got {
				if !reflect.DeepEqual(got[i], want[i]) {
					t.Fatalf("n=%d shard=%d", n, shard)
				}
			}
		}
		if !reflect.DeepEqual(workers, before) {
			t.Fatal("input changed")
		}
	}
}
func BenchmarkRankWorkers(b *testing.B) {
	workers := make([]ClusterNode, 64)
	for i := range workers {
		workers[i].ID = fmt.Sprintf("worker-%d", i)
	}
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			legacyRankWorkers("index-0000", workers)
		}
	})
	b.Run("precomputed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rankWorkers("index-0000", workers)
		}
	})
}
