package vector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

type partitionBlobs struct {
	mu     sync.Mutex
	values map[string][]byte
}

func (b *partitionBlobs) Exists(context.Context, string) (bool, error) { return true, nil }
func (b *partitionBlobs) Create(context.Context, string) error         { return nil }
func (b *partitionBlobs) Put(_ context.Context, bucket, key string, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.values[key] = append([]byte(nil), v...)
	return nil
}
func (b *partitionBlobs) Get(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(v)), nil
}
func partitionObjects() *Objects {
	return &Objects{Client: &partitionBlobs{values: map[string][]byte{}}, Bucket: "test"}
}
func TestPartitionRouterAndLayout(t *testing.T) {
	l, e := newLayout(4)
	if e != nil {
		t.Fatal(e)
	}
	r := IdentityPartitionRouter{l}
	for id, want := range map[string]DataPartitionID{"a": 2, "b": 2, "c": 2, "向量-1": 3} {
		if got := r.DataForID(id); got != want {
			t.Fatal(id, got, want)
		}
	}
	for j := 0; j < 4; j++ {
		id, e := r.IndexForRecord(DataPartitionID(j), Record{Vector: []float64{999}})
		if e != nil || id != IndexPartitionID(fmt.Sprintf("index-%04d", j)) {
			t.Fatal(id, e)
		}
	}
	if _, e := r.IndexForRecord(4, Record{}); e == nil {
		t.Fatal("unbound source")
	}
	root, _ := newFakeRepo()
	ctx := context.Background()
	if _, e = root.ensureLayout(ctx, 4); e != nil {
		t.Fatal(e)
	}
	if _, e = root.ensureLayout(ctx, 4); e != nil {
		t.Fatal(e)
	}
	if _, e = root.ensureLayout(ctx, 8); e == nil {
		t.Fatal("layout changed")
	}
	for _, n := range []int{0, 65} {
		if _, e = newLayout(n); e == nil {
			t.Fatal(n)
		}
	}
	legacy, _ := newFakeRepo()
	putLogRecords(t, legacy, 1)
	if _, e = legacy.ensureLayout(ctx, 4); e == nil {
		t.Fatal("legacy silently repartitioned")
	}
	if _, e = legacy.ensureLayout(ctx, 1); e != nil {
		t.Fatal(e)
	}
}
func TestPartitionedLifecycleAndRecovery(t *testing.T) {
	ctx := context.Background()
	root, db := newFakeRepo()
	objects := partitionObjects()
	p, e := OpenPartitionedService(ctx, root, objects, 4)
	if e != nil {
		t.Fatal(e)
	}
	truth := NewIndex(2)
	counts := map[DataPartitionID]int{}
	for j := 0; j < 180; j++ {
		id := fmt.Sprint(j)
		m := Mutation{RequestID: "create-" + id, Vector: []float64{float64(j % 10), float64(j % 7)}, Payload: map[string]string{"group": fmt.Sprint(j % 3)}}
		rec, e := p.Mutate(ctx, id, m, false)
		if e != nil {
			t.Fatal(e)
		}
		_ = truth.Apply([]Record{rec})
		counts[p.router.DataForID(id)]++
	}
	if len(counts) != 4 {
		t.Fatal(counts)
	}
	if e = p.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	for _, q := range []Search{{Vector: []float64{0, 0}, K: 1, Exact: true}, {Vector: []float64{2, 3}, K: 17, Exact: true, Filter: map[string]string{"group": "1"}}, {Vector: []float64{0, 0}, K: 1000, Exact: true, MinVisible: &Token{"179", 1}}} {
		got, e := p.Search(q)
		want, _ := truth.Search(q)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("global top-k differs", e, len(got), len(want))
		}
	}
	m, e := p.Checkpoint(ctx)
	if e != nil || len(m.Partitions) != 4 {
		t.Fatal(m, e)
	}
	for _, binding := range p.layout.Bindings {
		repo := p.data[binding.Data]
		if _, ok := db.data[string(repo.key("record", "0"))]; ok && p.router.DataForID("0") != binding.Data {
			t.Fatal("record in wrong source")
		}
		manifest := m.Partitions[string(binding.Index)]
		if manifest.Offset != uint64(counts[binding.Data]) {
			t.Fatal(manifest)
		}
		if repo.Namespace() == p.indexes[binding.Index].(*Service).Repo.Namespace() {
			t.Fatal("data and index metadata coupled")
		}
	}
	if _, e = p.PruneState(ctx); e != nil {
		t.Fatal(e)
	}
	changed, e := p.Mutate(ctx, "0", Mutation{RequestID: "update", Vector: []float64{-1, -1}, Payload: map[string]string{"group": "moved"}}, false)
	if e != nil {
		t.Fatal(e)
	}
	_ = truth.Apply([]Record{changed})
	deleted, e := p.Mutate(ctx, "1", Mutation{RequestID: "delete"}, true)
	if e != nil {
		t.Fatal(e)
	}
	_ = truth.Apply([]Record{deleted})
	// Same request ID cannot be reused even when the other ID hashes elsewhere.
	other := "other"
	for p.router.DataForID(other) == p.router.DataForID("0") {
		other += "x"
	}
	if _, e = p.Mutate(ctx, other, Mutation{RequestID: "update", Vector: []float64{1, 1}}, false); !errors.Is(e, ErrConflict) {
		t.Fatal("namespace idempotency lost", e)
	}
	recovered, e := OpenPartitionedService(ctx, root, objects, 4)
	if e != nil {
		t.Fatal(e)
	}
	q := Search{Vector: []float64{0, 0}, K: 1000, Exact: true, MinVisible: &Token{"1", 2}}
	got, e := recovered.Search(q)
	want, _ := truth.Search(q)
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("partition recovery differs", e)
	}
	if e = recovered.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = OpenPartitionedService(ctx, root, objects, 2); e == nil {
		t.Fatal("changed partition count accepted")
	}
}

type brokenPartition struct {
	PartitionIndex
	err  error
	gate chan struct{}
}

func (b brokenPartition) Search(q Search) ([]Hit, error) {
	if b.gate != nil {
		<-b.gate
	}
	return nil, b.err
}
func (b brokenPartition) Sync(context.Context) error { return b.err }
func TestPartitionFailureAndDeadline(t *testing.T) {
	root, _ := newFakeRepo()
	p, e := OpenPartitionedService(context.Background(), root, partitionObjects(), 2)
	if e != nil {
		t.Fatal(e)
	}
	id := p.router.QueryIndexes(Search{})[0]
	original := p.indexes[id]
	p.indexes[id] = brokenPartition{PartitionIndex: original, err: injected}
	if hits, e := p.Search(Search{Vector: []float64{0, 0}, K: 10}); !errors.Is(e, ErrPartitionUnavailable) || hits != nil {
		t.Fatal("partial results leaked", hits, e)
	}
	if e = p.Sync(context.Background()); !errors.Is(e, ErrPartitionUnavailable) {
		t.Fatal(e)
	}
	gate := make(chan struct{})
	p.indexes[id] = brokenPartition{PartitionIndex: original, gate: gate}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, e = p.SearchContext(ctx, Search{Vector: []float64{0, 0}, K: 1}); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	close(gate)
}
func TestSinglePartitionCompatibility(t *testing.T) {
	root, _ := newFakeRepo()
	ctx := context.Background()
	objects := partitionObjects()
	putLogRecords(t, root, 3)
	old, e := NewService(ctx, root, objects)
	if e != nil {
		t.Fatal(e)
	}
	m, e := old.Checkpoint(ctx)
	if e != nil {
		t.Fatal(e)
	}
	p, e := OpenPartitionedService(ctx, root, objects, 1)
	if e != nil {
		t.Fatal(e)
	}
	got, e := p.Get(ctx, "1")
	if e != nil || got.Revision != 1 {
		t.Fatal(got, e)
	}
	latest, e := p.Checkpoint(ctx)
	if e != nil || latest.Format != m.Format || latest.Offset != m.Offset || len(latest.Partitions) != 0 {
		t.Fatal(latest, e)
	}
}

func (b *partitionBlobs) Delete(_ context.Context, bucket, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.values, key)
	return nil
}
