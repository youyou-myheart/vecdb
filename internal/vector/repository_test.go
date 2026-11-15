package vector

import (
	"context"
	"encoding/json"
	"errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var injected = errors.New("injected failure")

// This fake tests application decisions/failure propagation, NOT TiKV concurrency semantics.
type fakeDB struct {
	nowMillis                             int64
	mu                                    sync.Mutex
	data                                  map[string][]byte
	beginErr, commitErr, iterErr, nextErr error
	getFail                               string
	setFail                               int
	commits, rollbacks                    int
	committedDespiteError                 bool
}
type fakeTx struct {
	start  uint64
	db     *fakeDB
	staged map[string][]byte
	sets   int
}

func newFakeRepo() (*Repository, *fakeDB) {
	db := &fakeDB{data: map[string][]byte{}}
	r := &Repository{Prefix: "test/", Dimension: 2}
	r.beginTxn = func() (transactionAPI, error) {
		if db.beginErr != nil {
			return nil, db.beginErr
		}
		now := db.nowMillis
		if now == 0 {
			now = time.Now().UnixMilli()
		}
		return &fakeTx{start: uint64(now) << 18, db: db, staged: map[string][]byte{}}, nil
	}
	return r, db
}
func (f *fakeTx) StartTS() uint64 { return f.start }
func (f *fakeTx) Get(ctx context.Context, k []byte) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if f.db.getFail != "" && strings.Contains(string(k), f.db.getFail) {
		return nil, injected
	}
	v, ok := f.db.data[string(k)]
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}
func (f *fakeTx) Set(k, v []byte) error {
	f.sets++
	if f.sets == f.db.setFail {
		return injected
	}
	f.staged[string(k)] = append([]byte(nil), v...)
	return nil
}
func (f *fakeTx) Commit(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if f.db.commitErr != nil && !f.db.committedDespiteError {
		return f.db.commitErr
	}
	for k, v := range f.staged {
		if v == nil {
			delete(f.db.data, k)
		} else {
			f.db.data[k] = v
		}
	}
	f.db.commits++
	return f.db.commitErr
}
func (f *fakeTx) Rollback() error {
	f.db.mu.Lock()
	defer f.db.mu.Unlock()
	f.db.rollbacks++
	f.staged = nil
	return nil
}
func (f *fakeTx) Iter(start, end []byte) (iteratorAPI, error) {
	if f.db.iterErr != nil {
		return nil, f.db.iterErr
	}
	keys := []string{}
	for k := range f.db.data {
		if k >= string(start) && k < string(end) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	values := [][]byte{}
	for _, k := range keys {
		values = append(values, f.db.data[k])
	}
	return &fakeIter{values: values, keys: keys, err: f.db.nextErr}, nil
}

func (f *fakeTx) Delete(k []byte) error { return f.Set(k, nil) }

type fakeIter struct {
	keys   []string
	values [][]byte
	pos    int
	err    error
}

func (f *fakeIter) Key() []byte          { return []byte(f.keys[f.pos]) }
func (f *fakeIter) Valid() bool          { return f.pos < len(f.values) }
func (f *fakeIter) Value() []byte        { return f.values[f.pos] }
func (f *fakeIter) Next() error          { f.pos++; return f.err }
func (f *fakeIter) Close()               {}
func seed(db *fakeDB, key []byte, v any) { b, _ := json.Marshal(v); db.data[string(key)] = b }

func TestRepositorySchema(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		setup   func(*fakeDB)
		wantErr bool
	}{
		{"create", func(*fakeDB) {}, false},
		{"same", func(d *fakeDB) { d.data["test/schema"] = []byte("v2:dimension=2:metric=l2:log=1") }, false},
		{"mismatch", func(d *fakeDB) { d.data["test/schema"] = []byte("wrong") }, true},
		{"begin_failure", func(d *fakeDB) { d.beginErr = injected }, true},
		{"read_failure", func(d *fakeDB) { d.getFail = "schema" }, true},
		{"write_failure", func(d *fakeDB) { d.setFail = 1 }, true},
		{"commit_failure", func(d *fakeDB) { d.commitErr = injected }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, d := newFakeRepo()
			tc.setup(d)
			if e := r.ensureSchema(ctx); (e != nil) != tc.wantErr {
				t.Fatal(e)
			}
			if !tc.wantErr && string(d.data["test/schema"]) != "v2:dimension=2:metric=l2:log=1" {
				t.Fatal("schema missing")
			}
		})
	}
	for _, d := range []int{0, 4097} {
		if _, e := NewRepository(nil, "x", d); e == nil {
			t.Fatal("invalid schema accepted")
		}
	}
	if _, e := NewRepository(nil, "", 2); e == nil {
		t.Fatal("empty namespace accepted")
	}
}
func TestRepositoryLifecycle(t *testing.T) {
	ctx := context.Background()
	r, d := newFakeRepo()
	zero := uint64(0)
	m := Mutation{RequestID: "create", Vector: []float64{1, 2}, Payload: map[string]string{"tag": "a"}, ExpectedRevision: &zero}
	first, e := r.Mutate(ctx, "a/b", m, false)
	if e != nil || first.Revision != 1 {
		t.Fatal(first, e)
	}
	if len(d.data) != 4 || d.commits != 1 {
		t.Fatal("record/event/receipt not committed together")
	}
	got, e := r.Get(ctx, "a/b")
	if e != nil || got.Vector[1] != 2 {
		t.Fatal(got, e)
	}
	again, e := r.Mutate(ctx, "a/b", m, false)
	if e != nil || again.Revision != 1 || d.commits != 1 {
		t.Fatal(again, e)
	}
	if _, e = r.Mutate(ctx, "other", m, false); !errors.Is(e, ErrConflict) {
		t.Fatal("idempotency key reused across ID", e)
	}
	revision := uint64(1)
	updated, e := r.Mutate(ctx, "a/b", Mutation{RequestID: "update", Vector: []float64{9, 9}, ExpectedRevision: &revision}, false)
	if e != nil || updated.Revision != 2 {
		t.Fatal(updated, e)
	}
	if _, e = r.Mutate(ctx, "a/b", Mutation{RequestID: "stale", Vector: []float64{0, 0}, ExpectedRevision: &revision}, false); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	deleted, e := r.Mutate(ctx, "a/b", Mutation{RequestID: "delete"}, true)
	if e != nil || !deleted.Deleted || deleted.Vector != nil || deleted.Payload != nil {
		t.Fatal(deleted, e)
	}
	recreated, e := r.Mutate(ctx, "a/b", Mutation{RequestID: "recreate", Vector: []float64{0, 1}}, false)
	if e != nil || recreated.Revision != 4 {
		t.Fatal(recreated, e)
	}
	events, e := r.ReadEvents(ctx, 0, 0, 256)
	if e != nil || len(events.Events) != 4 {
		t.Fatal(events, e)
	}
	if _, e = r.Get(ctx, "missing"); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	if r.Namespace() != "test/" || r.VectorDimension() != 2 {
		t.Fatal("metadata")
	}
}
func TestRepositoryMutationFailures(t *testing.T) {
	for _, name := range []string{"begin", "request_read", "record_read", "corrupt_receipt", "corrupt_record", "set1", "set2", "set3", "commit", "overflow"} {
		t.Run(name, func(t *testing.T) {
			r, d := newFakeRepo()
			switch name {
			case "begin":
				d.beginErr = injected
			case "request_read":
				d.getFail = "request/"
			case "record_read":
				d.getFail = "record/"
			case "corrupt_receipt":
				d.data[string(r.key("request", "req"))] = []byte("{")
			case "corrupt_record":
				d.data[string(r.key("record", "a"))] = []byte("{")
			case "set1":
				d.setFail = 1
			case "set2":
				d.setFail = 2
			case "set3":
				d.setFail = 3
			case "commit":
				d.commitErr = injected
			case "overflow":
				seed(d, r.key("record", "a"), Record{ID: "a", Revision: math.MaxUint64})
			}
			before := len(d.data)
			if _, e := r.Mutate(context.Background(), "a", Mutation{RequestID: "req", Vector: []float64{1, 2}}, false); e == nil {
				t.Fatal("expected error")
			}
			if len(d.data) != before || d.commits != 0 {
				t.Fatal("partial commit")
			}
		})
	}
	r, d := newFakeRepo()
	for _, m := range []Mutation{{}, {RequestID: strings.Repeat("x", 257)}, {RequestID: "x", Vector: []float64{1}}} {
		if _, e := r.Mutate(context.Background(), "a", m, false); e == nil {
			t.Fatal("invalid accepted")
		}
	}
	if _, e := r.Mutate(context.Background(), "", Mutation{RequestID: "a"}, true); e == nil {
		t.Fatal("empty ID")
	}
	if _, e := r.Mutate(context.Background(), "a", Mutation{RequestID: "a", Vector: []float64{math.NaN()}}, true); e == nil {
		t.Fatal("non-serializable delete body")
	}
	if len(d.data) != 0 {
		t.Fatal("validation wrote data")
	}
}
func TestRepositoryUnknownCommitRetry(t *testing.T) {
	r, d := newFakeRepo()
	d.commitErr = injected
	d.committedDespiteError = true
	m := Mutation{RequestID: "req", Vector: []float64{1, 2}}
	if _, e := r.Mutate(context.Background(), "a", m, false); e == nil || !strings.Contains(e.Error(), "retry same request_id") {
		t.Fatal(e)
	}
	d.commitErr = nil
	rec, e := r.Mutate(context.Background(), "a", m, false)
	if e != nil || rec.Revision != 1 || d.commits != 1 {
		t.Fatal(rec, e)
	}
}
func TestRepositoryScanFailures(t *testing.T) {
	for _, name := range []string{"begin", "iter", "next", "corrupt", "limit", "canceled", "empty"} {
		t.Run(name, func(t *testing.T) {
			r, d := newFakeRepo()
			seed(d, r.key("outbox", "a"), Record{ID: "a", Revision: 1})
			seed(d, r.key("outbox", "b"), Record{ID: "b", Revision: 1})
			ctx := context.Background()
			switch name {
			case "begin":
				d.beginErr = injected
			case "iter":
				d.iterErr = injected
			case "next":
				d.nextErr = injected
			case "corrupt":
				d.data[string(r.key("outbox", "b"))] = []byte("bad")
			case "limit":
				r.scanLimit = 1
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "empty":
				d.data = map[string][]byte{}
			}
			rows, e := r.Scan(ctx, "outbox")
			if name == "empty" {
				if e != nil || len(rows) != 0 {
					t.Fatal(rows, e)
				}
			} else if e == nil || rows != nil {
				t.Fatal("partial scan escaped", rows, e)
			}
		})
	}
	r, d := newFakeRepo()
	r.scanLimit = 2
	for _, id := range []string{"a", "b"} {
		seed(d, r.key("outbox", id), Record{ID: id, Revision: 1})
	}
	seed(d, r.key("record", "outside"), Record{ID: "outside", Revision: 1})
	if rows, e := r.Scan(context.Background(), "outbox"); e != nil || len(rows) != 2 {
		t.Fatal(rows, e)
	}
}
func TestRepositoryManifest(t *testing.T) {
	ctx := context.Background()
	r, d := newFakeRepo()
	if m, e := r.Manifest(ctx); e != nil || m != nil {
		t.Fatal(m, e)
	}
	m := Manifest{Generation: "g1", Dimension: 2}
	if e := r.Publish(ctx, "", m); e != nil {
		t.Fatal(e)
	}
	if e := r.Publish(ctx, "", Manifest{Generation: "g2"}); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if e := r.Publish(ctx, "g1", Manifest{Generation: "g2"}); e != nil {
		t.Fatal(e)
	}
	got, e := r.Manifest(ctx)
	if e != nil || got.Generation != "g2" {
		t.Fatal(got, e)
	}
	for _, name := range []string{"begin", "get", "json", "set", "commit"} {
		t.Run(name, func(t *testing.T) {
			rr, dd := newFakeRepo()
			switch name {
			case "begin":
				dd.beginErr = injected
			case "get":
				dd.getFail = "manifest"
			case "json":
				dd.data["test/manifest"] = []byte("bad")
			case "set":
				dd.setFail = 1
			case "commit":
				dd.commitErr = injected
			}
			if e := rr.Publish(ctx, "", m); e == nil {
				t.Fatal("expected publish error")
			}
			if name == "begin" || name == "get" || name == "json" {
				if _, e := rr.Manifest(ctx); e == nil {
					t.Fatal("expected read error")
				}
			}
		})
	}
	d.getFail = "record"
	if _, e := r.Get(ctx, "a"); e == nil {
		t.Fatal("read error ignored")
	}
	d.beginErr = injected
	if _, e := r.Get(ctx, "a"); e == nil {
		t.Fatal("begin error ignored")
	}
}
