package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func putLogRecords(t *testing.T, r *Repository, n int) {
	t.Helper()
	for j := 0; j < n; j++ {
		if _, e := r.Mutate(context.Background(), fmt.Sprint(j), Mutation{RequestID: fmt.Sprint(j), Vector: []float64{float64(j), 0}}, false); e != nil {
			t.Fatal(e)
		}
	}
}
func TestOrderedLogBatchesAndRetry(t *testing.T) {
	ctx := context.Background()
	r, d := newFakeRepo()
	putLogRecords(t, r, 600)
	var cursor uint64
	total := 0
	for cursor < 600 {
		batch, e := r.ReadEvents(ctx, cursor, 0, 256)
		if e != nil || batch.Head != 600 || len(batch.Events) > 256 {
			t.Fatal(batch, e)
		}
		for _, v := range batch.Events {
			cursor++
			if v.Sequence != cursor {
				t.Fatal(v)
			}
			total++
		}
	}
	if total != 600 {
		t.Fatal(total)
	}
	batch, e := r.ReadEvents(ctx, cursor, 0, 256)
	if e != nil || len(batch.Events) != 0 {
		t.Fatal(batch, e)
	}
	if _, e = r.Mutate(ctx, "0", Mutation{RequestID: "0", Vector: []float64{0, 0}}, false); e != nil {
		t.Fatal(e)
	}
	batch, _ = r.ReadEvents(ctx, 599, 0, 256)
	if batch.Head != 600 || len(batch.Events) != 1 {
		t.Fatal(batch)
	}
	delete(d.data, string(r.logKey(599)))
	if _, e = r.ReadEvents(ctx, 598, 0, 256); e == nil {
		t.Fatal("gap skipped")
	}
}
func TestLogFailureAndBounds(t *testing.T) {
	for _, name := range []string{"head-read", "bad-head", "floor-read", "iter", "next", "bad-event", "bad-sequence", "gap", "set4", "head-overflow"} {
		t.Run(name, func(t *testing.T) {
			r, d := newFakeRepo()
			ctx := context.Background()
			if name == "set4" {
				d.setFail = 4
				if _, e := r.Mutate(ctx, "a", Mutation{RequestID: "a", Vector: []float64{0, 0}}, false); e == nil || len(d.data) != 0 {
					t.Fatal("partial transaction", e)
				}
				return
			}
			if name == "head-overflow" {
				d.data[r.Prefix+"log-head"] = []byte("18446744073709551615")
				if _, e := r.Mutate(ctx, "a", Mutation{RequestID: "a", Vector: []float64{0, 0}}, false); e == nil {
					t.Fatal("overflow")
				}
				return
			}
			putLogRecords(t, r, 2)
			switch name {
			case "head-read":
				d.getFail = "log-head"
			case "bad-head":
				d.data[r.Prefix+"log-head"] = []byte("bad")
			case "floor-read":
				d.getFail = "log-floor"
			case "iter":
				d.iterErr = injected
			case "next":
				d.nextErr = injected
			case "bad-event":
				d.data[string(r.logKey(1))] = []byte("bad")
			case "bad-sequence":
				seed(d, r.logKey(1), LogEvent{2, Record{ID: "a", Revision: 1, Vector: []float64{0, 0}}})
			case "gap":
				delete(d.data, string(r.logKey(1)))
			}
			b, e := r.ReadEvents(ctx, 0, 0, 256)
			if e == nil || len(b.Events) != 0 {
				t.Fatal("partial batch", b, e)
			}
		})
	}
	r, _ := newFakeRepo()
	for _, limit := range []int{0, 257} {
		if _, e := r.ReadEvents(context.Background(), 0, 0, limit); e == nil {
			t.Fatal(limit)
		}
		if _, e := r.PruneEvents(context.Background(), limit); e == nil {
			t.Fatal(limit)
		}
	}
}
func TestCheckpointCursorPruneAndRecovery(t *testing.T) {
	ctx := context.Background()
	r, d := newFakeRepo()
	objects := &Objects{Client: &fakeBlobs{exists: true}, Bucket: "test"}
	putLogRecords(t, r, 10)
	s, e := NewService(ctx, r, objects)
	if e != nil || s.Cursor() != 10 {
		t.Fatal(e)
	}
	if floor, e := s.Prune(ctx); e != nil || floor != 0 {
		t.Fatal(floor, e)
	}
	m, e := s.Checkpoint(ctx)
	if e != nil || m.Offset != 10 || m.LogVersion != LogVersion {
		t.Fatal(m, e)
	}
	if _, e = r.Mutate(ctx, "0", Mutation{RequestID: "delete"}, true); e != nil {
		t.Fatal(e)
	}
	if floor, e := r.PruneEvents(ctx, 4); e != nil || floor != 4 {
		t.Fatal(floor, e)
	}
	if floor, e := s.Prune(ctx); e != nil || floor != 10 {
		t.Fatal(floor, e)
	}
	if _, ok := d.data[string(r.logKey(11))]; !ok {
		t.Fatal("uncheckpointed event deleted")
	}
	if _, e = r.ReadEvents(ctx, 0, 0, 256); !errors.Is(e, ErrLogCompacted) {
		t.Fatal(e)
	}
	recovered, e := NewService(ctx, r, objects)
	if e != nil || recovered.Cursor() != 11 {
		t.Fatal(e)
	}
	hits, e := recovered.Search(Search{Vector: []float64{0, 0}, K: 100, Exact: true})
	if e != nil || len(hits) != 9 {
		t.Fatal(hits, e)
	}
	// An already-running lagging consumer must recover from one authoritative snapshot.
	lag := &Service{Repo: r, Objects: objects, Index: mustGraph(t, 2)}
	if e = lag.Sync(ctx); e != nil || lag.Cursor() != 11 {
		t.Fatal(e)
	}
	stale := m
	stale.Offset = 9
	if e = r.Publish(ctx, m.Generation, stale); e == nil {
		t.Fatal("checkpoint regressed")
	}
	// Idempotency survives event deletion.
	if _, e = r.Mutate(ctx, "0", Mutation{RequestID: "0", Vector: []float64{0, 0}}, false); e != nil {
		t.Fatal(e)
	}
	rec, _ := r.Get(ctx, "0")
	if !rec.Deleted {
		t.Fatal("old request resurrected record")
	}
	// Cursor binding is part of object validation, not just manifest metadata.
	state, e := objects.LoadState(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	state.Offset++
	raw, _ := json.Marshal(state)
	objects.Client.(*fakeBlobs).data = raw
	if _, e = NewService(ctx, r, objects); e == nil {
		t.Fatal("altered cursor accepted")
	}
}
func TestLegacyBootstrapAndIncrementalSync(t *testing.T) {
	ctx := context.Background()
	r, d := newFakeRepo()
	seed(d, r.key("record", "legacy"), Record{ID: "legacy", Revision: 5, Vector: []float64{1, 0}})
	seed(d, r.key("outbox", "old"), Record{ID: "legacy", Revision: 1, Vector: []float64{9, 0}})
	s, e := NewService(ctx, r, &Objects{Client: &fakeBlobs{exists: true}, Bucket: "test"})
	if e != nil {
		t.Fatal(e)
	}
	if s.Cursor() != 0 {
		t.Fatal(s.Cursor())
	}
	putLogRecords(t, r, 300)
	if e = s.Sync(ctx); e != nil || s.Cursor() != 300 {
		t.Fatal(s.Cursor(), e)
	}
	if _, e = r.Mutate(ctx, "legacy", Mutation{RequestID: "del-legacy"}, true); e != nil {
		t.Fatal(e)
	}
	if e = s.Sync(ctx); e != nil || s.Cursor() != 301 {
		t.Fatal(e)
	}
	for _, rec := range s.Index.Snapshot() {
		if rec.ID == "legacy" && (!rec.Deleted || rec.Revision != 6) {
			t.Fatal(rec)
		}
	}
}

func TestPruneFailureDoesNotAdvanceFloor(t *testing.T) {
	for _, fail := range []int{1, 2, 3} {
		r, d := newFakeRepo()
		putLogRecords(t, r, 2)
		seed(d, []byte(r.Prefix+"manifest"), Manifest{LogVersion: LogVersion, Offset: 2})
		d.setFail = fail
		if _, e := r.PruneEvents(context.Background(), 256); e == nil {
			t.Fatal("failure ignored", fail)
		}
		if _, ok := d.data[r.Prefix+"log-floor"]; ok {
			t.Fatal("floor advanced")
		}
		if _, ok := d.data[string(r.logKey(1))]; !ok {
			t.Fatal("partial delete")
		}
	}
}
func TestSchemaOfflineUpgrade(t *testing.T) {
	r, d := newFakeRepo()
	d.data[r.Prefix+"schema"] = []byte("v1:dimension=2:metric=l2")
	if e := r.ensureSchema(context.Background()); e != nil {
		t.Fatal(e)
	}
	if string(d.data[r.Prefix+"schema"]) != "v2:dimension=2:metric=l2:log=1" {
		t.Fatal("not upgraded")
	}
}

type countingLogRepo struct {
	*Repository
	calls, events, failAt int
}

func (r *countingLogRepo) ReadEvents(ctx context.Context, after, through uint64, limit int) (LogBatch, error) {
	r.calls++
	if r.calls == r.failAt {
		return LogBatch{}, injected
	}
	b, e := r.Repository.ReadEvents(ctx, after, through, limit)
	r.events += len(b.Events)
	return b, e
}
func TestIncrementalRetryDoesNotRescanHistory(t *testing.T) {
	ctx := context.Background()
	base, _ := newFakeRepo()
	r := &countingLogRepo{Repository: base}
	s, e := NewService(ctx, r, &Objects{Client: &fakeBlobs{exists: true}, Bucket: "test"})
	if e != nil {
		t.Fatal(e)
	}
	putLogRecords(t, base, 600)
	r.calls = 0
	r.events = 0
	r.failAt = 2
	if e = s.Sync(ctx); !errors.Is(e, injected) || s.Cursor() != 256 || r.events != 256 {
		t.Fatal(s.Cursor(), r.events, e)
	}
	r.failAt = 0
	if e = s.Sync(ctx); e != nil || s.Cursor() != 600 || r.events != 600 {
		t.Fatal(s.Cursor(), r.events, e)
	}
	if e = s.Sync(ctx); e != nil || r.events != 600 {
		t.Fatal("history reread", r.events, e)
	}
}
