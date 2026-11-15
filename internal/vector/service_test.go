package vector

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type fakeRepo struct {
	events                           []Record
	manifest                         *Manifest
	scanErr, manifestErr, publishErr error
	published                        int
	expected                         string
}

func (f *fakeRepo) Namespace() string                              { return "test/" }
func (f *fakeRepo) VectorDimension() int                           { return 2 }
func (f *fakeRepo) Scan(context.Context, string) ([]Record, error) { return f.events, f.scanErr }
func (f *fakeRepo) Manifest(context.Context) (*Manifest, error)    { return f.manifest, f.manifestErr }
func (f *fakeRepo) Publish(_ context.Context, expected string, m Manifest) error {
	f.published++
	f.expected = expected
	if f.publishErr != nil {
		return f.publishErr
	}
	f.manifest = &m
	return nil
}

type fakeObjects struct {
	records                     []Record
	ensureErr, loadErr, saveErr error
	saves                       int
}

func (f *fakeObjects) Ensure(context.Context) error                     { return f.ensureErr }
func (f *fakeObjects) Load(context.Context, Manifest) ([]Record, error) { return f.records, f.loadErr }
func (f *fakeObjects) Save(_ context.Context, ns string, d int, r []Record) (Manifest, error) {
	f.saves++
	f.records = r
	return Manifest{Generation: "new", Dimension: d, Key: ns + "snapshot"}, f.saveErr
}
func TestServiceStartupFailures(t *testing.T) {
	for _, name := range []string{"ensure", "manifest", "dimension", "load", "invalid_id", "invalid_revision", "invalid_vector", "scan"} {
		t.Run(name, func(t *testing.T) {
			r := &fakeRepo{manifest: &Manifest{Generation: "old", Dimension: 2}}
			o := &fakeObjects{}
			switch name {
			case "ensure":
				o.ensureErr = injected
			case "manifest":
				r.manifestErr = injected
			case "dimension":
				r.manifest.Dimension = 3
			case "load":
				o.loadErr = injected
			case "invalid_id":
				o.records = []Record{{Revision: 1}}
			case "invalid_revision":
				o.records = []Record{{ID: "a"}}
			case "invalid_vector":
				o.records = []Record{{ID: "a", Revision: 1, Vector: []float64{1}}}
			case "scan":
				r.scanErr = injected
			}
			if s, e := NewService(context.Background(), r, o); e == nil || s != nil {
				t.Fatal("invalid startup accepted", e)
			}
		})
	}
}
func TestServiceRecoveryAndCheckpoint(t *testing.T) {
	r := &fakeRepo{manifest: &Manifest{Generation: "old", Dimension: 2}, events: []Record{{ID: "a", Revision: 2, Deleted: true}, {ID: "b", Revision: 1, Vector: []float64{1, 2}}}}
	o := &fakeObjects{records: []Record{{ID: "a", Revision: 1, Vector: []float64{0, 0}}}}
	s, e := NewService(context.Background(), r, o)
	if e != nil {
		t.Fatal(e)
	}
	hits, e := s.Search(Search{Vector: []float64{0, 0}, K: 10, MinVisible: &Token{"a", 2}})
	if e != nil || len(hits) != 1 || hits[0].ID != "b" {
		t.Fatal(hits, e)
	}
	if _, e = s.Checkpoint(context.Background()); e != nil {
		t.Fatal(e)
	}
	if r.expected != "old" || r.published != 1 || len(o.records) != 2 || !o.records[0].Deleted {
		t.Fatal("checkpoint lost tombstone or CAS generation")
	}
}
func TestServiceFailureNeverPublishes(t *testing.T) {
	for _, name := range []string{"manifest", "scan", "save", "publish"} {
		t.Run(name, func(t *testing.T) {
			r := &fakeRepo{}
			o := &fakeObjects{}
			s, e := NewService(context.Background(), r, o)
			if e != nil {
				t.Fatal(e)
			}
			switch name {
			case "manifest":
				r.manifestErr = injected
			case "scan":
				r.scanErr = injected
			case "save":
				o.saveErr = injected
			case "publish":
				r.publishErr = ErrConflict
			}
			if _, e = s.Checkpoint(context.Background()); e == nil {
				t.Fatal("error ignored")
			}
			if r.manifest != nil {
				t.Fatal("published failed checkpoint")
			}
			if name != "publish" && r.published != 0 {
				t.Fatal("publish called before verification")
			}
		})
	}
}
func TestServiceSyncAtomicAndConcurrent(t *testing.T) {
	r := &fakeRepo{}
	s, e := NewService(context.Background(), r, &fakeObjects{})
	if e != nil {
		t.Fatal(e)
	}
	for _, bad := range []Record{{ID: "bad", Revision: 1, Vector: []float64{1}}, {Revision: 1, Deleted: true}, {ID: "bad", Deleted: true}} {
		r.events = []Record{{ID: "good", Revision: 1, Vector: []float64{1, 1}}, bad}
		if e = s.Sync(context.Background()); e == nil {
			t.Fatal("invalid event accepted")
		}
		if len(s.Index.Snapshot()) != 0 {
			t.Fatal("partially applied batch")
		}
	}
	r.events = []Record{{ID: "good", Revision: 1, Vector: []float64{1, 1}}}
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := s.Sync(context.Background()); e != nil {
				t.Error(e)
			}
			_, e := s.Search(Search{Vector: []float64{1, 1}, K: 1})
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	r.scanErr = errors.New("scan")
	if e = s.Sync(context.Background()); e == nil || len(s.Index.Snapshot()) != 1 {
		t.Fatal("failed scan changed view")
	}
}

func TestServiceReportsBaselineBeforeReplay(t *testing.T) {
	r := &fakeRepo{manifest: &Manifest{Generation: "old", Dimension: 2}}
	objects := &fakeObjects{records: []Record{{ID: "a", Revision: 1, Vector: []float64{1, 2}}}}
	observed := false
	s, e := newServiceWithProgress(context.Background(), r, objects, func(offset uint64) {
		observed = true
		r.scanErr = injected // Replay fails AFTER the successfully loaded baseline.
	})
	if !observed || !errors.Is(e, injected) || s != nil {
		t.Fatal(observed, s, e)
	}
	observed = false
	r.scanErr = nil
	objects.loadErr = injected
	_, e = newServiceWithProgress(context.Background(), r, objects, func(uint64) { observed = true })
	if observed || !errors.Is(e, injected) {
		t.Fatal("reported baseline before snapshot load", observed, e)
	}
}
