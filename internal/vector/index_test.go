package vector

import (
	"math"
	"testing"
)

func TestReplayDoesNotResurrectAndFiltersBeforeTopK(t *testing.T) {
	idx := NewIndex(2)
	idx.Apply([]Record{{ID: "a", Revision: 3, Deleted: true}, {ID: "a", Revision: 1, Vector: []float64{0, 0}}, {ID: "b", Revision: 1, Vector: []float64{1, 0}, Payload: map[string]string{"type": "x"}}, {ID: "c", Revision: 1, Vector: []float64{2, 0}, Payload: map[string]string{"type": "y"}}})
	hits, e := idx.Search(Search{Vector: []float64{0, 0}, K: 1, Filter: map[string]string{"type": "y"}, MinVisible: &Token{"a", 3}})
	if e != nil || len(hits) != 1 || hits[0].ID != "c" {
		t.Fatalf("hits=%v err=%v", hits, e)
	}
	idx.Apply([]Record{{ID: "a", Revision: 4, Vector: []float64{0, 0}}})
	hits, e = idx.Search(Search{Vector: []float64{0, 0}, K: 1})
	if e != nil || hits[0].ID != "a" {
		t.Fatal(hits, e)
	}
}
func TestValidationAndMissingFilter(t *testing.T) {
	if ValidateVector([]float64{math.NaN()}, 1) == nil {
		t.Fatal("NaN accepted")
	}
	i := NewIndex(1)
	i.Apply([]Record{{ID: "x", Revision: 1, Vector: []float64{0}}})
	hits, e := i.Search(Search{Vector: []float64{0}, K: 1, Filter: map[string]string{"missing": ""}})
	if e != nil || len(hits) != 0 {
		t.Fatal(hits, e)
	}
	if _, e = i.Search(Search{Vector: []float64{0}, K: 1, MinVisible: &Token{"x", 2}}); e == nil {
		t.Fatal("future token accepted")
	}
}

func TestIndexBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    []float64
		d    int
		bad  bool
	}{
		{"valid", []float64{1, -1}, 2, false}, {"bound", []float64{1e15, -1e15}, 2, false},
		{"dimension", []float64{1}, 2, true}, {"nil", nil, 2, true}, {"nan", []float64{math.NaN()}, 1, true},
		{"infinity", []float64{math.Inf(1)}, 1, true}, {"overflow", []float64{1e16}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if e := ValidateVector(tc.v, tc.d); (e != nil) != tc.bad {
				t.Fatal(e)
			}
		})
	}
	i := NewIndex(2)
	for _, q := range []Search{{Vector: []float64{1}, K: 1}, {Vector: []float64{1, 2}, K: 0}, {Vector: []float64{1, 2}, K: 1001}, {Vector: []float64{1, 2}, K: 1, MinVisible: &Token{}}, {Vector: []float64{1, 2}, K: 1, MinVisible: &Token{ID: "a"}}} {
		if _, e := i.Search(q); e == nil {
			t.Fatal("invalid query accepted", q)
		}
	}
	hits, e := i.Search(Search{Vector: []float64{0, 0}, K: 1})
	if e != nil || hits == nil || len(hits) != 0 {
		t.Fatal(hits, e)
	}
}
func TestIndexDistanceTieAndCopies(t *testing.T) {
	i := NewIndex(2)
	r := Record{ID: "a", Revision: 1, Vector: []float64{3, 4}, Payload: map[string]string{"tag": "original"}}
	i.Apply([]Record{r, {ID: "b", Revision: 1, Vector: []float64{-3, -4}}})
	r.Vector[0] = 999
	r.Payload["tag"] = "changed"
	hits, e := i.Search(Search{Vector: []float64{0, 0}, K: 10})
	if e != nil || len(hits) != 2 || hits[0].ID != "a" || hits[0].Distance != 25 || hits[0].Payload["tag"] != "original" {
		t.Fatal(hits, e)
	}
	hits[0].Payload["tag"] = "mutated-result"
	snapshot := i.Snapshot()
	snapshot[0].Vector[0] = 999
	snapshot[0].Payload["tag"] = "mutated-snapshot"
	hits, e = i.Search(Search{Vector: []float64{0, 0}, K: 1})
	if e != nil || hits[0].Distance != 25 || hits[0].Payload["tag"] != "original" {
		t.Fatal("external mutation reached index", hits, e)
	}
	i.Apply([]Record{{ID: "a", Revision: 1, Deleted: true}})
	hits, _ = i.Search(Search{Vector: []float64{0, 0}, K: 10})
	if len(hits) != 2 {
		t.Fatal("equal revision changed state")
	}
}
