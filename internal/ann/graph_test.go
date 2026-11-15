package ann

import (
	"reflect"
	"sync"
	"testing"
)

func TestGraphRoundTrip(t *testing.T) {
	g, err := New(2, 16, 200)
	if err != nil {
		t.Fatal(err)
	}
	for id, v := range [][]float32{{1, 0}, {0, 1}, {10, 10}} {
		if err = g.Add(uint64(id+1), v); err != nil {
			t.Fatal(err)
		}
	}
	check := func(g *Graph) {
		t.Helper()
		ids, count, err := g.Search([]float32{1, 0}, 1, 32)
		if err != nil || len(ids) != 1 || ids[0] != 1 || count < 1 {
			t.Fatal(ids, count, err)
		}
	}
	check(g)
	data, err := g.Export()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := New(2, 16, 200)
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.Import(data); err != nil {
		t.Fatal(err)
	}
	check(restored)
	if err = restored.Add(4, []float32{1, 0.01}); err != nil {
		t.Fatal(err)
	}
	check(restored)
}
func TestArguments(t *testing.T) {
	for _, args := range [][3]int{{0, 16, 200}, {2, 1, 200}, {2, 16, 1}} {
		if _, err := New(args[0], args[1], args[2]); err == nil {
			t.Fatal(args)
		}
	}
	g, _ := New(2, 16, 200)
	if err := g.Add(1, nil); err == nil {
		t.Fatal("missing dimension check")
	}
	for _, args := range [][2]int{{0, 32}, {1, 0}, {100001, 32}} {
		if _, _, err := g.Search([]float32{0, 0}, args[0], args[1]); err == nil {
			t.Fatal(args)
		}
	}
	if _, _, err := g.Search(nil, 1, 32); err == nil {
		t.Fatal("missing dimension check")
	}
	ids, _, err := g.Search([]float32{0, 0}, 1, 32)
	if err != nil || len(ids) != 0 {
		t.Fatal(ids, err)
	}
}

func TestClose(t *testing.T) {
	g, err := New(2, 16, 200)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
	g.Close()
	if g.h.p != nil {
		t.Fatal("native handle retained")
	}
}

func TestConcurrentQueryLocalState(t *testing.T) {
	g, err := New(8, 16, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for j := 0; j < 500; j++ {
		v := make([]float32, 8)
		for k := range v {
			v[k] = float32((j * (k + 3)) % 97)
		}
		if err = g.Add(uint64(j), v); err != nil {
			t.Fatal(err)
		}
	}
	q := []float32{3, 6, 9, 12, 15, 18, 21, 24}
	var wg sync.WaitGroup
	for _, ef := range []int{16, 64, 128, 256} {
		want, count, err := g.Search(q, 10, ef)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(ef, count int, want []uint64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				got, n, e := g.Search(q, 10, ef)
				if e != nil || n != count || !reflect.DeepEqual(got, want) {
					t.Error("query-local state leaked", ef, n, count, e)
					return
				}
			}
		}(ef, count, want)
	}
	wg.Wait()
}
