package ann

import (
	"bytes"
	"testing"
)

func TestCompactPatchRoundTrip(t *testing.T) {
	g, err := New(64, 16, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for batch := 0; batch < 5; batch++ {
		vectors := map[uint64][]float32{}
		if err = g.BeginPatch(); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < 80; n++ {
			id := uint64(batch*80 + n + 1)
			v := make([]float32, 64)
			for d := range v {
				v[d] = float32((n%7)*100+d) + float32(batch)/100
			}
			vectors[id] = v
			if err = g.Add(id, v); err != nil {
				t.Fatal(err)
			}
		}
		full, err := g.FinishPatch()
		if err != nil {
			t.Fatal(err)
		}
		compact, err := CompactPatch(full, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(compact) >= len(full) {
			t.Fatal("vector bytes were retained")
		}
		expanded, err := ExpandPatch(compact, 64, func(id uint64) ([]float32, error) {
			if v, ok := vectors[id]; ok {
				return v, nil
			}
			return g.Vector(id)
		})
		if err != nil || !bytes.Equal(full, expanded) {
			t.Fatal("patch/RNG/adjacency changed", err)
		}
		if err = g.ApplyPatch(expanded, false); err != nil {
			t.Fatal(err)
		}
		if _, err = CompactPatch(full[:len(full)-1], 64); err == nil {
			t.Fatal("truncated patch accepted")
		}
		if _, err = ExpandPatch(compact, 64, func(uint64) ([]float32, error) { return []float32{1}, nil }); err == nil {
			t.Fatal("short vector accepted")
		}
	}
}
