package ann

import "testing"

func TestPatchLinks(t *testing.T) {
	g, e := New(2, 16, 200)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close()
	if e = g.BeginPatch(); e != nil {
		t.Fatal(e)
	}
	for n := 0; n < 50; n++ {
		if e = g.Add(uint64(n+1), []float32{float32(n % 5), float32(n)}); e != nil {
			t.Fatal(e)
		}
	}
	patch, e := g.FinishPatch()
	if e != nil {
		t.Fatal(e)
	}
	links, e := PatchLinks(patch)
	if e != nil || len(links) != 50 {
		t.Fatal(len(links), e)
	}
	for _, row := range links {
		if row.Label != uint64(row.InternalID)+1 || len(row.Layers) == 0 {
			t.Fatal(row)
		}
		for _, layer := range row.Layers {
			for _, neighbor := range layer {
				if neighbor >= 50 || neighbor == row.InternalID {
					t.Fatal(row)
				}
			}
		}
	}
	for _, bad := range [][]byte{nil, patch[:len(patch)-1], append(append([]byte{}, patch...), 1)} {
		if _, e = PatchLinks(bad); e == nil {
			t.Fatal("invalid patch accepted")
		}
	}
}
