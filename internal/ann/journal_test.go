package ann

import (
	"bytes"
	"testing"
)

func TestGraphPatchReplicationAndPromotion(t *testing.T) {
	newGraph := func() *Graph {
		g, e := New(8, 16, 200)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(g.Close)
		return g
	}
	a, b := newGraph(), newGraph()
	check := func(x, y *Graph) {
		t.Helper()
		xb, e := x.Export()
		if e != nil {
			t.Fatal(e)
		}
		yb, e := y.Export()
		if e != nil {
			t.Fatal(e)
		}
		xm, _ := x.Metadata()
		ym, _ := y.Metadata()
		if !bytes.Equal(xb, yb) || !bytes.Equal(xm, ym) {
			t.Fatal("replica graph/state differ")
		}
	}
	for batch := 0; batch < 6; batch++ {
		leader, follower := a, b
		if batch >= 3 {
			leader, follower = b, a
		}
		before, e := leader.Export()
		if e != nil {
			t.Fatal(e)
		}
		if e = leader.BeginPatch(); e != nil {
			t.Fatal(e)
		}
		for j := 0; j < 100; j++ {
			id := batch*100 + j
			v := make([]float32, 8)
			for k := range v {
				v[k] = float32(id%7)*10 + float32((id*(k+3))%97)/100
			}
			if e = leader.Add(uint64(id), v); e != nil {
				t.Fatal(e)
			}
		}
		after, e := leader.Export()
		if e != nil {
			t.Fatal(e)
		}
		patch, e := leader.FinishPatch()
		if e != nil {
			t.Fatal(e)
		}
		rolled, _ := leader.Export()
		if !bytes.Equal(before, rolled) {
			t.Fatal("rollback changed graph")
		}
		check(a, b)
		for _, g := range []*Graph{leader, follower} {
			if e = g.ApplyPatch(patch, false); e != nil {
				t.Fatal(e)
			}
			got, _ := g.Export()
			if !bytes.Equal(got, after) {
				t.Fatal("patch not identical to leader")
			}
		}
		check(a, b)
		if e = b.ApplyPatch(patch, false); e == nil {
			t.Fatal("duplicate native patch accepted")
		}
		if e = b.ApplyPatch(patch, true); e != nil {
			t.Fatal(e)
		}
		for _, bad := range [][]byte{patch[:len(patch)-1], append(append([]byte{}, patch...), 0), {0}} {
			if e = b.ApplyPatch(bad, false); e == nil {
				t.Fatal("bad patch accepted")
			}
			got, _ := b.Export()
			if !bytes.Equal(got, before) {
				t.Fatal("bad patch mutated graph")
			}
		}
		if e = b.ApplyPatch(patch, false); e != nil {
			t.Fatal(e)
		}
		if batch == 2 {
			snapshot, _ := b.Export()
			meta, _ := b.Metadata()
			restored := newGraph()
			if e = restored.Import(snapshot); e != nil {
				t.Fatal(e)
			}
			if e = restored.RestoreMetadata(meta); e != nil {
				t.Fatal(e)
			}
			b = restored
			check(a, b)
		}
	}
}
