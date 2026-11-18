package vector

import (
	"errors"
	"fmt"
	"testing"
)

func TestRoutedBatchBoundsAndFairness(t *testing.T) {
	for _, large := range []bool{false, true} {
		in := newRoutedInput(&ClusterService{PartitionedService: &PartitionedService{layout: PartitionLayout{DataPartitions: 6}}})
		for id := DataPartitionID(0); id < 6; id++ {
			size := 100
			if large {
				size = dispatchBytes / 2
			}
			in.queue[id] = &queuedDelivery{Delivery: Delivery{Source: id, Events: make([]LogEvent, 32)}, bytes: size}
			in.bytes += size
		}
		first := in.takeBatch()
		want := 4
		if large {
			want = 2
		}
		if len(first) != want || first[0].Source != 1 || first[len(first)-1].Source != DataPartitionID(want) {
			t.Fatal("batch limit or rotation", len(first), first)
		}
		seen := map[DataPartitionID]bool{}
		for _, q := range first {
			seen[q.Source] = true
		}
		for in.hasQueued() {
			for _, q := range in.takeBatch() {
				if seen[q.Source] {
					t.Fatal("duplicate source", q.Source)
				}
				seen[q.Source] = true
			}
		}
		if len(seen) != 6 || in.bytes != 0 || len(in.takeBatch()) != 0 {
			t.Fatal("lost input or accounting", seen, in.bytes)
		}
	}
}

// Two independent source logs feeding the same Index, with actual admission,
// durable commit, publication and graph replay (including a revoked Index leader).
func TestRoutedBatchAtomicSourcesAndReplay(t *testing.T) {
	for _, scenario := range []string{"normal", "revoke", "uncertain_commit"} {
		t.Run(scenario, func(t *testing.T) {
			revoke := scenario == "revoke"
			f, c, owner0 := routedFixture(t)
			c.layout.DataPartitions = 2
			c.router = clusterRouter{IdentityPartitionRouter{c.layout}, c.layout.Routing}
			data1 := *f.r
			data1.Prefix += "source1/"
			c.data[1] = &data1
			if err := f.c.Schedule(f.ctx, f.master, sourceLayout(2), f.nodes); err != nil {
				t.Fatal(err)
			}
			a, err := f.c.Assignment(f.ctx, sourceID(1))
			if err != nil {
				t.Fatal(err)
			}
			var node ClusterNode
			for _, n := range f.nodes {
				if n.ID == a.Node {
					node = n
				}
			}
			owner1, err := f.c.Acquire(f.ctx, sourceID(1), node)
			if err != nil {
				t.Fatal(err)
			}
			owner1, err = f.c.Update(f.ctx, owner1, true, false)
			if err != nil {
				t.Fatal(err)
			}
			g := f.services[0].graphReplica
			var inputs []Delivery
			for source, owner := range []Ownership{owner0, owner1} {
				id := ""
				for n := 0; ; n++ {
					id = fmt.Sprintf("record-%d", n)
					if c.router.DataForID(id) == DataPartitionID(source) {
						break
					}
				}
				r, err := c.data[DataPartitionID(source)].Mutate(f.ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)
				if err != nil {
					t.Fatal(err)
				}
				q := Delivery{Source: DataPartitionID(source), Owner: owner, RouteVersion: 1, Through: 1, Events: []LogEvent{{Sequence: 1, Record: r}}}
				inputs = append(inputs, q)
				if err = g.enqueue(f.ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			if revoke {
				if _, err = f.c.Update(f.ctx, g.journal.owner, false, true); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "uncertain_commit" {
				head, err := g.journal.Claim(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				f.db.commitErr, f.db.committedDespiteError = injected, true
				err = g.commitRouted(f.ctx, f.services[0], head)
				f.db.commitErr, f.db.committedDespiteError = nil, false
				if !errors.Is(err, injected) || g.input.hasQueued() {
					t.Fatal("uncertain commit not exercised", err)
				}
				pending, err := g.journal.Head(f.ctx)
				if err != nil || pending.Committed != pending.Visible || pending.Sources["0"] != 1 || pending.Sources["1"] != 1 {
					t.Fatal("missing durable batch", pending, err)
				}
			}
			err = f.services[0].Sync(f.ctx)
			if revoke && !errors.Is(err, ErrFenced) || !revoke && err != nil {
				t.Fatal(err)
			}
			head, err := g.journal.Head(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if revoke {
				want = 0
			}
			if head.Committed.Sequence != 2+want || head.Visible != head.Committed || head.Sources["0"] != want || head.Sources["1"] != want {
				t.Fatal("sources committed separately or partially", head)
			}
			if err = f.services[1].Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			equalGraph(t, f.services[0].Index, f.services[1].Index)
			if len(f.services[0].Index.records) != int(2*want) {
				t.Fatal("lost record")
			}
			if !revoke {
				for _, q := range inputs {
					if err = g.enqueue(f.ctx, q); err != nil {
						t.Fatal(err)
					}
				}
				if err = f.services[0].Sync(f.ctx); err != nil {
					t.Fatal(err)
				}
				after, err := g.journal.Head(f.ctx)
				if err != nil || after.Committed != head.Committed {
					t.Fatal("retry rebuilt committed inputs", after, err)
				}
			}
		})
	}
}
