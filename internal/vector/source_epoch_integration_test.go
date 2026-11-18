package vector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

type epochTiKVFixture struct {
	ctx      context.Context
	r        *Repository
	c        *ClusterService
	services []*Service
	source   Ownership
	node     ClusterNode
}

func newEpochTiKVFixture(t *testing.T, indexes int) *epochTiKVFixture {
	t.Helper()
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "source-epochs/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Client.Close() })
	r.DataSnapshots = true
	coord := &Coordinator{root: r, ttl: 60000, referenceLogs: true, data: map[DataPartitionID]*Repository{0: r}}
	node := ClusterNode{ID: "worker", Session: "s", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	if err = coord.Heartbeat(ctx, node); err != nil {
		t.Fatal(err)
	}
	acquire := func(id IndexPartitionID) Ownership {
		testAssignment(t, coord, node, id)
		o, e := coord.Acquire(ctx, id, node)
		if e != nil {
			t.Fatal(e)
		}
		o, e = coord.Update(ctx, o, true, false)
		if e != nil {
			t.Fatal(e)
		}
		return o
	}
	source := acquire(sourceID(0))
	layout, _ := newLayout(1)
	layout.Bindings = nil
	layout.Routing = &ClusterRouting{Version: 1}
	objects := partitionObjects()
	c := &ClusterService{root: r, coordinator: coord, objects: objects, graphReplication: true, consumerRetention: true, PartitionedService: &PartitionedService{layout: layout, data: map[DataPartitionID]*Repository{0: r}, dimension: 2}}
	f := &epochTiKVFixture{ctx: ctx, r: r, c: c, source: source, node: node}
	for n := 0; n < indexes; n++ {
		id := IndexPartitionID(fmt.Sprintf("index-%04d", n))
		replica := replicaID(id, node)
		owner := acquire(replica)
		tx, e := r.begin()
		if e != nil {
			t.Fatal(e)
		}
		e = putJSON(tx, coord.planKey(id), ReplicaPlan{Publisher: replica, Serving: []Assignment{{Partition: replica, Node: node.ID, Session: node.Session}}})
		if e == nil {
			e = tx.Commit(ctx)
		}
		_ = tx.Rollback()
		if e != nil {
			t.Fatal(e)
		}
		c.layout.Bindings = append(c.layout.Bindings, PartitionBinding{Data: 0, Index: id})
		c.layout.Routing.Centers = append(c.layout.Routing.Centers, []float64{float64(n) * 10, 0})
		repo := *r
		repo.IndexPrefix = r.Prefix + string(id) + "/"
		repo.Fence = coord.ReplicaFence(id, owner)
		s, e := newGraphService(ctx, &repo, objects, graphJournal{coord, id, owner})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(s.Index.graph.Close)
		s.graphReplica.retention = true
		s.graphReplica.input = newRoutedInput(c)
		f.services = append(f.services, s)
	}
	c.router = clusterRouter{IdentityPartitionRouter{c.layout}, c.layout.Routing}
	return f
}
func (f *epochTiKVFixture) delivery(t *testing.T, deleted bool) Delivery {
	t.Helper()
	record, err := f.r.Mutate(f.ctx, "v", Mutation{RequestID: "write", Vector: []float64{0, 0}}, deleted)
	if err != nil {
		t.Fatal(err)
	}
	return Delivery{Source: 0, Owner: f.source, RouteVersion: 1, Through: 1, Events: []LogEvent{{Sequence: 1, Record: record}}}
}

// Holding the former shared pessimistic lock cannot stall either Index. The
// actual graph commits run concurrently, using independent Index fence keys.
func TestSourceEpochTiKVIndexesDoNotLockSharedSource(t *testing.T) {
	f := newEpochTiKVFixture(t, 2)
	q := f.delivery(t, true)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	// First-time epoch admission is independent too.
	for n, s := range f.services {
		wg.Add(1)
		go func(n int, s *Service) { defer wg.Done(); errs[n] = s.graphReplica.enqueue(f.ctx, q) }(n, s)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	holder, err := f.r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	native, ok := holder.(interface {
		SetPessimistic(bool)
		LockKeys(context.Context, *kv.LockCtx, ...[]byte) error
	})
	if !ok {
		t.Fatal("missing native transaction")
	}
	native.SetPessimistic(true)
	if err = native.LockKeys(f.ctx, kv.NewLockCtx(holder.StartTS(), kv.LockNoWait, time.Now()), f.c.coordinator.fenceKey(f.source.Partition)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	for n, s := range f.services {
		wg.Add(1)
		go func(n int, s *Service) { defer wg.Done(); errs[n] = s.Sync(ctx) }(n, s)
	}
	wg.Wait()
	for n, err := range errs {
		if err != nil {
			t.Fatal("Index depended on shared source lock", n, err)
		}
		h, err := f.services[n].graphReplica.journal.Head(ctx)
		if err != nil || h.Sources["0"] != 1 || h.SourceEpochs["0"] != q.Owner.Epoch {
			t.Fatal(h, err)
		}
	}
	_ = holder.Rollback()
	// A new epoch accepted by Index 0 must not fence Index 1 prematurely.
	if _, err = f.c.coordinator.Update(f.ctx, f.source, false, true); err != nil {
		t.Fatal(err)
	}
	newer, err := f.c.coordinator.Acquire(f.ctx, f.source.Partition, f.node)
	if err != nil {
		t.Fatal(err)
	}
	newer, err = f.c.coordinator.Update(f.ctx, newer, true, false)
	if err != nil {
		t.Fatal(err)
	}
	fresh := q
	fresh.Owner = newer
	if err = f.services[0].graphReplica.enqueue(f.ctx, fresh); err != nil {
		t.Fatal(err)
	}
	requireEpochRejection(t, f.services[0].graphReplica.enqueue(f.ctx, q), newer.Epoch, 1)
	if err = f.services[1].graphReplica.enqueue(f.ctx, q); err != nil {
		t.Fatal("epoch leaked across Indexes", err)
	}
}

func TestSourceEpochTiKVStaleSnapshotCannotAppendAfterBarrierOrGC(t *testing.T) {
	for _, gc := range []bool{false, true} {
		t.Run(fmt.Sprintf("gc_%t", gc), func(t *testing.T) {
			f := newEpochTiKVFixture(t, 1)
			a := f.services[0]
			g := a.graphReplica
			q := f.delivery(t, false)
			if err := g.enqueue(f.ctx, q); err != nil {
				t.Fatal(err)
			}
			h, err := g.journal.Head(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			delta, err := a.Index.GenerateDelta([]Record{q.Events[0].Record})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := referenceDelta(delta, []Delivery{q})
			if err != nil {
				t.Fatal(err)
			}
			// This transaction sees the old head and accepted epoch. Before prewrite,
			// another transaction advances the barrier or retires the exact data ref.
			staleRepo := *f.r
			staleCoord := *f.c.coordinator
			staleCoord.root = &staleRepo
			staleJournal := g.journal
			staleJournal.c = &staleCoord
			fired := false
			accepted := f.source.Epoch
			staleRepo.beginTxn = func() (transactionAPI, error) {
				tx, e := f.r.begin()
				return dispatchHookTx{transactionAPI: tx, beforeCommit: func() error {
					if fired {
						return nil
					}
					fired = true
					if !gc {
						if _, e := f.c.coordinator.Update(f.ctx, f.source, false, true); e != nil {
							return e
						}
						newer, e := f.c.coordinator.Acquire(f.ctx, f.source.Partition, f.node)
						if e != nil {
							return e
						}
						newer, e = f.c.coordinator.Update(f.ctx, newer, true, false)
						if e != nil {
							return e
						}
						accepted = newer.Epoch
						fresh := q
						fresh.Owner = newer
						return g.enqueue(f.ctx, fresh)
					}
					if e := a.Sync(f.ctx); e != nil {
						return e
					}
					if e := f.c.advanceCompleted(f.ctx, 0, f.source, 0, 1); e != nil {
						return e
					}
					if _, e := a.Checkpoint(f.ctx); e != nil {
						return e
					}
					// Checkpoint alone cannot retire data referenced by an unpruned graph log.
					if e := f.c.maintainData(f.ctx, 0, f.source); e != nil {
						return e
					}
					if _, e := f.r.ReadEvents(f.ctx, 0, 0, 1); e != nil {
						return e
					}
					if _, e := g.journal.Prune(f.ctx, g.repo, 256); e != nil {
						return e
					}
					return f.c.maintainData(f.ctx, 0, f.source)
				}}, e
			}
			_, err = staleJournal.commitPayload(f.ctx, h, 1, "delta", Manifest{}, []Delivery{q}, f.c, payload)
			if !fired || !tikverr.IsErrWriteConflict(err) {
				t.Fatal("stale snapshot was not serialized by Index fence", err)
			}
			_, err = staleJournal.commitPayload(f.ctx, h, 1, "delta", Manifest{}, []Delivery{q}, f.c, payload)
			if gc {
				if !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
				batch, e := f.r.ReadEvents(f.ctx, 0, 0, 1)
				if !errors.Is(e, ErrLogCompacted) || batch.Floor != 1 {
					t.Fatal("GC interleaving was not exercised", batch, e)
				}
				restored, e := newGraphService(f.ctx, g.repo, f.c.objects, g.journal)
				if e != nil {
					t.Fatal(e)
				}
				defer restored.Index.graph.Close()
				equalGraph(t, a.Index, restored.Index)
			} else {
				requireEpochRejection(t, err, accepted, 0)
			}
		})
	}
}
