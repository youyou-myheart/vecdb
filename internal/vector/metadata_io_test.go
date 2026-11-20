package vector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type countedGraphTail struct {
	transactionAPI
	scans *[]string
}

func (t countedGraphTail) Last(ctx context.Context, start, end []byte) ([]byte, []byte, error) {
	*t.scans = append(*t.scans, string(start))
	return t.transactionAPI.Last(ctx, start, end)
}

func TestDispatchPollsOnlyRequiredGraphTails(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprint("delete_", deleted), func(t *testing.T) {
			f, c, owner := routedFixture(t)
			layout, _ := newLayout(3)
			layout.DataPartitions = 1
			layout.Routing = &ClusterRouting{Version: 1, Centers: [][]float64{{-10, 0}, {0, 0}, {10, 0}}}
			c.layout = layout
			c.router = clusterRouter{IdentityPartitionRouter{layout}, layout.Routing}
			if _, err := f.r.Mutate(f.ctx, "v", Mutation{RequestID: "write", Vector: []float64{10, 1}}, false); err != nil {
				t.Fatal(err)
			}
			if deleted {
				if _, err := f.r.Mutate(f.ctx, "v", Mutation{RequestID: "del"}, true); err != nil {
					t.Fatal(err)
				}
			}
			// Seed the source ACKs at compacted graph anchors. No delivery RPC should be necessary.
			var expected []string
			head := uint64(1)
			if deleted {
				head = 2
			}
			for i, b := range layout.Bindings {
				j := graphJournal{c: f.c, id: b.Index}
				seed(f.db, j.key("head"), GraphHead{Sources: map[string]uint64{"0": head}})
				if deleted || i == 2 {
					expected = append(expected, string(j.key("entries/")))
				}
			}
			begin := f.r.beginTxn
			var scanned []string
			f.r.beginTxn = func() (transactionAPI, error) { tx, err := begin(); return countedGraphTail{tx, &scanned}, err }
			s := &dataDispatcher{owner: owner}
			if err := c.dispatchSource(f.ctx, 0, s); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(scanned, expected) || s.completed != head {
				t.Fatalf("scans %v, expected %v, completed %d", scanned, expected, s.completed)
			}
		})
	}
}
func TestDispatcherIdleAndCachedWindowMetadataIO(t *testing.T) {
	f, c, o := routedFixture(t)
	lease := &appendLease{}
	confirmAppendGrant(t, lease, o.Epoch, o.Expires, time.Minute)
	s := &dataDispatcher{owner: o, writer: &mutationGroup{lease: lease}, wake: make(chan struct{}, 1)}
	c.dispatchers = map[DataPartitionID]*dataDispatcher{0: s}
	if err := c.loadDispatchWindow(f.ctx, 0, s); err != nil {
		t.Fatal(err)
	}
	begin := f.r.beginTxn
	calls := 0
	f.r.beginTxn = func() (transactionAPI, error) { calls++; return begin() }
	c.wakeDispatcher(0)
	select {
	case <-s.wake:
	default:
		t.Fatal("live dispatcher did not wake")
	}
	if calls != 0 {
		t.Fatal("idle wake opened TiKV transaction", calls)
	}
	f.write(t, "fresh", false)
	lease.head.Store(1)
	if err := c.loadDispatchWindow(f.ctx, 0, s); err != nil || len(s.window) != 1 {
		t.Fatal(err, s.window)
	}
	calls = 0
	if err := c.loadDispatchWindow(f.ctx, 0, s); err != nil || calls != 0 {
		t.Fatal("cached window reread", err, calls)
	}
	lease.StepDown()
	c.wakeDispatcher(0)
	if calls != 0 || len(s.wake) != 0 {
		t.Fatal("retired wake renewed authority", calls)
	}
}
func TestReplicaProgressReportsOnlyConfirmedChanges(t *testing.T) {
	f := newMigrationFixture(t)
	p := f.tick()
	f.ready(p, 0)
	o := f.owners[p.Serving[0].Partition]
	renewed, err := f.c.Resolve(f.ctx, o.Partition)
	if err != nil {
		t.Fatal(err)
	}
	j := graphJournal{f.c, "index-0000", o}
	service := &localPartition{owner: o}
	var metrics pipelineMetrics
	report := func(v uint64, pos GraphPosition) error {
		return service.reportGraphProgress(f.ctx, j, v, renewed, pos, &metrics)
	}
	first := GraphPosition{Sequence: 1, DataOffset: 2, Hash: "first"}
	before := f.db.commits
	if err = report(p.Version, first); err != nil || f.db.commits != before+1 {
		t.Fatal(err, f.db.commits-before)
	}
	renewed, err = f.c.Resolve(f.ctx, o.Partition)
	if err != nil || renewed.Offset != 2 {
		t.Fatal(err, renewed)
	}
	f.db.beginErr = injected // No transaction on an idle confirmed heartbeat.
	if err = report(p.Version, first); err != nil {
		t.Fatal(err)
	}
	f.db.beginErr = nil
	// An epoch-control entry can advance the graph sequence without a data change.
	next := GraphPosition{Sequence: 2, DataOffset: 2, Hash: "next"}
	f.db.commitErr = injected
	f.db.committedDespiteError = true
	if err = report(p.Version, next); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	f.db.commitErr = nil
	before = f.db.commits
	if err = report(p.Version, next); err != nil || f.db.commits != before+1 {
		t.Fatal("unknown outcome cached", err)
	}
	if got, err := j.Progress(f.ctx, o); err != nil || got != next {
		t.Fatal(got, err)
	}
	if err = report(p.Version+1, next); !errors.Is(err, ErrFenced) {
		t.Fatal("stale plan report accepted", err)
	}
}

// Open a real TiKV page snapshot, then compact its entire range before reading
// the first entry. That page includes its original payloads. A fresh page gets
// a reset pointing at the canonical checkpoint.
func TestTiKVGraphSubscriptionSnapshotAcrossGC(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	s := f.services[0]
	g := s.graphReplica
	if err := g.enqueue(f.ctx, f.delivery(t, false)); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	m, err := s.Checkpoint(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if mirror, err := g.repo.Manifest(f.ctx); err != nil || mirror != nil {
		t.Fatalf("routed index manifest mirror=%v, err=%v", mirror, err)
	}
	r := *f.r
	coordinator := *f.c.coordinator
	coordinator.root = &r
	reader := &ClusterService{PartitionedService: f.c.PartitionedService, coordinator: &coordinator, graphReplication: true}
	begins := 0
	r.beginTxn = func() (transactionAPI, error) {
		tx, err := f.r.begin()
		if err != nil {
			return nil, err
		}
		begins++
		if begins == 1 {
			pos, err := g.journal.Prune(f.ctx, g.repo, LogBatchSize)
			if err != nil || pos.Sequence != 2 {
				_ = tx.Rollback()
				t.Fatalf("prune=%+v err=%v", pos, err)
			}
		}
		return tx, nil
	}
	q := ChangeRequest{Partition: g.journal.id, Stream: "graph"}
	page, err := reader.ReadChanges(f.ctx, q)
	if err != nil || begins != 1 || page.ResetRequired || page.Head != 2 || len(page.Graph) != 2 || len(page.Graph[1].PayloadData) == 0 {
		t.Fatalf("begins %d, entries %d, reset %t, err %v", begins, len(page.Graph), page.ResetRequired, err)
	}
	page, err = reader.ReadChanges(f.ctx, q)
	if err != nil || begins != 2 || !page.ResetRequired || page.Floor != 2 || page.Checkpoint == nil || page.Checkpoint.Generation != m.Generation {
		t.Fatal("new snapshot missed reset", begins, page, err)
	}
}

func TestDataMaintenancePreflightSharesSnapshotAcrossSources(t *testing.T) {
	f, c, owner := routedFixture(t)
	c.data[1] = &Repository{Prefix: f.r.Prefix + "second/"}
	c.data[2] = &Repository{Prefix: f.r.Prefix + "third/"}
	j := f.services[0].graphReplica.journal
	pos := GraphPosition{Sequence: 1}
	seed(f.db, j.key("head"), GraphHead{Committed: pos, Visible: pos, Floor: pos, CheckpointPosition: pos,
		FloorSources: map[string]uint64{"0": 2, "1": 2, "2": 2}})
	seed(f.db, completedKey(c.data[0]), uint64(2))
	seed(f.db, []byte(c.data[0].Prefix+"log-floor"), uint64(2))
	seed(f.db, completedKey(c.data[1]), uint64(2))
	seed(f.db, completedKey(c.data[2]), "malformed")
	begin := f.r.beginTxn
	begins := 0
	f.r.beginTxn = func() (transactionAPI, error) {
		begins++
		return begin()
	}
	selected, err := c.dataMaintenanceCandidates(f.ctx, map[DataPartitionID]Ownership{0: owner, 1: owner, 2: owner})
	want := map[DataPartitionID]Ownership{1: owner, 2: owner}
	if err != nil || begins != 1 || !reflect.DeepEqual(selected, want) {
		t.Fatal("covered source, eligible source and malformed source", selected, begins, err)
	}
}

func TestDataMaintenanceSelectsOwnedHealthyWriters(t *testing.T) {
	c := &ClusterService{writers: make(map[DataPartitionID]*mutationGroup)}
	for _, id := range []DataPartitionID{1, 3} {
		lease := &appendLease{owner: Ownership{Partition: sourceID(id), Epoch: 7}}
		confirmAppendGrant(t, lease, lease.owner.Epoch, 1<<60, time.Minute)
		if id == 3 {
			lease.StepDown()
			lease.uncertain.Store(true)
		}
		c.writers[id] = &mutationGroup{lease: lease}
	}
	owners := c.localDataOwners()
	if len(owners) != 1 || owners[1].Epoch != 7 {
		t.Fatal(owners)
	}
	delete(c.writers, 1)
	if len(c.localDataOwners()) != 0 || len(owners) != 1 {
		t.Fatal("stale maintenance candidate")
	}
}
func TestGraphCheckpointMirrorByLayout(t *testing.T) {
	for _, routed := range []bool{false, true} {
		t.Run(fmt.Sprint("routed_", routed), func(t *testing.T) {
			f, _, owner := routedFixture(t)
			enableReferences(f)
			s := f.services[0]
			g := s.graphReplica
			// An old routed index can still contain this key after an upgrade.
			g.repo.IndexPrefix = "index/index-0000/"
			old := Manifest{Generation: "old-mirror"}
			seed(f.db, []byte(g.repo.Namespace()+"manifest"), old)
			f.write(t, "v", false)
			if routed {
				if err := g.enqueue(f.ctx, deliveryFor(t, f, owner, 0)); err != nil {
					t.Fatal(err)
				}
			} else {
				g.input = nil
			}
			m, err := s.Checkpoint(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			saved, err := g.repo.Manifest(f.ctx)
			if err != nil || saved == nil {
				t.Fatal(saved, err)
			}
			if routed && saved.Generation != old.Generation || !routed && saved.Generation != m.Generation {
				t.Fatal(saved, m)
			}
			restored, err := newGraphService(f.ctx, g.repo, f.objects, g.journal)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restored.Index.graph.Close)
			if restored.Cursor() != 1 {
				t.Fatal("wrong checkpoint restored", restored.Cursor())
			}
		})
	}
}
