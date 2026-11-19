package vector

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// The first context check in native construction occurs after snapshot capture,
// while ingestion can still write to the original graph.
type recallBuildContext struct {
	context.Context
	once   sync.Once
	update func()
}

func (c *recallBuildContext) Err() error {
	c.once.Do(c.update)
	return c.Context.Err()
}

func TestRecallRebuildReturnsInstalledEpochAfterConcurrentWrite(t *testing.T) {
	i := mustGraph(t, 2)
	t.Cleanup(func() { i.graph.Close() })
	if err := i.Apply([]Record{{ID: "first", Revision: 1, Vector: []float64{1, 2}}}); err != nil {
		t.Fatal(err)
	}
	s := &Service{Index: i}
	sampleEpoch := i.epoch
	if err := i.Apply([]Record{{ID: "between_probe_and_capture", Revision: 1, Vector: []float64{3, 4}}}); err != nil {
		t.Fatal(err)
	}
	ctx := &recallBuildContext{Context: context.Background(), update: func() {
		if err := i.Apply([]Record{{ID: "second", Revision: 1, Vector: []float64{2, 3}}}); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := s.rebuildForRecall(ctx, sampleEpoch, 1)
	if err != nil || got.Epoch != i.epoch || got.DataOffset != i.epoch || got.Epoch != sampleEpoch+3 || i.Stats().Live != 3 {
		t.Fatalf("reset=%+v index epoch=%d stats=%+v err=%v", got, i.epoch, i.Stats(), err)
	}
	if err := i.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.rebuildForRecall(context.Background(), got.Epoch, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("probe superseded by a manual rebuild returned %v", err)
	}
}

func TestRecallGraphRebuildCoversInterveningWritesAndFencesStaleProbe(t *testing.T) {
	f := newGraphFixture(t)
	s, follower := f.services[0], f.services[1]
	t.Cleanup(func() { s.Index.graph.Close(); follower.Index.graph.Close() })
	f.write(t, "first", false)
	if err := s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	probed := s.Index.epoch
	term, ok := s.recallTerm()
	if !ok {
		t.Fatal("publisher has no term")
	}
	if _, err := s.rebuildForRecall(f.ctx, probed, term+1); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale term returned %v", err)
	}
	f.write(t, "second", false)
	if err := s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	before := s.graphReplica.position
	got, err := s.rebuildForRecall(f.ctx, probed, term)
	if err != nil || got.Epoch != s.Index.epoch || got.DataOffset != before.DataOffset ||
		s.graphReplica.position.Sequence != before.Sequence+1 || s.Index.Stats().Live != 2 {
		t.Fatalf("before=%+v reset=%+v position=%+v err=%v", before, got, s.graphReplica.position, err)
	}
	if err := follower.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, s.Index, follower.Index)
	if _, err := follower.rebuildForRecall(f.ctx, follower.Index.epoch, term); !errors.Is(err, ErrFenced) {
		t.Fatalf("follower reset returned %v", err)
	}
	if err := s.Rebuild(f.ctx); err != nil {
		t.Fatal(err)
	}
	afterManual := s.graphReplica.position
	if _, err := s.rebuildForRecall(f.ctx, got.Epoch, term); !errors.Is(err, ErrConflict) || s.graphReplica.position != afterManual {
		t.Fatalf("superseded probe published another reset: position=%v err=%v", s.graphReplica.position, err)
	}
}

type recallResetUpload struct {
	blobAPI
	onSaved func()
}

func (b recallResetUpload) Put(ctx context.Context, bucket, key string, body []byte) error {
	if err := b.blobAPI.Put(ctx, bucket, key, body); err != nil {
		return err
	}
	b.onSaved()
	return nil
}

func TestRecallResetFencesLeaseChangeDuringObjectSave(t *testing.T) {
	for _, change := range []string{"same grant", "new durable generation", "expired publisher grant", "draining publisher"} {
		t.Run(change, func(t *testing.T) {
			f, _, _ := leasedFixture(t)
			s := f.services[0]
			g := s.graphReplica
			initial := g.position
			term, ok := s.recallTerm()
			if !ok {
				t.Fatal("fixture has no publisher lease")
			}
			uploaded := false
			f.objects.Client = recallResetUpload{f.objects.Client, func() {
				uploaded = true
				switch change {
				case "new durable generation":
					// Force the durable grant to expire while leaving both local
					// handles and the replica's separate liveness lease valid.
					tx, err := f.r.begin()
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					old := g.writer.lease
					old.Expires = txnMillis(tx)
					if err = putJSON(tx, f.c.indexLeaseKey(g.journal.id), old); err != nil {
						t.Fatal(err)
					}
					if err = tx.Commit(f.ctx); err != nil {
						t.Fatal(err)
					}
					next, err := f.c.grantIndexLease(f.ctx, g.journal.id, g.journal.owner)
					if err != nil || next.Generation != term+1 {
						t.Fatalf("grant rollover: %+v, %v", next, err)
					}
					// Production installs a new local generation under writer.op,
					// which the rebuild still holds. The durable guard already moved.
				case "expired publisher grant":
					// Keep the separate replica grant alive and move the storage
					// timestamp beyond the publisher grant without a takeover.
					f.db.nowMillis = g.writer.lease.Expires - 1
					if _, err := f.c.Update(f.ctx, g.journal.owner, true, false); err != nil {
						t.Fatal(err)
					}
					f.db.nowMillis = g.writer.lease.Expires
				case "draining publisher":
					tx, err := f.r.begin()
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					p, err := f.c.planIn(f.ctx, tx, g.journal.id)
					if err != nil {
						t.Fatal(err)
					}
					p.LeaseDraining = true
					if err := putJSON(tx, f.c.planKey(g.journal.id), p); err != nil {
						t.Fatal(err)
					}
					if err := tx.Commit(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				if observed, valid := s.recallTerm(); !valid || observed != term {
					t.Fatalf("local grant changed: %d, %v", observed, valid)
				}
			}}
			result, err := s.rebuildForRecall(f.ctx, s.Index.epoch, term)
			if !uploaded {
				t.Fatal("test did not reach the reset upload")
			}
			head, headErr := g.journal.Head(f.ctx)
			if headErr != nil {
				t.Fatal(headErr)
			}
			if change == "same grant" {
				if err != nil || result.Epoch == 0 || head.Committed != g.position || g.position.Sequence != initial.Sequence+1 {
					t.Fatalf("valid reset: %+v head=%+v local=%+v err=%v", result, head.Committed, g.position, err)
				}
			} else if !errors.Is(err, ErrFenced) || head.Committed != initial || g.position != initial || result != (recallRebuildResult{}) {
				t.Fatalf("obsolete reset: %+v head=%+v local=%+v err=%v", result, head.Committed, g.position, err)
			}
		})
	}
}
