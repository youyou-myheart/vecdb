package vector

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type countingTail struct {
	transactionAPI
	reads *int
}

func (tx countingTail) LastKey(ctx context.Context, from, to []byte) ([]byte, error) {
	(*tx.reads)++
	return tx.transactionAPI.LastKey(ctx, from, to)
}
func TestOwnerAppendRecoveryAndTailReads(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint("unknown_committed_", committed), func(t *testing.T) {
			ctx := context.Background()
			repo, db := newFakeRepo()
			reads := 0
			begin := repo.beginTxn
			repo.beginTxn = func() (transactionAPI, error) { tx, err := begin(); return countingTail{tx, &reads}, err }
			c := &ClusterService{runCtx: ctx, coordinator: &Coordinator{root: repo}, PartitionedService: &PartitionedService{data: map[DataPartitionID]*Repository{0: repo}}}
			take := func(epoch uint64) *appendLease {
				o := Ownership{Partition: sourceID(0), Epoch: epoch, Session: "worker"}
				db.data[string(c.coordinator.writeGuardKey(o.Partition))] = epochToken(epoch)
				g, err := c.recoverWriter(ctx, 0, o)
				if err != nil {
					t.Fatal(err)
				}
				g.lease.expires.Store(1 << 60)
				return g.lease
			}
			l := take(1)
			appendOne := func(l *appendLease, id string) error {
				q := preparedRequest(t, repo, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)
				rs, err := repo.appendGroup(ctx, []*mutationRequest{q}, l)
				if err != nil {
					return err
				}
				return rs[0].err
			}
			for _, id := range []string{"a", "a", "b", "c"} {
				if err := appendOne(l, id); err != nil {
					t.Fatal(err)
				}
			}
			if reads != 1 || l.head.Load() != 3 {
				t.Fatal(reads, l.head.Load())
			}
			db.commitErr = injected
			db.committedDespiteError = committed
			if err := appendOne(l, "d"); err == nil || !l.uncertain.Load() || l.head.Load() != 3 {
				t.Fatal("unknown commit was acknowledged", err)
			}
			db.commitErr = nil
			if err := appendOne(l, "e"); !errors.Is(err, ErrFenced) {
				t.Fatal(err)
			}
			next := take(2)
			if err := appendOne(next, "d"); err != nil {
				t.Fatal(err)
			}
			if err := appendOne(next, "e"); err != nil {
				t.Fatal(err)
			}
			l.uncertain.Store(false) // Simulate a delayed old writer that still believes it is healthy.
			if err := appendOne(l, "stale"); !errors.Is(err, ErrFenced) {
				t.Fatal(err)
			}
			if next.head.Load() != 5 || reads != 2 {
				t.Fatal(next.head.Load(), reads)
			}
			for n := uint64(1); n <= 5; n++ {
				if db.data[string(repo.logKey(n))] == nil {
					t.Fatalf("gap at %d", n)
				}
				delete(db.data, string(repo.logKey(n)))
			}
			db.data[repo.Prefix+"log-floor"] = []byte("5")
			next = take(3)
			if err := appendOne(next, "f"); err != nil || next.head.Load() != 6 || reads != 3 {
				t.Fatal(err, reads, next.head.Load())
			}
		})
	}
}

func TestDataWriterProtocolUpgradeRequiresOfflineCluster(t *testing.T) {
	repo, db := newFakeRepo()
	ctx := context.Background()
	route := &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}
	upgrade := func() error { _, err := repo.ensureRoutedLayout(ctx, 2, route, "hash", "node", true); return err }
	if err := upgrade(); err != nil {
		t.Fatal(err)
	}
	const previous = "v14-cluster-routing-lease-hash-node-retention=true"
	key := repo.Prefix + "cluster-mode"
	db.data[key] = []byte(previous)
	db.nowMillis = 1000
	coord := &Coordinator{root: repo, ttl: 3000}
	if err := coord.Heartbeat(ctx, ClusterNode{ID: "old", Session: "s", Roles: []string{RoleGateway}, URL: "http://old"}); err != nil {
		t.Fatal(err)
	}
	if err := upgrade(); err == nil || string(db.data[key]) != previous {
		t.Fatal("live writer protocol replaced", err)
	}
	db.nowMillis = 5000
	if err := upgrade(); err != nil || string(db.data[key]) == previous {
		t.Fatal(err)
	}
}
