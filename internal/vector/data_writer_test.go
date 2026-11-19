package vector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"example.com/vector-storage/internal/lease"
)

type countingTail struct {
	transactionAPI
	reads *int
}

func (tx countingTail) LastKey(ctx context.Context, from, to []byte) ([]byte, error) {
	(*tx.reads)++
	return tx.transactionAPI.LastKey(ctx, from, to)
}

type boundedAppendTestTx struct{ transactionAPI }

func (tx boundedAppendTestTx) commitBeforeLeaseExpiry(ctx context.Context, _ int64) error {
	return tx.Commit(ctx)
}

func confirmAppendGrant(t *testing.T, l *appendLease, epoch uint64, expires int64, remaining time.Duration) {
	t.Helper()
	if !l.Confirm(time.Now(), lease.Grant{Epoch: epoch, Expires: expires, Remaining: remaining}) {
		t.Fatal("test lease was not confirmed")
	}
}

func TestOwnedWriteGuardEvery100ConfirmedBatches(t *testing.T) {
	ctx := context.Background()
	repo, db := newFakeRepo()
	begin := repo.beginTxn
	repo.beginTxn = func() (transactionAPI, error) {
		tx, err := begin()
		return boundedAppendTestTx{tx}, err
	}
	const key = "cluster/write-epochs/data-0000"
	lease := &appendLease{boundCommitTS: true, guard: []byte(key), token: []byte("1")}
	confirmAppendGrant(t, lease, 1, 1<<60, time.Minute)
	db.getFail = key
	request := func(i int) []*mutationRequest {
		id := fmt.Sprint(i)
		return []*mutationRequest{preparedRequest(t, repo, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)}
	}
	for i := 1; i <= 201; i++ {
		batch := request(i)
		if i == 100 {
			if _, err := repo.appendGroup(ctx, request(1), lease); err != nil || db.data[key] != nil {
				t.Fatal("idempotent replay moved the cadence", err)
			}
			db.commitErr = indexWriteConflict()
			if _, err := repo.mutateGroupOnce(ctx, batch, lease); !errors.Is(err, db.commitErr) || db.data[key] != nil {
				t.Fatal("aborted commit wrote the guard", err)
			}
			db.commitErr = nil
		}
		if _, err := repo.appendGroup(ctx, batch, lease); err != nil {
			t.Fatal(i, err)
		}
		if got := db.data[key]; (got != nil) != (i == 100 || i == 200) {
			t.Fatal("wrong guard write cadence", i, string(got))
		}
		delete(db.data, key)
	}
}

func TestOwnerAppendRecoveryAndTailReads(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint("unknown_committed_", committed), func(t *testing.T) {
			ctx := context.Background()
			repo, db := newFakeRepo()
			db.nowMillis = 1000
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
				confirmAppendGrant(t, g.lease, epoch, db.nowMillis+1000, time.Minute)
				return g.lease
			}
			l := take(1)
			db.getFail = "cluster/write-epochs/"
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
			db.nowMillis = 2000
			next := take(2)
			if err := appendOne(next, "d"); err != nil {
				t.Fatal(err)
			}
			if err := appendOne(next, "e"); err != nil {
				t.Fatal(err)
			}
			// Simulate a suspended writer whose local clock still admits the
			// old grant, but whose next transaction starts after PD expiry.
			stale := &appendLease{owner: l.owner, guard: l.guard, token: l.token}
			confirmAppendGrant(t, stale, l.owner.Epoch, 2000, time.Minute)
			if err := appendOne(stale, "stale"); !errors.Is(err, ErrFenced) {
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

func TestExpiredAppendGrantStopsBeforeTiKV(t *testing.T) {
	ctx := context.Background()
	repo, _ := newFakeRepo()
	group := newMutationGroup(ctx, repo, &pipelineMetrics{}, "data-0000")
	defer group.close()
	group.lease = &appendLease{}
	if group.lease.Confirm(time.Now().Add(-time.Second), lease.Grant{Epoch: 1, Expires: 1 << 60, Remaining: time.Second}) {
		t.Fatal("expired grant confirmed")
	}
	repo.beginTxn = func() (transactionAPI, error) { t.Fatal("expired owner started a transaction"); return nil, injected }
	m := Mutation{RequestID: "a", Vector: []float64{1, 2}}
	if _, err := group.mutate(ctx, "a", m, false); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
	if _, err := repo.appendGroup(ctx, []*mutationRequest{preparedRequest(t, repo, "a", m, false)}, group.lease); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
}

func TestMutateOwnedRejectsAcknowledgmentAfterLeaseLoss(t *testing.T) {
	ctx := context.Background()
	repo, _ := newFakeRepo()
	layout, err := newLayout(1)
	if err != nil {
		t.Fatal(err)
	}
	c := &ClusterService{
		runCtx:             ctx,
		root:               repo,
		coordinator:        &Coordinator{root: repo},
		writers:            make(map[DataPartitionID]*mutationGroup),
		PartitionedService: &PartitionedService{data: map[DataPartitionID]*Repository{0: repo}, router: IdentityPartitionRouter{layout}},
	}
	owner := Ownership{Partition: sourceID(0), Epoch: 1, Session: "s"}
	group, err := c.recoverWriter(ctx, 0, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer group.close()
	confirmAppendGrant(t, group.lease, owner.Epoch, 1<<60, time.Minute)
	c.writers[0] = group
	group.onCommit = group.lease.Retire
	m := Mutation{RequestID: "a", Vector: []float64{1, 2}}
	if _, err := c.MutateOwned(ctx, repo.Prefix, 0, owner.Epoch, owner.Session, "a", m, false); !errors.Is(err, ErrFenced) {
		t.Fatal("acknowledged write after lease loss", err)
	}
	c.retireWriter(0)
	owner.Epoch++
	next, err := c.recoverWriter(ctx, 0, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	confirmAppendGrant(t, next.lease, owner.Epoch, 1<<60, time.Minute)
	c.writers[0] = next
	record, err := c.MutateOwned(ctx, repo.Prefix, 0, owner.Epoch, owner.Session, "a", m, false)
	if err != nil || record.Revision != 1 || next.lease.head.Load() != 1 {
		t.Fatal("retry did not reuse the durable receipt", record, err)
	}
}

func TestOwnedWritesSubmitFirstThenCombineQueuedBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	repo, db := newFakeRepo()
	c := &ClusterService{runCtx: ctx, coordinator: &Coordinator{root: repo}, PartitionedService: &PartitionedService{data: map[DataPartitionID]*Repository{0: repo}}}
	group, err := c.recoverWriter(ctx, 0, Ownership{Partition: sourceID(0), Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); group.close() })
	confirmAppendGrant(t, group.lease, group.lease.owner.Epoch, 1<<60, time.Minute)
	begin := repo.beginTxn
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	repo.beginTxn = func() (transactionAPI, error) {
		tx, err := begin()
		return gatedMutationTx{tx, &once, entered, release}, err
	}
	submit := func(id string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := group.mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)
			done <- err
		}()
		return done
	}
	first := submit("first")
	receiveConsumer(t, entered)
	var pending []<-chan error
	for i := range mutationGroupRecords {
		pending = append(pending, submit(fmt.Sprint(i)))
	}
	for {
		group.mu.Lock()
		queued := len(group.queue)
		group.mu.Unlock()
		if queued == mutationGroupRecords {
			break
		}
		if !waitIndexRetry(ctx, time.Millisecond) {
			t.Fatal("write requests never filled the next batch", queued)
		}
	}
	close(release)
	if err := receiveConsumer(t, first); err != nil {
		t.Fatal(err)
	}
	for _, done := range pending {
		if err := receiveConsumer(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if db.commits != 2 || group.lease.head.Load() != 1+mutationGroupRecords {
		t.Fatal(db.commits, group.lease.head.Load())
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
