package vector

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

type recordingSourceLocks struct {
	transactionAPI
	enabled bool
	keys    []string
	err     error
}

func (t *recordingSourceLocks) SetPessimistic(v bool) { t.enabled = v }
func (t *recordingSourceLocks) LockKeys(_ context.Context, lc *kv.LockCtx, keys ...[]byte) error {
	if !t.enabled || lc.ForUpdateTS != t.StartTS() || lc.LockWaitTime() != kv.LockNoWait {
		return errors.New("unsafe lock order/snapshot")
	}
	for _, key := range keys {
		t.keys = append(t.keys, string(key))
	}
	return t.err
}
func TestSourceCommitLocksOrderedAtOriginalSnapshot(t *testing.T) {
	r, _ := newFakeRepo()
	tx, _ := r.begin()
	defer tx.Rollback()
	c := &Coordinator{root: r}
	wrapped := &recordingSourceLocks{transactionAPI: tx}
	inputs := []Delivery{{Owner: Ownership{Partition: sourceID(2)}}, {Owner: Ownership{Partition: sourceID(0)}}, {Owner: Ownership{Partition: sourceID(2)}}}
	if err := lockSourceFences(context.Background(), wrapped, c, inputs); err != nil {
		t.Fatal(err)
	}
	if len(wrapped.keys) != 2 || wrapped.keys[0] != string(c.fenceKey(sourceID(0))) || wrapped.keys[1] != string(c.fenceKey(sourceID(2))) {
		t.Fatal(wrapped.keys)
	}
	wrapped.err = injected
	if err := lockSourceFences(context.Background(), wrapped, c, inputs); !errors.Is(err, injected) {
		t.Fatal(err)
	}
}

func TestSourceCommitLocksTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "source-lock/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Client.Close()
	c := &Coordinator{root: r, ttl: 60000}
	node := ClusterNode{ID: "worker", Session: "s", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	if err = c.Heartbeat(ctx, node); err != nil {
		t.Fatal(err)
	}
	testAssignment(t, c, node, sourceID(0))
	owner, err := c.Acquire(ctx, sourceID(0), node)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = c.Update(ctx, owner, true, false)
	if err != nil {
		t.Fatal(err)
	}
	inputs := []Delivery{{Owner: owner}}

	holder, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if err = lockSourceFences(ctx, holder, c, inputs); err != nil {
		t.Fatal(err)
	}
	if err = c.Fence(owner)(ctx, holder); err != nil {
		t.Fatal(err)
	}
	contender, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = lockSourceFences(ctx, contender, c, inputs); !errors.Is(err, errSourceLockBusy) {
		t.Fatal("contended lock did not fail before prewrite", err)
	}
	_ = contender.Rollback()
	// A competing source revocation must not finish while the graph transaction
	// holds the fence. Retry only definite write conflicts after that lock clears.
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		finished <- retryIndexCommit(ctx, func() error { _, e := c.Update(ctx, owner, false, true); return e }, func() {})
	}()
	<-started
	select {
	case e := <-finished:
		t.Fatal("revocation passed held fence", e)
	case <-time.After(30 * time.Millisecond):
	}
	if err = holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Never read the old owner through an old snapshot after acquiring a lock
	// at a newer timestamp. The old transaction must conflict, a fresh one fences.
	fresh, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Rollback()
	if err = lockSourceFences(ctx, fresh, c, inputs); err != nil {
		t.Fatal(err)
	}
	if err = c.Fence(owner)(ctx, fresh); !errors.Is(err, ErrFenced) {
		t.Fatal("revoked owner accepted", err)
	}
	_ = fresh.Rollback()
	// A key updated after the snapshot cannot be silently locked and read old.
	old, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback()
	newer, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Rollback()
	if err = newer.Set(c.fenceKey(owner.Partition), []byte("changed")); err != nil {
		t.Fatal(err)
	}
	if err = newer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = lockSourceFences(ctx, old, c, inputs); !tikverr.IsErrWriteConflict(err) {
		t.Fatal("stale snapshot not rejected", err)
	}
}
