package vector

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	tikverr "github.com/tikv/client-go/v2/error"
)

// Exercise the actual reverse-scan snapshot and TiKV conflict semantics. The
// fake DB intentionally cannot verify concurrent transactions or MVCC reads.
func TestTiKVDurableGraphTailAndCompetingAppends(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "durable-tail/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Client.Close()
	c := &Coordinator{root: r, ttl: 60000, referenceLogs: true}
	node := ClusterNode{ID: "worker", Session: "s", URL: "http://localhost:1", Roles: []string{RoleWorker}}
	if err = c.Heartbeat(ctx, node); err != nil {
		t.Fatal(err)
	}
	id := IndexPartitionID("index-0000")
	replica := replicaID(id, node)
	testAssignment(t, c, node, replica)
	owner, err := c.Acquire(ctx, replica, node)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = c.Update(ctx, owner, true, false)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = putJSON(tx, c.planKey(id), ReplicaPlan{Publisher: replica, Serving: []Assignment{{Partition: replica, Node: node.ID, Session: node.Session}}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	j := graphJournal{c, id, owner}
	h, err := j.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback()
	if _, _, err = old.Last(ctx, j.key("entries/"), j.key("entries0")); !tikverr.IsErrNotFound(err) {
		t.Fatal(err)
	}
	// Both appenders were built against the same base. Only one may win and
	// the loser may never replace the winner's immutable entry at sequence 1.
	start := make(chan struct{})
	errs := make([]error, 2)
	entries := make([]GraphEntry, 2)
	var wg sync.WaitGroup
	for n := range errs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			entries[n], errs[n] = j.commitPayload(ctx, h, 1, "delta", Manifest{}, nil, nil, bytes.Repeat([]byte{byte(n + 1)}, graphPayloadChunk+17))
		}(n)
	}
	close(start)
	wg.Wait()
	winner := -1
	for n, err := range errs {
		if err == nil {
			if winner != -1 {
				t.Fatal("two appenders succeeded")
			}
			winner = n
		} else if !errors.Is(err, ErrConflict) && !tikverr.IsErrWriteConflict(err) {
			t.Fatal(err)
		}
	}
	if winner == -1 {
		t.Fatal(errs)
	}
	tail, err := j.Head(ctx)
	if err != nil || tail.Committed != entries[winner].Position || tail.Visible != tail.Committed {
		t.Fatal(tail, err)
	}
	persisted, err := j.Entry(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Payload == nil || persisted.Payload.Chunks != 0 || len(persisted.PayloadData) <= graphPayloadChunk {
		t.Fatal("expected a single inline entry larger than the legacy chunk size")
	}
	verify, err := r.begin()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = verify.Last(ctx, j.key("payload/"), j.key("payload0"))
	_ = verify.Rollback()
	if !tikverr.IsErrNotFound(err) {
		t.Fatal("unexpected external payload key", err)
	}
	payload, err := j.loadPayload(ctx, persisted)
	if err != nil || !bytes.Equal(payload, bytes.Repeat([]byte{byte(winner + 1)}, graphPayloadChunk+17)) {
		t.Fatal("atomic inline payload differs from winning entry", err)
	}
	snapshot, err := j.headIn(ctx, old)
	if err != nil || snapshot.Committed.Sequence != 0 {
		t.Fatal("reverse scan escaped transaction snapshot", snapshot, err)
	}
	if _, _, err = old.Last(ctx, j.key("missing/"), j.key("missing0")); !tikverr.IsErrNotFound(err) {
		t.Fatal("last read escaped prefix", err)
	}
	// Exercise the single-key storage limit at the largest supported delta.
	large := maximumGraphTestPayload(t)
	if _, err := j.commitPayload(ctx, tail, 2, "delta", Manifest{}, nil, nil, large); err != nil {
		t.Fatal("commit maximum inline delta", err)
	}
	stored, err := j.Entry(ctx, 2)
	if err != nil || stored.Payload == nil || stored.Payload.Chunks != 0 || !bytes.Equal(stored.PayloadData, large) {
		t.Fatal("read maximum inline delta", err)
	}
	tail, err = j.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Revoke ownership after the append read its snapshot but before commit.
	base := *r
	other := *c
	other.root = &base
	r.beginTxn = func() (transactionAPI, error) {
		tx, err := base.begin()
		return dispatchHookTx{transactionAPI: tx, beforeCommit: func() error { _, e := other.Update(ctx, owner, false, true); return e }}, err
	}
	err = retryIndexCommit(ctx, func() error {
		_, e := j.commitPayload(ctx, tail, 2, "delta", Manifest{}, nil, nil, []byte("stale"))
		return e
	}, func() {})
	r.beginTxn = nil
	if !errors.Is(err, ErrFenced) {
		t.Fatal("stale leader appended", err)
	}
	after, err := j.Head(ctx)
	if err != nil || after.Committed != tail.Committed {
		t.Fatal("stale append changed tail", after, err)
	}
}

func maximumGraphTestPayload(t *testing.T) []byte {
	t.Helper()
	large := make([]byte, maxGraphPayload)
	// Incompressible data exercises the physical TiKV entry limit too.
	if _, err := rand.New(rand.NewSource(42)).Read(large); err != nil {
		t.Fatal(err)
	}
	return large
}

func TestNativeGraphSlotMaximumInlinePayload(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	f.c.coordinator.holdReleasedLeases = true
	enableNativeLease(t, f)
	g := f.services[0].graphReplica
	h, token, err := g.writer.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	large := maximumGraphTestPayload(t)
	entry, err := g.journal.commitWithAuthority(f.ctx, h, 1, "delta", Manifest{}, nil, nil, large, token, g.writer)
	if err != nil {
		t.Fatal("one-phase commit of maximum inline delta", err)
	}
	stored, err := g.journal.Entry(f.ctx, entry.Position.Sequence)
	if err != nil || stored.Position != entry.Position || stored.Payload == nil || stored.Payload.Chunks != 0 || !bytes.Equal(stored.PayloadData, large) {
		t.Fatal("read maximum single-slot delta", err)
	}
}
