package vector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func preparedRequest(t *testing.T, r *Repository, id string, m Mutation, deleted bool) *mutationRequest {
	t.Helper()
	q, err := r.prepareMutation(id, m, deleted)
	if err != nil {
		t.Fatal(err)
	}
	return &mutationRequest{preparedMutation: q, ctx: context.Background()}
}

func TestMutationGroupOrderedRevisionsReceiptsAndConflictIsolation(t *testing.T) {
	r, db := newFakeRepo()
	zero, one := uint64(0), uint64(1)
	create := Mutation{RequestID: "create", Vector: []float64{1, 2}, ExpectedRevision: &zero}
	batch := []*mutationRequest{
		preparedRequest(t, r, "a", create, false),
		preparedRequest(t, r, "a", create, false), // Same receipt, no second LSN.
		preparedRequest(t, r, "a", Mutation{RequestID: "bad-cas", Vector: []float64{1, 2}, ExpectedRevision: &zero}, false),
		preparedRequest(t, r, "b", create, false), // Cross-ID reuse rejected within the transaction.
		preparedRequest(t, r, "a", Mutation{RequestID: "update", Vector: []float64{3, 4}, ExpectedRevision: &one}, false),
		preparedRequest(t, r, "a", Mutation{RequestID: "delete"}, true),
		preparedRequest(t, r, "a", Mutation{RequestID: "restore", Vector: []float64{5, 6}}, false),
		preparedRequest(t, r, "b", Mutation{RequestID: "other", Vector: []float64{7, 8}}, false),
	}
	results, err := r.mutateGroup(context.Background(), batch)
	if err != nil || db.commits != 1 {
		t.Fatal(err, db.commits)
	}
	for n, result := range results {
		if n == 2 || n == 3 {
			if !errors.Is(result.err, ErrConflict) {
				t.Fatal(n, result)
			}
		} else if result.err != nil {
			t.Fatal(n, result.err)
		}
	}
	for n, want := range map[int]uint64{0: 1, 1: 1, 4: 2, 5: 3, 6: 4, 7: 1} {
		if results[n].record.Revision != want {
			t.Fatal(n, results[n])
		}
	}
	log, err := r.ReadEvents(context.Background(), 0, 0, 256)
	if err != nil || log.Head != 5 || len(log.Events) != 5 {
		t.Fatal(log, err)
	}
	for n, event := range log.Events {
		if event.Sequence != uint64(n+1) {
			t.Fatal(log)
		}
	}
	// An uncertain caller retry must reproduce original ACKs after later writes.
	again, err := r.mutateGroup(context.Background(), []*mutationRequest{batch[0], batch[4], batch[5]})
	if err != nil || db.commits != 1 || again[0].record.Revision != 1 || again[1].record.Revision != 2 || !again[2].record.Deleted {
		t.Fatal(again, err, db.commits)
	}
}

func TestMutationGroupAtomicStorageFailuresAndUnknownCommit(t *testing.T) {
	for _, scenario := range []string{"begin", "read", "set", "commit", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			r, db := newFakeRepo()
			batch := []*mutationRequest{}
			for n := 0; n < 3; n++ {
				batch = append(batch, preparedRequest(t, r, fmt.Sprint(n), Mutation{RequestID: fmt.Sprint(n), Vector: []float64{1, 2}}, false))
			}
			switch scenario {
			case "begin":
				db.beginErr = injected
			case "read":
				db.getFail = "record/"
			case "set":
				db.setFail = 6 // One complete record already staged.
			case "commit", "unknown":
				db.commitErr = injected
				db.committedDespiteError = scenario == "unknown"
			}
			results, err := r.mutateGroup(context.Background(), batch)
			if !errors.Is(err, injected) {
				t.Fatal(err)
			}
			for _, result := range results {
				if !errors.Is(result.err, injected) || result.record.Revision != 0 {
					t.Fatal(result)
				}
			}
			if scenario != "unknown" && len(db.data) != 0 {
				t.Fatal("partial batch persisted", db.data)
			}
			db.beginErr, db.getFail, db.setFail, db.commitErr, db.committedDespiteError = nil, "", 0, nil, false
			results, err = r.mutateGroup(context.Background(), batch)
			if err != nil || db.commits != 1 {
				t.Fatal(err, db.commits)
			}
			for _, result := range results {
				if result.err != nil || result.record.Revision != 1 {
					t.Fatal(result)
				}
			}
			log, err := r.ReadEvents(context.Background(), 0, 0, 256)
			if err != nil || log.Head != 3 {
				t.Fatal(log, err)
			}
		})
	}
}

type gatedMutationTx struct {
	transactionAPI
	once             *sync.Once
	entered, release chan struct{}
}

func (tx gatedMutationTx) Commit(ctx context.Context) error {
	tx.once.Do(func() {
		close(tx.entered)
		select {
		case <-ctx.Done():
		case <-tx.release:
		}
	})
	return tx.transactionAPI.Commit(ctx)
}

func TestMutationGroupDrainBatchCancelAndShutdown(t *testing.T) {
	r, db := newFakeRepo()
	begin := r.beginTxn
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.beginTxn = func() (transactionAPI, error) {
		tx, e := begin()
		return gatedMutationTx{tx, &once, entered, release}, e
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := newMutationGroup(ctx, r, &pipelineMetrics{}, "data-0000")
	t.Cleanup(func() { cancel(); group.close() })
	call := func(ctx context.Context, id string) <-chan mutationResult {
		out := make(chan mutationResult, 1)
		go func() {
			rec, err := group.mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}}, false)
			out <- mutationResult{rec, err}
		}()
		return out
	}
	first := call(ctx, "first")
	receiveConsumer(t, entered)
	select {
	case <-first:
		t.Fatal("ACK before durable commit")
	default:
	}
	cancelCtx, cancelRequest := context.WithCancel(ctx)
	canceled := call(cancelCtx, "canceled")
	var pending []<-chan mutationResult
	for n := 0; n < 20; n++ {
		pending = append(pending, call(ctx, fmt.Sprint(n)))
	}
	deadline := time.Now().Add(time.Second)
	for {
		group.mu.Lock()
		count := len(group.queue)
		group.mu.Unlock()
		if count == 21 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("requests did not queue", count)
		}
		time.Sleep(time.Millisecond)
	}
	cancelRequest()
	if result := receiveConsumer(t, canceled); !errors.Is(result.err, context.Canceled) {
		t.Fatal(result)
	}
	close(release)
	if result := receiveConsumer(t, first); result.err != nil {
		t.Fatal(result)
	}
	for _, out := range pending {
		if result := receiveConsumer(t, out); result.err != nil {
			t.Fatal(result)
		}
	}
	group.close()
	if db.commits != 2 {
		t.Fatal("queued writes not grouped", db.commits)
	}
	log, err := r.ReadEvents(ctx, 0, 0, 256)
	if err != nil || log.Head != 21 {
		t.Fatal(log, err)
	}
	if _, err = group.mutate(ctx, "closed", Mutation{RequestID: "closed", Vector: []float64{1, 2}}, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestMutationGroupPrefetchUsesOneBatchRead(t *testing.T) {
	r, db := newFakeRepo()
	begin := r.beginTxn
	var calls int
	r.beginTxn = func() (transactionAPI, error) {
		tx, e := begin()
		return countedBatchGetTx{transactionAPI: tx, calls: &calls}, e
	}
	batch := []*mutationRequest{}
	for n := 0; n < 10; n++ {
		batch = append(batch, preparedRequest(t, r, fmt.Sprint(n), Mutation{RequestID: fmt.Sprint(n), Vector: []float64{1, 2}}, false))
	}
	if _, err := r.mutateGroup(context.Background(), batch); err != nil || calls != 1 || db.commits != 1 {
		t.Fatal(err, calls, db.commits)
	}
}

func TestSourceCommitsBatchesHeadsAtOneSnapshot(t *testing.T) {
	r, db := newFakeRepo()
	layout, _ := newLayout(8)
	c := &ClusterService{root: r, coordinator: &Coordinator{root: r}, PartitionedService: &PartitionedService{layout: layout}}
	for n, binding := range layout.Bindings {
		j := graphJournal{c: c.coordinator, id: binding.Index}
		seed(db, j.key("head"), GraphHead{Sources: map[string]uint64{"0": uint64(n + 1)}})
	}
	begin := r.beginTxn
	calls, begins := 0, 0
	r.beginTxn = func() (transactionAPI, error) { begins++; tx, e := begin(); return countedBatchGetTx{tx, &calls}, e }
	targets := (IdentityPartitionRouter{layout}).QueryIndexes(Search{})
	commits, err := c.sourceCommits(context.Background(), 0, targets)
	if err != nil || calls != 1 || begins != 1 || len(commits) != 8 {
		t.Fatal(err, commits, calls, begins)
	}
	for n, binding := range layout.Bindings {
		if commits[binding.Index] != uint64(n+1) {
			t.Fatal(commits)
		}
	}
}

func TestMutationGroupBackpressureCountBytesAndCancellation(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprint(large), func(t *testing.T) {
			r, _ := newFakeRepo()
			begin := r.beginTxn
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			r.beginTxn = func() (transactionAPI, error) {
				tx, e := begin()
				return gatedMutationTx{tx, &once, entered, release}, e
			}
			ctx, cancel := context.WithCancel(context.Background())
			group := newMutationGroup(ctx, r, &pipelineMetrics{}, "data-0000")
			t.Cleanup(func() { cancel(); group.close() })
			call := func(ctx context.Context, id, payload string) <-chan mutationResult {
				out := make(chan mutationResult, 1)
				go func() {
					rec, e := group.mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}, Payload: map[string]string{"value": payload}}, false)
					out <- mutationResult{rec, e}
				}()
				return out
			}
			first := call(ctx, "first", "")
			receiveConsumer(t, entered)
			count, payload := mutationGroupRecords, ""
			if large {
				count, payload = 2, strings.Repeat("x", mutationGroupBytes/3)
			}
			var pending []<-chan mutationResult
			for n := 0; n < count; n++ {
				pending = append(pending, call(ctx, fmt.Sprint(n), payload))
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				group.mu.Lock()
				queued, bytes := len(group.queue), group.bytes
				group.mu.Unlock()
				if queued == count {
					if bytes > mutationGroupBytes {
						t.Fatal("byte budget exceeded", bytes)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("admissions stalled", queued)
				}
				time.Sleep(time.Millisecond)
			}
			blockedCtx, stop := context.WithCancel(ctx)
			blocked := call(blockedCtx, "blocked", payload)
			// The full queue must not reject immediately or commit extra work.
			select {
			case result := <-blocked:
				t.Fatal("backpressure did not wait", result)
			case <-time.After(10 * time.Millisecond):
			}
			stop()
			if result := receiveConsumer(t, blocked); !errors.Is(result.err, context.Canceled) {
				t.Fatal(result)
			}
			close(release)
			if result := receiveConsumer(t, first); result.err != nil {
				t.Fatal(result)
			}
			for _, out := range pending {
				if result := receiveConsumer(t, out); result.err != nil {
					t.Fatal(result)
				}
			}
			// Re-admission around the drainer's empty/exit boundary must wake it.
			if result := receiveConsumer(t, call(ctx, "after-drain", "")); result.err != nil {
				t.Fatal(result)
			}
			group.close()
			if _, err := r.Get(ctx, "blocked"); !errors.Is(err, ErrNotFound) {
				t.Fatal("unadmitted request persisted", err)
			}
		})
	}
}

type countedBatchGetTx struct {
	transactionAPI
	calls *int
}

func (tx countedBatchGetTx) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	*tx.calls++
	return batchGet(ctx, tx.transactionAPI, keys)
}
