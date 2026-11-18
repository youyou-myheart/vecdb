package vector

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func consumerTest(t *testing.T, step consumerStep) (*routedInput, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	in := newRoutedInput(&ClusterService{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runIndexConsumer(ctx, in, "index-0000", step, func(error) {}) }()
	t.Cleanup(func() { cancel(); receiveConsumer(t, done) })
	return in, cancel, done
}

func receiveConsumer[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not make progress")
		var zero T
		return zero
	}
}

func waitConsumerIdle(t *testing.T, in *routedInput) {
	t.Helper()
	end := time.After(2 * time.Second)
	for !in.waiting.Load() {
		select {
		case <-end:
			t.Fatal("consumer did not block")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestIndexConsumerDrainsAndWaitsForNotification(t *testing.T) {
	var remaining atomic.Int64
	remaining.Store(3)
	calls := make(chan int64, 16)
	in, _, _ := consumerTest(t, func(context.Context) (bool, bool, error) {
		n := remaining.Add(-1)
		calls <- n
		return true, n > 0, nil
	})
	for _, want := range []int64{2, 1, 0} {
		if got := receiveConsumer(t, calls); got != want {
			t.Fatal(got, want)
		}
	}
	waitConsumerIdle(t, in)
	select {
	case got := <-calls:
		t.Fatal("idle Leader polled", got)
	case <-time.After(40 * time.Millisecond):
	}
	remaining.Store(2)
	in.notify()
	if receiveConsumer(t, calls) != 1 || receiveConsumer(t, calls) != 0 {
		t.Fatal("did not drain new input")
	}
	waitConsumerIdle(t, in)
}

func TestIndexConsumerWakeBetweenEmptyCheckAndWait(t *testing.T) {
	checked, release := make(chan struct{}), make(chan struct{})
	calls := make(chan struct{}, 4)
	var count atomic.Int64
	in, _, _ := consumerTest(t, func(ctx context.Context) (bool, bool, error) {
		if count.Add(1) == 1 {
			close(checked)
			select {
			case <-release:
			case <-ctx.Done():
				return true, false, ctx.Err()
			}
			return true, false, nil // Queue was observed empty before the racing admission.
		}
		calls <- struct{}{}
		return true, false, nil
	})
	receiveConsumer(t, checked)
	for n := 0; n < 1000; n++ {
		in.notify()
	}
	if len(in.wake) != 1 {
		t.Fatal("notifications must coalesce")
	}
	close(release)
	receiveConsumer(t, calls)
	waitConsumerIdle(t, in)
	select {
	case <-calls:
		t.Fatal("coalesced notification became duplicate work")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestIndexConsumerRetriesWithoutNewDataAndCancels(t *testing.T) {
	for _, leader := range []bool{true, false} {
		t.Run(fmt.Sprintf("leader_%v", leader), func(t *testing.T) {
			calls := make(chan time.Time, 8)
			count := 0
			in, cancel, done := consumerTest(t, func(context.Context) (bool, bool, error) {
				count++
				calls <- time.Now()
				if count == 1 {
					return leader, true, injected // Errors must back off even after progress.
				}
				return leader, false, nil
			})
			failed := receiveConsumer(t, calls)
			retried := receiveConsumer(t, calls)
			if retried.Sub(failed) < initialIndexRetry {
				t.Fatal("failed IO did not back off", retried.Sub(failed))
			}
			waitConsumerIdle(t, in)
			cancel()
			receiveConsumer(t, done)
			if in.waiting.Load() {
				t.Fatal("cancelled consumer still reported waiting")
			}
		})
	}
}

func TestFollowerPollsAndWakesOnPromotion(t *testing.T) {
	calls := make(chan struct{}, 8)
	var leader atomic.Bool
	in, _, _ := consumerTest(t, func(context.Context) (bool, bool, error) {
		calls <- struct{}{}
		return leader.Load(), false, nil
	})
	receiveConsumer(t, calls)
	waitConsumerIdle(t, in)
	select {
	case <-calls:
		t.Fatal("idle Follower polled before its timer")
	case <-time.After(20 * time.Millisecond):
	}
	receiveConsumer(t, calls) // Follower tails remote graph journal without admission.
	waitConsumerIdle(t, in)
	leader.Store(true)
	in.planChanged(2, "new-publisher")
	receiveConsumer(t, calls)
	waitConsumerIdle(t, in)
	in.planChanged(2, "new-publisher")
	select {
	case <-calls:
		t.Fatal("unchanged plan woke idle Leader")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestConsumerContinuedInputCanBeCancelled(t *testing.T) {
	for _, leader := range []bool{true, false} {
		t.Run(fmt.Sprintf("leader_%v", leader), func(t *testing.T) {
			calls := make(chan struct{}, 1)
			_, cancel, done := consumerTest(t, func(context.Context) (bool, bool, error) {
				select {
				case calls <- struct{}{}:
				default:
				}
				return leader, true, nil
			})
			receiveConsumer(t, calls)
			cancel()
			receiveConsumer(t, done)
		})
	}
}

func TestFollowerDrainsUntilEmptyThenPolls(t *testing.T) {
	calls := make(chan time.Time, 5)
	count := 0 // Only the consumer goroutine accesses count.
	in, cancel, done := consumerTest(t, func(ctx context.Context) (bool, bool, error) {
		count++
		calls <- time.Now()
		if count == 5 {
			// Hold the first idle poll so observations do not depend on how
			// quickly the test goroutine is scheduled after that poll.
			<-ctx.Done()
			return false, false, ctx.Err()
		}
		return false, count <= 3, nil
	})
	for n := 0; n < 3; n++ {
		receiveConsumer(t, calls)
	}
	empty := receiveConsumer(t, calls)
	polled := receiveConsumer(t, calls)
	cancel()
	receiveConsumer(t, done)
	if polled.Sub(empty) < followerPollInterval {
		t.Fatal("Follower did not wait after an empty read", polled.Sub(empty))
	}
	for reason, want := range map[string]float64{"start": 1, "follower_continue": 3, "follower_poll": 1} {
		var metric dto.Metric
		if err := in.cluster.observations.pipeline.consumerWake.WithLabelValues("index-0000", reason).Write(&metric); err != nil {
			t.Fatal(err)
		}
		if got := metric.GetCounter().GetValue(); got != want {
			t.Fatalf("%s: got %v want %v", reason, got, want)
		}
	}
}

func TestFollowerStepContinuesOnReplayAndIgnoresStaleQueue(t *testing.T) {
	f, c, source := routedFixture(t)
	enableReferences(f)
	a, b := f.services[0], f.services[1]
	entry := &localPartition{owner: b.graphReplica.journal.owner, service: b}
	// A demoted Leader may still have queued deliveries. They cannot keep a
	// Follower busy or be used to generate graph mutations.
	b.graphReplica.input.queue[0] = &queuedDelivery{}
	for n := uint64(0); n < 2; n++ {
		f.write(t, "v", n == 1)
		if err := a.graphReplica.enqueue(f.ctx, deliveryFor(t, f, source, n)); err != nil {
			t.Fatal(err)
		}
		if err := a.Sync(f.ctx); err != nil {
			t.Fatal(err)
		}
		leader, more, err := c.stepRoutedIndex(f.ctx, entry)
		if err != nil || leader || !more || b.graphReplica.position != a.graphReplica.position {
			t.Fatal("Follower did not request another tail after replay", leader, more, err)
		}
		equalGraph(t, a.Index, b.Index)
		leader, more, err = c.stepRoutedIndex(f.ctx, entry)
		if err != nil || leader || more || !b.graphReplica.input.hasQueued() {
			t.Fatal("empty Follower tail spun on stale input", leader, more, err)
		}
	}
}

func TestConsumerSlowIndexDoesNotBlockAnotherIndex(t *testing.T) {
	blocked := make(chan struct{})
	_, _, _ = consumerTest(t, func(ctx context.Context) (bool, bool, error) {
		close(blocked)
		<-ctx.Done()
		return true, false, ctx.Err()
	})
	receiveConsumer(t, blocked)
	fast := make(chan struct{}, 1)
	in, _, _ := consumerTest(t, func(context.Context) (bool, bool, error) { fast <- struct{}{}; return true, false, nil })
	receiveConsumer(t, fast)
	waitConsumerIdle(t, in)
}

func TestRoutedConsumerRetirementCancelsIdleWait(t *testing.T) {
	f, c, _ := routedFixture(t)
	s := f.services[0]
	ctx, cancel := context.WithCancel(f.ctx)
	entry := &localPartition{owner: s.graphReplica.journal.owner, service: s, cancel: cancel, loadDone: make(chan struct{})}
	c.locals = map[IndexPartitionID]*localPartition{entry.owner.Partition: entry}
	go func() { defer close(entry.loadDone); c.runRoutedIndex(ctx, entry) }()
	t.Cleanup(func() { cancel(); receiveConsumer(t, entry.loadDone) })
	waitConsumerIdle(t, s.graphReplica.input)
	c.remove(entry)
	finished := make(chan struct{})
	go func() { c.retirementWG.Wait(); close(finished) }()
	receiveConsumer(t, finished)
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if !entry.retired || entry.service != nil {
		t.Fatal("consumer/native graph not retired")
	}
}

func TestUncertainGraphCommitWakesPublicationWithoutNewInput(t *testing.T) {
	f, _, owner := routedFixture(t)
	f.write(t, "v", false)
	s := f.services[0]
	g := s.graphReplica
	head, err := g.journal.Claim(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.enqueue(f.ctx, deliveryFor(t, f, owner, 0)); err != nil {
		t.Fatal(err)
	}
	head, err = g.journal.Head(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	<-g.input.wake
	f.db.commitErr = injected
	f.db.committedDespiteError = true
	if err = g.commitRouted(f.ctx, s, head); err == nil {
		t.Fatal("uncertain commit not injected")
	}
	f.db.commitErr = nil
	f.db.committedDespiteError = false
	if len(g.input.wake) != 1 || g.input.hasQueued() {
		t.Fatal("durable replay failed to wake empty queue")
	}
	if err = s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	head, err = g.journal.Head(f.ctx)
	if err != nil || head.Committed != head.Visible || g.position != head.Visible || head.Sources["0"] != 1 {
		t.Fatal(head, err)
	}
}
