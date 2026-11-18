package vector

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func waitFollowerState(t *testing.T, q *followerReadQueue, predicate func(*followerReadQueue) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		ok := predicate(q)
		q.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("follower queue state did not converge")
}
func asyncFollowerWait(q *followerReadQueue, ctx context.Context) <-chan followerPermit {
	done := make(chan followerPermit, 1)
	go func() { p, err := q.wait(ctx); p.err = err; done <- p }()
	return done
}
func followerResult(t *testing.T, done <-chan followerPermit) followerPermit {
	t.Helper()
	select {
	case p := <-done:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("waiter stuck")
		return followerPermit{}
	}
}

func TestFollowerQueueBatchesAndSealsBeforeCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	requests := make(chan chan GraphPosition, 4)
	q := newFollowerReadQueue(GraphPosition{}, func(ctx context.Context) (GraphPosition, error) {
		response := make(chan GraphPosition, 1)
		requests <- response
		select {
		case p := <-response:
			return p, nil
		case <-ctx.Done():
			return GraphPosition{}, ctx.Err()
		}
	})
	var first, second []<-chan followerPermit
	for i := 0; i < 32; i++ {
		first = append(first, asyncFollowerWait(q, ctx))
	}
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 32 })
	go q.run()
	defer q.close()
	one := <-requests
	for i := 0; i < 16; i++ {
		second = append(second, asyncFollowerWait(q, ctx))
	}
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 48 })
	one <- GraphPosition{Sequence: 10, Hash: "ten"}
	// The first batch is still waiting for apply. Capturing the next batch must
	// nevertheless proceed, and arrivals must not reuse the first position.
	two := <-requests
	two <- GraphPosition{Sequence: 20, Hash: "twenty"}
	q.applied(GraphPosition{Sequence: 10, Hash: "ten"})
	for _, done := range first {
		p := followerResult(t, done)
		if p.err != nil || p.target.Sequence != 10 {
			t.Fatal(p)
		}
	}
	for _, done := range second {
		select {
		case p := <-done:
			t.Fatal("later batch released early", p)
		default:
		}
	}
	q.applied(GraphPosition{Sequence: 20, Hash: "twenty"})
	for _, done := range second {
		p := followerResult(t, done)
		if p.err != nil || p.target.Sequence != 20 {
			t.Fatal(p)
		}
	}
	select {
	case <-requests:
		t.Fatal("extra leader capture")
	default:
	}
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 0 && q.completed == 48 })
}

func TestFollowerQueueMixedDeadlinesAndRepeatedTimeouts(t *testing.T) {
	q := newFollowerReadQueue(GraphPosition{}, func(context.Context) (GraphPosition, error) { return GraphPosition{Sequence: 9}, nil })
	go q.run()
	defer q.close()
	long, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	survivor := asyncFollowerWait(q, long)
	for i := 0; i < 12; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		p := followerResult(t, asyncFollowerWait(q, ctx))
		cancel()
		if !errors.Is(p.err, context.DeadlineExceeded) {
			t.Fatal(p)
		}
		waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 1 })
	}
	q.applied(GraphPosition{Sequence: 9})
	if p := followerResult(t, survivor); p.err != nil {
		t.Fatal(p)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.timedOut != 12 || len(q.pending) != 0 {
		t.Fatal(q.timedOut, len(q.pending))
	}
}

func TestFollowerQueueCancelsUnusedCaptureAndRetirement(t *testing.T) {
	entered := make(chan struct{}, 4)
	var calls atomic.Int64
	q := newFollowerReadQueue(GraphPosition{}, func(ctx context.Context) (GraphPosition, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
		return GraphPosition{}, ctx.Err()
	})
	go q.run()
	ctx, cancel := context.WithCancel(context.Background())
	one := asyncFollowerWait(q, ctx)
	<-entered
	cancel()
	if p := followerResult(t, one); !errors.Is(p.err, context.Canceled) {
		t.Fatal(p)
	}
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return q.batch == nil && len(q.pending) == 0 })
	two := asyncFollowerWait(q, context.Background())
	<-entered
	q.close()
	if p := followerResult(t, two); !errors.Is(p.err, ErrFenced) {
		t.Fatal(p)
	}
	if _, err := q.wait(context.Background()); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
}

func TestFollowerQueueCapacityAndCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := newFollowerReadQueue(GraphPosition{}, func(context.Context) (GraphPosition, error) { return GraphPosition{}, nil })
	all := make([]<-chan followerPermit, followerQueueLimit)
	for i := range all {
		all[i] = asyncFollowerWait(q, ctx)
	}
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == followerQueueLimit })
	if _, err := q.wait(ctx); !errors.Is(err, ErrFollowerQueueFull) {
		t.Fatal(err)
	}
	cancel()
	for _, done := range all {
		if p := followerResult(t, done); !errors.Is(p.err, context.Canceled) {
			t.Fatal(p)
		}
	}
	go q.run()
	defer q.close()
	waitFollowerState(t, q, func(q *followerReadQueue) bool {
		return len(q.pending) == 0 && q.uncaptured.Len() == 0 && q.rejected == 1
	})
}

func TestFollowerQueueCaptureTimeoutAndHashMismatch(t *testing.T) {
	t.Run("leader_unreachable", func(t *testing.T) {
		q := newFollowerReadQueue(GraphPosition{}, func(ctx context.Context) (GraphPosition, error) { <-ctx.Done(); return GraphPosition{}, ctx.Err() })
		go q.run()
		defer q.close()
		p := followerResult(t, asyncFollowerWait(q, context.Background()))
		if !errors.Is(p.err, context.DeadlineExceeded) {
			t.Fatal(p)
		}
		waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 0 })
	})
	t.Run("mismatched_history", func(t *testing.T) {
		q := newFollowerReadQueue(GraphPosition{Sequence: 10, Hash: "a"}, func(context.Context) (GraphPosition, error) { return GraphPosition{Sequence: 10, Hash: "b"}, nil })
		go q.run()
		defer q.close()
		if p := followerResult(t, asyncFollowerWait(q, context.Background())); !errors.Is(p.err, ErrGraphCorrupt) {
			t.Fatal(p)
		}
	})
}

func TestFollowerQueueCancellationPreservesSharedCapture(t *testing.T) {
	entered := make(chan context.Context, 1)
	respond := make(chan struct{})
	q := newFollowerReadQueue(GraphPosition{}, func(ctx context.Context) (GraphPosition, error) {
		entered <- ctx
		select {
		case <-respond:
			return GraphPosition{}, nil
		case <-ctx.Done():
			return GraphPosition{}, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	one := asyncFollowerWait(q, ctx)
	two := asyncFollowerWait(q, context.Background())
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 2 })
	go q.run()
	defer q.close()
	captureCtx := <-entered
	cancel()
	if p := followerResult(t, one); !errors.Is(p.err, context.Canceled) {
		t.Fatal(p)
	}
	if err := captureCtx.Err(); err != nil {
		t.Fatal("one caller canceled shared capture", err)
	}
	close(respond)
	if p := followerResult(t, two); p.err != nil {
		t.Fatal(p)
	}
}

func TestFollowerQueueSplitsLargeBurstIntoBoundedBatches(t *testing.T) {
	q := newFollowerReadQueue(GraphPosition{}, func(context.Context) (GraphPosition, error) { return GraphPosition{}, nil })
	sizes := make(chan int, 10)
	q.observeBatch = func(n int, err error) { sizes <- n }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests []<-chan followerPermit
	for i := 0; i < 260; i++ {
		requests = append(requests, asyncFollowerWait(q, ctx))
	}
	waitFollowerState(t, q, func(q *followerReadQueue) bool { return len(q.pending) == 260 })
	go q.run()
	defer q.close()
	for _, done := range requests {
		if p := followerResult(t, done); p.err != nil {
			t.Fatal(p)
		}
	}
	for _, want := range []int{128, 128, 4} {
		if n := <-sizes; n != want {
			t.Fatal(n, want)
		}
	}
}
