package vector

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrFollowerReadUnsupported = errors.New("follower read requires leased graph replication and at least two replicas")
	ErrFollowerQueueFull       = errors.New("follower read queue is full")
)

const followerReadTimeout = 5 * time.Second
const followerCaptureTimeout = time.Second
const followerQueueLimit = 1024
const followerBatchLimit = 128

// A batch is sealed BEFORE issuing its leader RPC. Arrivals during that RPC
// require a fresh capture, even if the returned position has not changed.
type followerBatch struct {
	requests []*followerWaiter
	cancel   context.CancelFunc
	started  time.Time
	position GraphPosition
	err      error
}
type followerWaiter struct {
	element          *list.Element
	ctx              context.Context
	queued, captured time.Time
	batch            *followerBatch
	target           *GraphPosition
	done             chan followerPermit
}
type followerPermit struct {
	target               GraphPosition
	queue, capture, wait time.Duration
	err                  error
	stage                string
}

// This queue owns only bounded waiting metadata. Queries run in their request
// goroutines after permission, never in the capture/apply notification loop.
// Cancellation removes a waiter immediately, including from an in-flight batch.
type followerReadQueue struct {
	mu                                              sync.Mutex
	uncaptured                                      list.List
	captureWG                                       sync.WaitGroup
	pending                                         map[*followerWaiter]struct{}
	position                                        GraphPosition
	batch                                           *followerBatch
	closed                                          bool
	wake                                            chan struct{}
	results                                         chan *followerBatch
	ctx                                             context.Context
	cancel                                          context.CancelFunc
	done                                            chan struct{}
	capture                                         func(context.Context) (GraphPosition, error)
	observe                                         func(string, time.Duration, error)
	observeBatch                                    func(int, error)
	completed, timedOut, canceled, rejected, failed uint64
}

func newFollowerReadQueue(pos GraphPosition, capture func(context.Context) (GraphPosition, error)) *followerReadQueue {
	ctx, cancel := context.WithCancel(context.Background())
	return &followerReadQueue{pending: make(map[*followerWaiter]struct{}), position: pos,
		wake: make(chan struct{}, 1), results: make(chan *followerBatch, 1), done: make(chan struct{}), ctx: ctx, cancel: cancel, capture: capture}
}
func (q *followerReadQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
func (q *followerReadQueue) applied(pos GraphPosition) {
	q.mu.Lock()
	q.position = pos
	q.mu.Unlock()
	q.signal()
}
func (q *followerReadQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.cancel()
	q.mu.Unlock()
	<-q.done
}
func (q *followerReadQueue) wait(ctx context.Context) (permit followerPermit, err error) {
	if err := ctx.Err(); err != nil {
		return followerPermit{}, err
	}
	r := &followerWaiter{ctx: ctx, queued: time.Now(), done: make(chan followerPermit, 1)}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return followerPermit{}, ErrFenced
	}
	if len(q.pending) >= followerQueueLimit {
		q.rejected++
		q.mu.Unlock()
		return followerPermit{}, ErrFollowerQueueFull
	}
	q.pending[r] = struct{}{}
	r.element = q.uncaptured.PushBack(r)
	q.mu.Unlock()
	q.signal()
	defer func() {
		if q.observe == nil {
			return
		}
		q.observe("follower_queue", permit.queue, errForStage(permit.stage, "follower_queue", err))
		if permit.stage != "follower_queue" {
			q.observe("follower_leader_capture", permit.capture, errForStage(permit.stage, "follower_leader_capture", err))
		}
		if permit.stage == "follower_wait_apply" {
			q.observe("follower_wait_apply", permit.wait, err)
		}
	}()
	select {
	case result := <-r.done:
		// A deadline racing a ready notification must not start an expired query.
		if err := ctx.Err(); err != nil {
			return result, err
		}
		return result, result.err
	case <-ctx.Done():
		q.mu.Lock()
		q.finishLocked(r, ctx.Err())
		if b := q.batch; b != nil {
			live := false
			for _, member := range b.requests {
				if _, ok := q.pending[member]; ok {
					live = true
					break
				}
			}
			if !live {
				b.cancel()
			}
		}
		q.mu.Unlock()
		q.signal()
		return <-r.done, ctx.Err()
	}
}
func (q *followerReadQueue) finishLocked(r *followerWaiter, err error) {
	if _, ok := q.pending[r]; !ok {
		return
	}
	delete(q.pending, r)
	if r.element != nil {
		q.uncaptured.Remove(r.element)
		r.element = nil
	}
	result := followerPermit{err: err}
	now := time.Now()
	stage := "follower_queue"
	result.queue = now.Sub(r.queued)
	if r.batch != nil {
		result.queue = r.batch.started.Sub(r.queued)
		result.capture = now.Sub(r.batch.started)
		stage = "follower_leader_capture"
	}
	if r.target != nil {
		result.target = *r.target
		result.capture = r.captured.Sub(r.batch.started)
		result.wait = now.Sub(r.captured)
		stage = "follower_wait_apply"
	}
	result.stage = stage
	switch {
	case err == nil:
		q.completed++
	case errors.Is(err, context.DeadlineExceeded):
		q.timedOut++
	case errors.Is(err, context.Canceled):
		q.canceled++
	default:
		q.failed++
	}
	r.done <- result
}
func errForStage(stage, want string, err error) error {
	if stage == want {
		return err
	}
	return nil
}

func (q *followerReadQueue) run() {
	defer close(q.done)
	defer q.captureWG.Wait()
	defer func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.closed = true
		for r := range q.pending {
			q.finishLocked(r, ErrFenced)
		}
	}()
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-q.wake:
		case b := <-q.results:
			if q.observeBatch != nil {
				q.observeBatch(len(b.requests), b.err)
			}
			q.mu.Lock()
			q.batch = nil
			for _, r := range b.requests {
				if _, ok := q.pending[r]; !ok {
					continue
				}
				if q.closed {
					q.finishLocked(r, ErrFenced)
				} else if b.err != nil {
					q.finishLocked(r, b.err)
				} else {
					pos := b.position
					r.target, r.captured = &pos, time.Now()
				}
			}
			q.mu.Unlock()
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return
		}
		for r := range q.pending {
			if err := r.ctx.Err(); err != nil {
				q.finishLocked(r, err)
				continue
			}
			if r.target != nil && q.position.Sequence >= r.target.Sequence {
				var err error
				if q.position.Sequence == r.target.Sequence && q.position != *r.target {
					err = ErrGraphCorrupt
				}
				q.finishLocked(r, err)
			}
		}
		if q.batch == nil && !q.closed {
			b := &followerBatch{}
			for len(b.requests) < followerBatchLimit && q.uncaptured.Len() > 0 {
				element := q.uncaptured.Front()
				r := element.Value.(*followerWaiter)
				q.uncaptured.Remove(element)
				r.element = nil
				b.requests = append(b.requests, r)
			}
			if len(b.requests) > 0 {
				ctx, cancel := context.WithTimeout(q.ctx, followerCaptureTimeout)
				b.cancel, b.started = cancel, time.Now()
				for _, r := range b.requests {
					r.batch = b
				}
				q.batch = b
				q.captureWG.Add(1)
				go func() {
					defer q.captureWG.Done()
					b.position, b.err = q.capture(ctx)
					cancel()
					select {
					case q.results <- b:
					case <-q.ctx.Done():
					}
				}()
			}
		}
		q.mu.Unlock()
	}
}
