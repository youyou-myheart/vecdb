package vector

import (
	"context"
	"sync"
	"time"
)

const rawWritesInFlight = 16

type rawPendingWrite struct {
	requests []*mutationRequest
	results  []mutationResult
	batch    rawAcceptedBatch
	done     chan struct{}
	err      error
}

func (w *rawPendingWrite) ready() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// One drainer resolves requests in admission order, then issues each batch
// immediately. A slot stays occupied until all earlier writes are confirmed:
// a completed suffix may contain revisions derived from an unconfirmed prefix.
func (b *mutationGroup) drainRaw(ctx context.Context, batch []*mutationRequest) {
	defer b.wg.Done()
	var writes sync.WaitGroup
	defer writes.Wait()
	pending := make([]*rawPendingWrite, 0, rawWritesInFlight)
	var epoch uint64
	if b.lease != nil {
		epoch = b.lease.owner.Epoch
	}
	for {
		if err := b.requireRawLease(ctx, epoch); err != nil {
			b.failRawWrites(err, batch, pending)
			return
		}
		if len(pending) > 0 && pending[0].ready() {
			if err := b.confirmRawWrite(ctx, epoch, pending[0]); err != nil {
				b.failRawWrites(err, batch, pending)
				return
			}
			copy(pending, pending[1:])
			pending[len(pending)-1] = nil
			pending = pending[:len(pending)-1]
			continue
		}
		var queueChanged <-chan struct{}
		if len(pending) < rawWritesInFlight {
			if len(batch) == 0 {
				batch, queueChanged = b.takeRawBatch(len(pending) == 0)
			}
			if len(batch) > 0 {
				write, err := b.prepareRawWrite(batch)
				if err != nil {
					b.failRawWrites(err, write.requests, pending)
					return
				}
				batch = nil
				if write != nil {
					pending = append(pending, write)
					if write.batch.Number == 0 {
						close(write.done)
					} else {
						writes.Add(1)
						go func(work *rawPendingWrite) {
							defer writes.Done()
							b.writeRawBatch(ctx, work)
						}(write)
					}
				}
				continue
			}
			if len(pending) == 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
		case <-queueChanged:
		case <-pending[0].done:
		}
	}
}

func (b *mutationGroup) takeRawBatch(idle bool) ([]*mutationRequest, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.queue) == 0 {
		if idle {
			b.running = false
		}
		return nil, b.changed
	}
	batch := b.queue
	b.queue, b.bytes = nil, 0
	b.signalLocked()
	return batch, nil
}

func (b *mutationGroup) requireRawLease(ctx context.Context, epoch uint64) error {
	var current uint64
	if b.lease == nil || !b.lease.InLeaderLease(&current) || current != epoch {
		return ErrFenced
	}
	return ctx.Err()
}

func (b *mutationGroup) prepareRawWrite(batch []*mutationRequest) (*rawPendingWrite, error) {
	active := batch[:0]
	for _, request := range batch {
		if err := request.ctx.Err(); err != nil {
			request.result <- mutationResult{err: err}
			continue
		}
		b.metrics.observe("data_group_queue", b.partition, time.Since(request.admitted), nil)
		active = append(active, request)
	}
	if len(active) == 0 {
		return nil, nil
	}
	results, accepted, err := b.rawWriter.prepare(active)
	return &rawPendingWrite{requests: active, results: results, batch: accepted, done: make(chan struct{})}, err
}

func (b *mutationGroup) writeRawBatch(ctx context.Context, work *rawPendingWrite) {
	defer close(work.done)
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	measure := b.metrics.track("data_group_commit", b.partition)
	work.err = b.rawWriter.write(callCtx, work.batch)
	measure(&work.err)
	b.metrics.observeBatch("data_commit", b.partition, len(work.requests), work.err)
}

func (b *mutationGroup) confirmRawWrite(ctx context.Context, epoch uint64, work *rawPendingWrite) error {
	if work.err != nil {
		return work.err
	}
	if err := b.requireRawLease(ctx, epoch); err != nil {
		return err
	}
	if work.batch.Number != 0 {
		if err := b.rawWriter.accept(work.batch); err != nil {
			return err
		}
		b.lease.head.Store(work.batch.last())
		if b.onCommit != nil {
			b.onCommit()
		}
	}
	for n, request := range work.requests {
		result := work.results[n]
		result.record = cloneRecord(result.record)
		request.result <- result
	}
	return nil
}

func (b *mutationGroup) failRawWrites(err error, batch []*mutationRequest, pending []*rawPendingWrite) {
	b.mu.Lock()
	if b.lease != nil {
		b.lease.Retire()
		b.lease.uncertain.Store(true)
	}
	b.rawStop()
	b.closed = true
	queued := b.queue
	b.queue, b.bytes, b.running = nil, 0, false
	b.signalLocked()
	b.mu.Unlock()
	for _, work := range pending {
		fenceRawRequests(work.requests, err)
	}
	fenceRawRequests(batch, err)
	fenceRawRequests(queued, ErrFenced)
	if b.onCommit != nil {
		// Promptly reconcile a possibly durable prefix, including a lost reply.
		b.onCommit()
	}
}

func fenceRawRequests(batch []*mutationRequest, err error) {
	for _, request := range batch {
		request.result <- mutationResult{err: err}
	}
}
