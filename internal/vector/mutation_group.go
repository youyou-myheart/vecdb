package vector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

const mutationGroupRecords = 32
const mutationGroupBytes = 2 << 20

type mutationResult struct {
	record Record
	err    error
}
type mutationRequest struct {
	preparedMutation
	ctx      context.Context
	admitted time.Time
	result   chan mutationResult
}

// One short-lived drainer per local DataPartition. The first request starts
// immediately; subsequent commits combine only already queued work. No timer
// delays an ACK to fill a batch. Different partitions/gateways remain concurrent.
type mutationGroup struct {
	ctx       context.Context
	repo      *Repository
	metrics   *pipelineMetrics
	partition string
	onCommit  func()
	mu        sync.Mutex
	queue     []*mutationRequest
	bytes     int
	running   bool
	closed    bool
	changed   chan struct{}
	wg        sync.WaitGroup
}

func newMutationGroup(ctx context.Context, r *Repository, p *pipelineMetrics, partition string) *mutationGroup {
	return &mutationGroup{ctx: ctx, repo: r, metrics: p, partition: partition, changed: make(chan struct{})}
}

func (b *mutationGroup) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *mutationGroup) mutate(ctx context.Context, id string, m Mutation, deleted bool) (Record, error) {
	q, err := b.repo.prepareMutation(id, m, deleted)
	if err != nil {
		return Record{}, err
	}
	// Admission bounds both count and encoded bytes. One oversized request can
	// run alone, preserving the existing single-request validation contract.
	for {
		b.mu.Lock()
		if b.closed || b.ctx.Err() != nil {
			b.mu.Unlock()
			return Record{}, context.Canceled
		}
		if err = ctx.Err(); err != nil {
			b.mu.Unlock()
			return Record{}, err
		}
		if len(b.queue) < mutationGroupRecords && (len(b.queue) == 0 || b.bytes+q.size <= mutationGroupBytes) {
			// A canceled caller may return while its shared commit is resolving.
			// The drainer must own all request data, including the CAS pointer.
			q.mutation.Vector = append([]float64(nil), m.Vector...)
			q.mutation.Payload = cloneRecord(Record{Payload: m.Payload}).Payload
			if m.ExpectedRevision != nil {
				n := *m.ExpectedRevision
				q.mutation.ExpectedRevision = &n
			}
			r := &mutationRequest{preparedMutation: q, ctx: ctx, admitted: time.Now(), result: make(chan mutationResult, 1)}
			b.queue = append(b.queue, r)
			b.bytes += q.size
			if !b.running {
				b.running = true
				b.wg.Add(1)
				go b.drain()
			}
			b.mu.Unlock()
			select {
			case result := <-r.result:
				return result.record, result.err
			case <-ctx.Done():
				return Record{}, ctx.Err()
			case <-b.ctx.Done():
				return Record{}, b.ctx.Err()
			}
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return Record{}, ctx.Err()
		case <-b.ctx.Done():
			return Record{}, b.ctx.Err()
		}
	}
}

func (b *mutationGroup) close() {
	b.mu.Lock()
	b.closed = true
	b.signalLocked()
	b.mu.Unlock()
	b.wg.Wait()
}

func (b *mutationGroup) drain() {
	defer b.wg.Done()
	for {
		b.mu.Lock()
		batch := b.queue
		b.queue, b.bytes = nil, 0
		b.signalLocked()
		if len(batch) == 0 {
			b.running = false
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()
		active := batch[:0]
		for _, q := range batch {
			if err := q.ctx.Err(); err != nil {
				q.result <- mutationResult{err: err}
			} else {
				b.metrics.observe("data_group_queue", b.partition, time.Since(q.admitted), nil)
				active = append(active, q)
			}
		}
		if len(active) == 0 {
			continue
		}
		// One request's cancellation must not abort another request's commit.
		// Cluster shutdown cancels the group; unknown outcomes still require
		// retrying the same request ID, just like an ordinary mutation.
		ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
		measure := b.metrics.track("data_group_commit", b.partition)
		results, err := b.repo.mutateGroup(ctx, active)
		measure(&err)
		cancel()
		b.metrics.observeBatch("data_commit", b.partition, len(active), err)
		if b.onCommit != nil {
			// Also reconcile an uncertain commit, even if all callers canceled.
			b.onCommit()
		}
		for n, q := range active {
			q.result <- results[n]
		}
	}
}

func (r *Repository) mutateGroup(ctx context.Context, batch []*mutationRequest) ([]mutationResult, error) {
	for attempt := 0; ; attempt++ {
		results, err := r.mutateGroupOnce(ctx, batch)
		if !tikverr.IsErrWriteConflict(errors.Unwrap(err)) || attempt >= 15 {
			return results, err
		}
		if !waitIndexRetry(ctx, time.Duration(min(attempt+1, 10))*time.Millisecond) {
			results = make([]mutationResult, len(batch))
			for n := range results {
				results[n].err = ctx.Err()
			}
			return results, ctx.Err()
		}
	}
}

func (r *Repository) mutateGroupOnce(ctx context.Context, batch []*mutationRequest) (results []mutationResult, err error) {
	results = make([]mutationResult, len(batch))
	defer func() {
		if err != nil {
			for n := range results {
				results[n] = mutationResult{err: err}
			}
		}
	}()
	tx, err := r.begin()
	if err != nil {
		return results, err
	}
	defer tx.Rollback()
	keys := [][]byte{[]byte(r.Prefix + "log-head")}
	for _, q := range batch {
		keys = append(keys, r.key("record", q.id), r.key("request", q.mutation.RequestID))
	}
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return results, err
	}
	cached := prefetchedTransaction{transactionAPI: tx, values: values}
	changed := false
	for n, q := range batch {
		rec, wrote, e := r.mutateIn(ctx, cached, q.preparedMutation)
		if e != nil && !errors.Is(e, ErrConflict) {
			return results, e
		}
		results[n] = mutationResult{rec, e}
		changed = changed || wrote
	}
	if changed {
		if err = tx.Commit(ctx); err != nil {
			return results, fmt.Errorf("commit not confirmed; retry same request_id: %w", err)
		}
	}
	return results, nil
}
