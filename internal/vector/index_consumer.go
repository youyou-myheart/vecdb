package vector

import (
	"context"
	"errors"
	"log"
	"time"
)

// EventDrivenIndexing reports whether each local replica owns its consumer.
// Callers must not run a second periodic SyncOwned loop in this mode.
func (c *ClusterService) EventDrivenIndexing() bool {
	return c.PartitionedService != nil && c.layout.Routing != nil
}

const followerPollInterval = 100 * time.Millisecond
const initialIndexRetry = 10 * time.Millisecond
const maxIndexRetry = time.Second

type consumerStep func(context.Context) (leader, more bool, err error)

// Queue notifications only schedule work; every batch still reads the current
// plan and executes the existing source/replica fences. Timers are used for
// remote follower tailing and failed IO, never for a healthy empty Leader queue.
func runIndexConsumer(ctx context.Context, in *routedInput, partition string, step consumerStep, reportError func(error)) {
	p := &in.cluster.observations.pipeline
	retry := initialIndexRetry
	reason := "start"
	for ctx.Err() == nil {
		// Discard a notification already covered by the next queue check. Any
		// enqueue racing with the subsequent empty check leaves a new token.
		select {
		case <-in.wake:
		default:
		}
		p.init()
		p.consumerWake.WithLabelValues(partition, reason).Inc()
		batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		measure := p.track("index_consumer_work", partition)
		leader, more, err := step(batchCtx)
		measure(&err)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			reportError(err)
			if !waitIndexRetry(ctx, retry) {
				return
			}
			retry = min(2*retry, maxIndexRetry)
			reason = "retry"
			continue
		}
		retry = initialIndexRetry
		if leader && more {
			reason = "continue"
			continue
		}
		var timer *time.Timer
		var poll <-chan time.Time
		if !leader {
			timer = time.NewTimer(followerPollInterval)
			poll = timer.C
		}
		in.waiting.Store(true)
		idle := p.track("index_consumer_idle", partition)
		select {
		case <-ctx.Done():
		case <-in.wake:
			reason = "notification"
		case <-poll:
			reason = "follower_poll"
		}
		idle(nil)
		in.waiting.Store(false)
		if timer != nil {
			timer.Stop()
		}
	}
}

func waitIndexRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *ClusterService) runRoutedIndex(ctx context.Context, entry *localPartition) {
	entry.mu.RLock()
	var in *routedInput
	var id string
	if s := entry.service; s != nil && s.graphReplica != nil {
		in = s.graphReplica.input
		id = string(s.graphReplica.journal.id)
	}
	entry.mu.RUnlock()
	if in == nil {
		return
	}
	runIndexConsumer(ctx, in, id, func(ctx context.Context) (bool, bool, error) {
		if err := c.coordinator.Check(ctx, entry.owner); err != nil {
			return false, false, err
		}
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		s := entry.service
		if s == nil {
			return false, false, ErrFenced
		}
		// Release both locks between batches, allowing search/checkpoint and
		// retirement to proceed even under a continuous input stream.
		wait := s.graphReplica.track("graph_sync_lock_wait")
		s.syncMu.Lock()
		wait(nil)
		defer s.syncMu.Unlock()
		g := s.graphReplica
		err := g.sync(ctx, s, false)
		return g.leader, in.hasQueued(), err
	}, func(err error) {
		log.Printf("index consumer %s failed: %v", id, err)
		if errors.Is(err, ErrGraphCorrupt) {
			_, _ = c.coordinator.Update(ctx, entry.owner, false, true)
			c.remove(entry)
		}
	})
}
