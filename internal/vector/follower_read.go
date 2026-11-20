package vector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Timings are local monotonic durations, not differences between host clocks.
// WaitApplyMS excludes leader capture, query execution and transport overhead.
type FollowerReadInfo struct {
	IndexPartition  IndexPartitionID `json:"index_partition"`
	Replica         IndexPartitionID `json:"replica"`
	Node            string           `json:"node"`
	Session         string           `json:"session"`
	OwnerEpoch      uint64           `json:"owner_epoch"`
	Target          GraphPosition    `json:"target"`
	Applied         GraphPosition    `json:"applied"`
	QueueMS         float64          `json:"queue_ms"`
	LeaderCaptureMS float64          `json:"leader_capture_ms"`
	WaitApplyMS     float64          `json:"wait_apply_ms"`
	QueryMS         float64          `json:"query_ms"`
}
type FollowerSearchResult struct {
	Hits          []Hit              `json:"hits"`
	Partial       bool               `json:"partial"`
	FollowerReads []FollowerReadInfo `json:"follower_reads"`
}

func ValidateFollowerSearch(q Search, dimension int) error {
	if err := validateQuery(q, dimension); err != nil {
		return err
	}
	if q.EFSearch < 0 || q.EFSearch > 4096 {
		return errors.New("ef_search must be 0..4096")
	}
	if q.MinVisible != nil {
		return errors.New("follower read cannot be combined with min_visible")
	}
	return nil
}

// FollowerSearch enters each follower first. Only followers acquire leader
// applied positions; the SDK/Gateway never prefetches or caches a read barrier.
func (c *ClusterService) FollowerSearch(ctx context.Context, q Search) (FollowerSearchResult, error) {
	if !c.coordinator.leaseReads || !c.graphReplication || c.replicas < 2 {
		return FollowerSearchResult{}, ErrFollowerReadUnsupported
	}
	if err := ValidateFollowerSearch(q, c.dimension); err != nil {
		return FollowerSearchResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, followerReadTimeout)
	defer cancel()
	ids, routeErr := c.PartitionedService.queryIndexes(q)
	if routeErr != nil {
		return FollowerSearchResult{}, routeErr
	}
	replies := make([]partitionReply, len(ids))
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id IndexPartitionID) {
			defer wg.Done()
			p := &remotePartition{cluster: c, id: id}
			replies[i], errs[i] = p.followerCall(ctx, q)
			if errs[i] != nil {
				cancel()
			}
		}(i, id)
	}
	wg.Wait()
	// Prefer the originating error over cancellation of sibling partitions.
	var first error
	for i, err := range errs {
		if err != nil && (first == nil || errors.Is(first, context.Canceled)) {
			first = fmt.Errorf("%s: %w", ids[i], err)
		}
	}
	if first != nil {
		return FollowerSearchResult{}, first
	}
	result := FollowerSearchResult{Hits: []Hit{}}
	for _, r := range replies {
		if r.FollowerRead == nil {
			return FollowerSearchResult{}, ErrPartitionUnavailable
		}
		result.Hits = append(result.Hits, r.Hits...)
		result.FollowerReads = append(result.FollowerReads, *r.FollowerRead)
	}
	result.Hits = deduplicateHits(result.Hits)
	sort.Slice(result.Hits, func(i, j int) bool {
		if result.Hits[i].Distance == result.Hits[j].Distance {
			return result.Hits[i].ID < result.Hits[j].ID
		}
		return result.Hits[i].Distance < result.Hits[j].Distance
	})
	if len(result.Hits) > q.K {
		result.Hits = result.Hits[:q.K]
	}
	return result, nil
}

func (p *remotePartition) followerCall(ctx context.Context, q Search) (partitionReply, error) {
	if p.cluster.routeHintsEnabled() {
		return p.cachedRoutedCall(ctx, "follower_search", q, nil)
	}
	last := error(ErrPartitionUnavailable)
	for round := 0; round < 2; round++ {
		plan, err := p.routingPlan(ctx, "follower_search")
		if err != nil {
			return partitionReply{}, err
		}
		for _, a := range plan.Serving {
			if a.Partition == plan.Publisher {
				continue
			}
			o, err := p.cluster.coordinator.Resolve(ctx, a.Partition)
			if err == nil {
				var reply partitionReply
				reply, err = p.callOwner(ctx, "follower_search", q, o)
				if err == nil {
					return reply, nil
				}
			}
			last = err
			if ctx.Err() != nil {
				return partitionReply{}, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrFollowerQueueFull) {
				return partitionReply{}, err
			}
		}
	}
	return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, last)
}

func (g *graphReplica) followerAuthority() error {
	if g.writer == nil {
		return ErrFollowerReadUnsupported
	}
	w := g.writer
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.replicaLive() || w.publisher == "" || w.publisher == g.journal.owner.Partition || w.leaderLease.InLeaderLease(nil) {
		return ErrFenced
	}
	return nil
}

func (g *graphReplica) leaderApplied() (GraphPosition, error) {
	if g.writer == nil {
		return GraphPosition{}, ErrFollowerReadUnsupported
	}
	// Read the actual published view, not writer.head.Committed. No KV IO.
	g.viewMu.RLock()
	defer g.viewMu.RUnlock()
	w := g.writer
	w.mu.Lock()
	defer w.mu.Unlock()
	grant, ok := w.leaderLease.Snapshot()
	if !w.known || !ok || grant.Epoch != w.lease.Generation || !w.replicaLive() {
		return GraphPosition{}, ErrFenced
	}
	return g.position, nil
}

func (g *graphReplica) captureLeaderApplied(ctx context.Context) (GraphPosition, error) {
	if err := g.followerAuthority(); err != nil {
		return GraphPosition{}, err
	}
	g.writer.mu.Lock()
	route := g.writer.publisherRoute
	g.writer.mu.Unlock()
	if route.Partition == "" || route.URL == "" || route.Session == "" {
		return GraphPosition{}, ErrPartitionUnavailable
	}
	p := remotePartition{cluster: g.input.cluster, id: g.journal.id}
	r, err := p.callOwner(ctx, "applied_position", Search{}, Ownership{Partition: route.Partition, Node: route.Node, Session: route.Session, URL: route.URL})
	if err != nil {
		return GraphPosition{}, err
	}
	if r.GraphPosition == nil {
		return GraphPosition{}, ErrPartitionUnavailable
	}
	g.viewMu.RLock()
	behind := g.position.Sequence < r.GraphPosition.Sequence
	g.viewMu.RUnlock()
	if behind {
		g.input.notify()
	}
	return *r.GraphPosition, nil
}

func (g *graphReplica) readQueue() *followerReadQueue {
	g.viewMu.Lock()
	defer g.viewMu.Unlock()
	if g.followerReads == nil {
		q := newFollowerReadQueue(g.position, g.captureLeaderApplied)
		p := &g.input.cluster.observations.pipeline
		q.observe = func(stage string, d time.Duration, err error) { p.observe(stage, string(g.journal.id), d, err) }
		q.observeBatch = func(n int, err error) { p.observeBatch("follower_capture", string(g.journal.id), n, err) }
		g.followerReads = q
		go q.run()
	}
	return g.followerReads
}

// Waiting must not pin entry.mu: retirement must be able to close the queue
// and wake every request before freeing the native index.
func (c *ClusterService) executeFollowerRead(ctx context.Context, entry *localPartition, q Search) (_ partitionReply, err error) {
	ctx, cancel := context.WithTimeout(ctx, followerReadTimeout)
	defer cancel()
	entry.mu.RLock()
	s := entry.service
	if s == nil || s.graphReplica == nil || s.graphReplica.input == nil || s.graphReplica.writer == nil {
		entry.mu.RUnlock()
		return partitionReply{}, ErrFollowerReadUnsupported
	}
	g := s.graphReplica
	if err = g.followerAuthority(); err != nil {
		entry.mu.RUnlock()
		return partitionReply{}, err
	}
	if err = ValidateFollowerSearch(q, c.dimension); err != nil {
		entry.mu.RUnlock()
		return partitionReply{}, err
	}
	queue := g.readQueue()
	entry.mu.RUnlock()
	defer g.track("follower_read")(&err)
	permit, err := queue.wait(ctx)
	if err != nil {
		return partitionReply{}, err
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if entry.service != s || entry.retired {
		return partitionReply{}, ErrFenced
	}
	if err = ctx.Err(); err != nil {
		return partitionReply{}, err
	}
	if err = g.followerAuthority(); err != nil {
		return partitionReply{}, err
	}
	started := time.Now()
	hits, pos, err := s.searchGraphLocal(ctx, q)
	elapsed := time.Since(started)
	entry.queryNanos.Add(elapsed.Nanoseconds())
	c.observations.pipeline.observe("follower_query", string(g.journal.id), elapsed, err)
	if err != nil {
		return partitionReply{}, err
	}
	if err = ctx.Err(); err != nil {
		return partitionReply{}, err
	}
	if err = g.followerAuthority(); err != nil {
		return partitionReply{}, err
	}
	if pos.Sequence < permit.target.Sequence || pos.Sequence == permit.target.Sequence && pos != permit.target {
		return partitionReply{}, ErrGraphCorrupt
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	info := &FollowerReadInfo{IndexPartition: g.journal.id, Replica: entry.owner.Partition, Node: entry.owner.Node,
		Session: entry.owner.Session, OwnerEpoch: entry.owner.Epoch, Target: permit.target, Applied: pos,
		QueueMS: ms(permit.queue), LeaderCaptureMS: ms(permit.capture), WaitApplyMS: ms(permit.wait), QueryMS: ms(elapsed)}
	return partitionReply{Hits: hits, GraphPosition: &pos, AppliedOffset: pos.DataOffset, FollowerRead: info}, nil
}
