package vector

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// RecallPolicy controls background sampling of partitions that use HNSW.
// A zero Target disables recall-triggered maintenance.
type RecallPolicy struct {
	Target      float64
	Consecutive int
	Interval    time.Duration
	Cooldown    time.Duration
}

func DefaultRecallPolicy() RecallPolicy {
	return RecallPolicy{Target: .95, Consecutive: 3, Interval: time.Minute, Cooldown: 10 * time.Minute}
}

func (p RecallPolicy) Validate() error {
	if math.IsNaN(p.Target) || p.Target < 0 || p.Target > 1 {
		return errors.New("recall target must be between 0 and 1")
	}
	if p.Target > 0 && (p.Consecutive < 1 || p.Interval <= 0 || p.Cooldown <= 0) {
		return errors.New("recall checks require a positive interval, cooldown and number of consecutive samples")
	}
	return nil
}

type RecallStatus struct {
	Samples        uint64
	Queries        int
	Recall         float64
	OriginalRecall float64
	NearbyRecall   float64
	LowStreak      int
	Rebuilds       uint64
	Failures       uint64
	LastChecked    time.Time
	LastRebuild    time.Time
	NextCheck      time.Time
	Suppressed     bool
}

// PartitionedService can inspect its shards concurrently. Keep full native
// rebuilds from claiming CPU and memory on several shards at once.
var recallRebuildSlot = make(chan struct{}, 1)

type recallMonitor struct {
	// Only one maintenance call samples or rebuilds this shard at once. Stats
	// readers take mu without waiting for native searches, a build, or I/O.
	maintenance sync.Mutex
	mu          sync.Mutex
	status      RecallStatus
	anchors     []string
	term        uint64
	rebuiltAt   uint64
	hasRebuilt  bool
	retryAfter  time.Time
}

func (s *Service) RecallStatus() RecallStatus {
	m := &s.recall
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// Recall scheduling uses the local publisher grant. An immutable service
// belongs to one replica incarnation. A new writer grant invalidates any low
// samples observed by the previous publisher.
func (s *Service) recallTerm() (uint64, bool) {
	term := uint64(1) // Standalone partitions have a single local writer.
	if s.publisherTerm != nil {
		var ok bool
		if term, ok = s.publisherTerm(); !ok {
			return 0, false
		}
	}
	g := s.graphReplica
	if g == nil {
		return term, true
	}
	if w := g.writer; w != nil {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.publisher == g.journal.owner.Partition && w.replicaLive() && w.leaderLease.InLeaderLease(&term) {
			return term, true
		}
		return 0, false
	}
	if s.publisherTerm != nil {
		return term, true
	}
	// Directly constructed graph services (without Worker management) learn
	// their role from the existing synchronization loop.
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	return term, g.leader
}

func (m *recallMonitor) due(now time.Time, term, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.term != term {
		m.term = term
		m.status.LowStreak = 0
		m.status.NextCheck = time.Time{}
		m.anchors = nil
	}
	if m.status.Suppressed && m.hasRebuilt {
		if epoch == m.rebuiltAt {
			return false
		}
		m.status.Suppressed = false
	}
	return !now.Before(m.status.NextCheck)
}

func (m *recallMonitor) observe(now time.Time, policy RecallPolicy, sample recallProbeResult, watermark uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := &m.status
	st.NextCheck = now.Add(policy.Interval)
	if sample.Queries == 0 {
		st.LowStreak = 0
		return false
	}
	st.Samples++
	st.LastChecked = now
	st.Queries = sample.Queries
	st.Recall = sample.Recall
	st.OriginalRecall, st.NearbyRecall = sample.OriginalRecall, sample.NearbyRecall
	m.anchors = sample.Anchors
	if sample.Recall >= policy.Target {
		st.LowStreak = 0
		st.Suppressed = false
		return false
	}
	if m.hasRebuilt && m.rebuiltAt == watermark {
		// A deterministic rebuild over unchanged input would recreate the same
		// graph. Retain the failed verification for visibility until it changes.
		st.LowStreak = 0
		st.Suppressed = true
		return false
	}
	st.LowStreak = min(st.LowStreak+1, policy.Consecutive)
	return st.LowStreak >= policy.Consecutive && !now.Before(m.retryAfter)
}

func (m *recallMonitor) finished(now time.Time, policy RecallPolicy, epoch uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if errors.Is(err, ErrConflict) {
		// New data or a contended publication lock can obsolete an otherwise
		// valid sample. Keep its evidence and retry at the sampling interval.
		m.status.Failures++
		m.status.NextCheck = now.Add(policy.Interval)
		return
	}
	m.status.LowStreak = 0
	m.status.Suppressed = false
	m.retryAfter = now.Add(policy.Cooldown)
	if err != nil {
		m.status.Failures++
		m.status.NextCheck = m.retryAfter
		return
	}
	m.status.Rebuilds++
	m.status.LastRebuild = now
	m.status.NextCheck = now.Add(policy.Interval)
	m.hasRebuilt, m.rebuiltAt = true, epoch
}

// A prepared or rolled-back graph patch can change the in-memory epoch
// without a new durable data position. Only new input should re-enable a
// second deterministic rebuild after an unsuccessful verification.
func (s *Service) recallWatermark(epoch uint64) uint64 {
	if g := s.graphReplica; g != nil {
		g.viewMu.RLock()
		defer g.viewMu.RUnlock()
		return g.position.DataOffset
	}
	return epoch
}

func (s *Service) checkRecall(ctx context.Context, term uint64) error {
	policy := DefaultRecallPolicy()
	if s.policy.Recall != nil {
		policy = *s.policy.Recall
	}
	if policy.Target == 0 {
		return nil
	}
	s.Index.mu.RLock()
	small := s.Index.live <= s.Index.exactThreshold
	epoch := s.Index.epoch
	s.Index.mu.RUnlock()
	if small {
		s.recall.mu.Lock()
		s.recall.status.LowStreak = 0
		s.recall.status.Suppressed = false
		s.recall.mu.Unlock()
		return nil
	}
	m := &s.recall
	if !m.due(time.Now(), term, s.recallWatermark(epoch)) {
		return nil
	}
	// The probe itself bounds CPU concurrency and drops its index lock before
	// brute-force validation. Continuous ingestion must not prevent sampling.
	sample, err := s.Index.probeRecall(ctx, m.anchors)
	if err != nil {
		m.finished(time.Now(), policy, 0, err)
		return err
	}
	current, ok := s.recallTerm()
	if !ok || current != term {
		return nil
	}
	if !m.observe(time.Now(), policy, sample, s.recallWatermark(sample.Epoch)) {
		return nil
	}
	select {
	case recallRebuildSlot <- struct{}{}:
		defer func() { <-recallRebuildSlot }()
	default:
		// Another shard won this round. Keep the streak for the next sample.
		return nil
	}
	after, err := s.rebuildForRecall(ctx, sample.Epoch, term)
	m.finished(time.Now(), policy, after.DataOffset, err)
	return err
}
