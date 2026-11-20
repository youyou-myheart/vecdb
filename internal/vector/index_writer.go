package vector

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"example.com/vector-storage/internal/lease"
	"example.com/vector-storage/internal/shardlog"
)

// op serializes graph appends, including epoch barriers and maintenance. mu
// protects short-lived snapshots only: ordinary admission never waits for a
// graph build or commit. Service.syncMu always precedes op when both are needed.
type indexWriter struct {
	op             sync.Mutex
	mu             sync.Mutex
	lease          indexLease
	leaderLease    shardlog.Node
	replicaLease   *lease.Handle
	publisher      IndexPartitionID
	publisherRoute Assignment
	head           GraphHead
	known          bool
	controls       []GraphPosition
}

func (w *indexWriter) replicaLive() bool {
	return w.replicaLease != nil && w.replicaLease.InLeaderLease(nil)
}

func cloneGraphHead(h GraphHead) GraphHead {
	h.Sources = maps.Clone(h.Sources)
	h.SourceEpochs = maps.Clone(h.SourceEpochs)
	return h
}
func (w *indexWriter) snapshot() (GraphHead, []byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	grant, live := w.leaderLease.Snapshot()
	if !w.replicaLive() || !live || grant.Epoch != w.lease.Generation || !w.known {
		return GraphHead{}, nil, ErrFenced
	}
	return cloneGraphHead(w.head), w.lease.token(), nil
}
func (w *indexWriter) authority(token []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	grant, ok := w.leaderLease.Snapshot()
	if !ok || grant.Epoch != w.lease.Generation || !w.replicaLive() || string(token) != string(w.lease.token()) {
		return ErrFenced
	}
	return nil
}
func (w *indexWriter) commitExpiry(token []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	leader, ok := w.leaderLease.Snapshot()
	if !ok || leader.Epoch != w.lease.Generation || w.replicaLease == nil || string(token) != string(w.lease.token()) {
		return 0, ErrFenced
	}
	replica, ok := w.replicaLease.Snapshot()
	if !ok {
		return 0, ErrFenced
	}
	return min(leader.Expires, replica.Expires), nil
}

func (w *indexWriter) invalidate() {
	w.mu.Lock()
	w.known = false
	w.controls = nil
	w.mu.Unlock()
}
func (w *indexWriter) retire() {
	w.mu.Lock()
	w.leaderLease.StepDown()
	w.known = false
	w.mu.Unlock()
}
func (w *indexWriter) accept(h GraphHead) {
	w.mu.Lock()
	w.head, w.known = cloneGraphHead(h), true
	w.mu.Unlock()
}
func (w *indexWriter) advance(e GraphEntry) {
	w.mu.Lock()
	w.head.Committed, w.head.Visible = e.Position, e.Position
	w.head.Sources, w.head.SourceEpochs, w.head.LogBytes = maps.Clone(e.Sources), maps.Clone(e.SourceEpochs), e.LogBytes
	w.mu.Unlock()
}

func (g *graphReplica) syncLeased(ctx context.Context, s *Service, rebuild bool, expectedTerm uint64) (err error) {
	w := g.writer
	w.op.Lock()
	defer w.op.Unlock()
	w.mu.Lock()
	live := w.replicaLive()
	grant, liveGrant := w.leaderLease.Snapshot()
	leader := liveGrant && grant.Epoch == w.lease.Generation
	expectedLeader := w.publisher == g.journal.owner.Partition
	known := w.known
	generation := w.lease.Generation
	token := w.lease.token()
	w.mu.Unlock()
	g.leader = leader
	if !live || (expectedLeader && !leader) || (expectedTerm != 0 && (!expectedLeader || generation != expectedTerm)) {
		return ErrFenced
	}
	if !leader {
		if rebuild {
			return ErrFenced
		}
		h, err := g.readHead(ctx)
		if err != nil {
			return err
		}
		return g.catchUp(ctx, s, h.Visible)
	}
	defer func() {
		if err != nil {
			w.invalidate()
			g.input.notify()
		}
	}()
	if rebuild {
		w.invalidate()
		// Maintenance retains the fully fenced slow path. Its append is serialized
		// with all cached appends and forces one recovery before reusing the cache.
		if err := g.syncSlow(ctx, s, true, token); err != nil {
			return err
		}
		return w.authority(token)
	}
	if !known {
		measure := g.track("index_lease_recovery")
		h, e := g.journal.claimLeased(ctx, token)
		if e == nil {
			e = g.catchUp(ctx, s, h.Committed)
		}
		if e == nil {
			e = w.authority(token)
		}
		if e == nil {
			w.accept(h)
		}
		measure(&e)
		if e != nil {
			return e
		}
	}
	h, token, err := w.snapshot()
	if err != nil {
		return err
	}
	// Epoch entries change no graph state. They have already committed under op;
	// apply their positions without re-reading our own journal.
	w.mu.Lock()
	controls := w.controls
	w.controls = nil
	w.mu.Unlock()
	for _, pos := range controls {
		if pos.Sequence <= g.position.Sequence {
			continue
		}
		if pos.Sequence != g.position.Sequence+1 {
			return ErrConflict
		}
		if err = g.apply(s, nil, nil, pos); err != nil {
			return err
		}
	}
	if g.position != h.Committed {
		return ErrConflict
	}
	defer g.track("index_lease_fast_sync")(&err)
	g.fastAppend = true
	defer func() { g.fastAppend = false }()
	if err = g.commitRouted(ctx, s, h); err != nil {
		return err
	}
	latest, _, err := w.snapshot()
	if err != nil {
		return err
	}
	if g.position != latest.Committed {
		if g.prepared != latest.Committed || g.delta == nil {
			return ErrConflict
		}
		if err = g.apply(s, g.delta, nil, latest.Committed); err != nil {
			return err
		}
		g.prepared, g.delta, g.reset = GraphPosition{}, nil, nil
	}
	return w.authority(token)
}

// Exact events and sparse coverage are proved at admission from immutable data
// logs. Only the source epoch and advancing cursor can change before commit.
func validateAdmitted(h GraphHead, q Delivery) error {
	key := fmt.Sprint(q.Source)
	if !q.foldEpoch || q.Owner.Epoch <= h.SourceEpochs[key] {
		if err := checkSourceEpoch(h, q); err != nil {
			return err
		}
	}
	committed := h.Sources[key]
	if q.Through > committed && q.Previous < committed {
		return ErrConflict
	}
	return nil
}

func (g *graphReplica) admitLeased(ctx context.Context, q Delivery) (GraphHead, error) {
	w := g.writer
	h, token, err := w.snapshot()
	if err != nil {
		return h, err
	}
	key := fmt.Sprint(q.Source)
	if q.Owner.Epoch < h.SourceEpochs[key] {
		return h, &SourceEpochError{q.Source, h.SourceEpochs[key], h.Sources[key]}
	}
	if q.foldEpoch && q.Owner.Epoch > h.SourceEpochs[key] {
		// Authorize the new source incarnation using the asynchronously
		// refreshed owner registry. The delivery MAC authenticates its WAL
		// projection; the registry binds the claimed epoch and session.
		c := g.input.cluster
		routes, err := c.localSearchRoutes()
		if err != nil {
			return h, err
		}
		if !sameOwner(routes.Owners[q.Owner.Partition], q.Owner) {
			c.expireSearchRoutes(routes)
			return h, ErrFenced
		}
	}
	if q.foldEpoch || q.Owner.Epoch == h.SourceEpochs[key] {
		// Validation IO does not hold op; the consumer can build/commit concurrently.
		var tx transactionAPI
		if !q.verified && q.Through > h.Sources[key] {
			tx, err = g.repo.begin()
			if err != nil {
				return h, err
			}
			defer tx.Rollback()
		}
		err = g.input.cluster.validateDeliveryBounds(ctx, tx, g.journal.id, q, h.Sources[key])
		if err == nil && q.Through > h.Sources[key] {
			err = g.input.cluster.validateDeliveryEvents(ctx, tx, g.journal.id, q)
		}
		if err != nil {
			return h, err
		}
		latest, currentToken, err := w.snapshot()
		if err == nil {
			if string(currentToken) != string(token) {
				err = ErrFenced
			} else {
				err = validateAdmitted(latest, q)
			}
		}
		return latest, err
	}
	w.op.Lock()
	defer w.op.Unlock()
	h, token, err = w.snapshot()
	if err != nil {
		return h, err
	}
	// Rare epoch changes persist a control entry. Bound un-applied controls too.
	w.mu.Lock()
	full := len(w.controls) >= 1024
	w.mu.Unlock()
	if full {
		g.input.notify()
		return h, ErrConflict
	}
	next, err := g.journal.admitSourceCached(ctx, g.input.cluster, q, h, token, w)
	if err != nil {
		w.invalidate()
		g.input.notify()
		return h, err
	}
	if next.Committed != h.Committed {
		w.mu.Lock()
		w.controls = append(w.controls, next.Committed)
		w.mu.Unlock()
		w.accept(next)
	}
	return next, nil
}
