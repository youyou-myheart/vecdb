package vector

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
)

// op serializes graph appends, including epoch barriers and maintenance. mu
// protects short-lived snapshots only: ordinary admission never waits for a
// graph build or commit. Service.syncMu always precedes op when both are needed.
type indexWriter struct {
	op              sync.Mutex
	mu              sync.Mutex
	lease           indexLease
	deadline        time.Time
	replicaDeadline time.Time
	publisher       IndexPartitionID
	publisherRoute  Assignment
	head            GraphHead
	known           bool
	retired         bool
	controls        []GraphPosition
}

func cloneGraphHead(h GraphHead) GraphHead {
	h.Sources = maps.Clone(h.Sources)
	h.SourceEpochs = maps.Clone(h.SourceEpochs)
	return h
}
func (w *indexWriter) snapshot() (GraphHead, []byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retired || !time.Now().Before(w.deadline) || !w.known {
		return GraphHead{}, nil, ErrFenced
	}
	return cloneGraphHead(w.head), w.lease.token(), nil
}
func (w *indexWriter) authority(token []byte) error {
	return w.appendAuthority(token, nil)
}
func (w *indexWriter) appendAuthority(token []byte, tx transactionAPI) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retired || !time.Now().Before(w.deadline) || string(token) != string(w.lease.token()) {
		return ErrFenced
	}
	if tx != nil && txnMillis(tx) >= w.lease.Expires {
		return ErrFenced
	}
	return nil
}
func (w *indexWriter) invalidate() {
	w.mu.Lock()
	w.known = false
	w.controls = nil
	w.mu.Unlock()
}
func (w *indexWriter) retire() {
	w.mu.Lock()
	w.retired = true
	w.known = false
	w.mu.Unlock()
}
func (w *indexWriter) install(start time.Time, lease indexLease) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retired {
		return
	}
	if lease.Generation < w.lease.Generation || (lease.Generation == w.lease.Generation && lease.Expires < w.lease.Expires) {
		return
	}
	if string(w.lease.token()) != string(lease.token()) {
		w.known = false
		w.controls = nil
	}
	w.lease, w.deadline = lease, leaseDeadline(start, lease.remaining)
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

// Called only by the control loop, after a confirmed replica renewal. A failed
// grant never extends local authority. The monotonic deadline starts before IO.
func (g *graphReplica) refreshLease(ctx context.Context, plan ReplicaPlan, replicaStart time.Time) (err error) {
	w := g.writer
	if w == nil {
		return nil
	}
	w.mu.Lock()
	w.publisher = plan.Publisher
	w.publisherRoute = Assignment{}
	for _, a := range plan.Serving {
		if a.Partition == plan.Publisher {
			w.publisherRoute = a
			break
		}
	}
	w.replicaDeadline = leaseDeadline(replicaStart, time.Duration(g.journal.c.ttl)*time.Millisecond)
	w.mu.Unlock()
	defer g.input.notify()
	if plan.Publisher != g.journal.owner.Partition || plan.LeaseDraining {
		return nil
	}
	defer g.track("index_lease_renew")(&err)
	started := time.Now()
	grant, err := g.journal.c.grantIndexLease(ctx, g.journal.id, g.journal.owner)
	if err == nil {
		// Ordinary renewals must not wait for construction/rebuild. A fresh guard
		// is installed only between appends; if one is busy, leave the old local
		// authority unchanged and retry on the next control tick. The durable guard
		// already prevents any late old append, and failed installs do not extend it.
		w.mu.Lock()
		same := string(w.lease.token()) == string(grant.token())
		if same && !w.retired && grant.Expires >= w.lease.Expires {
			w.lease, w.deadline = grant, leaseDeadline(started, grant.remaining)
		}
		w.mu.Unlock()
		if !same {
			if !w.op.TryLock() {
				return ErrConflict
			}
			w.install(started, grant)
			w.op.Unlock()
		}
	}
	return err
}

func (g *graphReplica) syncLeased(ctx context.Context, s *Service, rebuild bool) (err error) {
	w := g.writer
	w.op.Lock()
	defer w.op.Unlock()
	w.mu.Lock()
	live := !w.retired && time.Now().Before(w.replicaDeadline)
	leader := !w.retired && time.Now().Before(w.deadline)
	expectedLeader := w.publisher == g.journal.owner.Partition
	known := w.known
	token := w.lease.token()
	w.mu.Unlock()
	g.leader = leader
	if !live || (expectedLeader && !leader) {
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
		return g.syncSlow(ctx, s, true)
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
	if err := checkSourceEpoch(h, q); err != nil {
		return err
	}
	committed := h.Sources[fmt.Sprint(q.Source)]
	if q.Through > committed && q.Previous < committed {
		return ErrConflict
	}
	return nil
}

func (g *graphReplica) admitLeased(ctx context.Context, q Delivery) (GraphHead, error) {
	w := g.writer
	h, _, err := w.snapshot()
	if err != nil {
		return h, err
	}
	key := fmt.Sprint(q.Source)
	if q.Owner.Epoch < h.SourceEpochs[key] {
		return h, &SourceEpochError{q.Source, h.SourceEpochs[key], h.Sources[key]}
	}
	if q.Owner.Epoch == h.SourceEpochs[key] {
		// Validation IO does not hold op; the consumer can build/commit concurrently.
		tx, err := g.repo.begin()
		if err != nil {
			return h, err
		}
		defer tx.Rollback()
		err = g.input.cluster.validateDeliveryBounds(ctx, tx, g.journal.id, q, h.Sources[key])
		if err == nil && q.Through > h.Sources[key] {
			err = g.input.cluster.validateDeliveryEvents(ctx, tx, g.journal.id, q)
		}
		if err != nil {
			return h, err
		}
		latest, _, err := w.snapshot()
		if err == nil {
			err = validateAdmitted(latest, q)
		}
		return latest, err
	}
	w.op.Lock()
	defer w.op.Unlock()
	h, token, err := w.snapshot()
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
