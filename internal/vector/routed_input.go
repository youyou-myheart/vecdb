package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// One bounded in-flight batch per source. Only the graph leader drains it;
// enqueue never waits for construction, object uploads or graph append.
type routedInput struct {
	cluster          *ClusterService
	mu               sync.Mutex
	queue            map[DataPartitionID]*queuedDelivery
	bytes            int
	lastSource       int
	scrubCursor      string
	wake             chan struct{}
	controlVersion   uint64
	controlPublisher IndexPartitionID
	waiting          atomic.Bool
}

func newRoutedInput(c *ClusterService) *routedInput {
	return &routedInput{cluster: c, queue: map[DataPartitionID]*queuedDelivery{}, wake: make(chan struct{}, 1)}
}

// The notification is sticky and coalesced. Never close wake: an in-flight
// admission can still notify while the ownership context is being cancelled.
func (in *routedInput) notify() {
	select {
	case in.wake <- struct{}{}:
	default:
	}
}

func (in *routedInput) hasQueued() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return len(in.queue) != 0
}

func (in *routedInput) planChanged(version uint64, publisher IndexPartitionID) {
	in.mu.Lock()
	changed := in.controlVersion != version || in.controlPublisher != publisher
	in.controlVersion, in.controlPublisher = version, publisher
	in.mu.Unlock()
	if changed {
		in.notify()
	}
}

type queuedDelivery struct {
	Delivery
	bytes    int
	admitted time.Time
}

// Caller holds in.mu. Retries retain the original admission time.
func (in *routedInput) oldestAge(now time.Time) time.Duration {
	var oldest time.Duration
	for _, q := range in.queue {
		if !q.admitted.IsZero() && now.Sub(q.admitted) > oldest {
			oldest = now.Sub(q.admitted)
		}
	}
	return oldest
}

func (g *graphReplica) enqueue(ctx context.Context, q Delivery) (err error) {
	defer g.track("index_admission")(&err)
	in := g.input
	defer func() {
		if err != nil {
			in.cluster.observations.rejections.Add(1)
			g.observeSourceEpochRejection("admission", err)
		}
	}()
	var h GraphHead
	var e error
	q.foldEpoch = g.repo.packed != nil && q.verified && g.writer != nil
	if g.writer != nil {
		h, e = g.admitLeased(ctx, q)
	} else {
		h, e = g.journal.admitSource(ctx, in.cluster, q)
	}
	if e != nil {
		return e
	}
	// A durable epoch barrier is replayable even if admission later encounters
	// backpressure or the request is an already committed duplicate.
	in.notify()
	raw, e := json.Marshal(q)
	if e != nil {
		return e
	}
	if len(raw) > dispatchBytes+(64<<10) {
		return errors.New("delivery exceeds byte budget")
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if old := in.queue[q.Source]; old != nil && old.Owner.Epoch < q.Owner.Epoch {
		in.bytes -= old.bytes
		delete(in.queue, q.Source)
	}
	if q.Through <= h.Sources[fmt.Sprint(q.Source)] {
		in.cluster.signalGraphCommit(g.journal.id, q, nil)
		in.cluster.observations.duplicates.Add(1)
		return nil
	}
	if old := in.queue[q.Source]; old != nil {
		if sameOwner(old.Owner, q.Owner) && old.Previous == q.Previous && old.Through == q.Through {
			in.cluster.observations.duplicates.Add(1)
			in.notify()
			return nil
		}
		return ErrConflict
	}
	if in.bytes+len(raw) > 4<<20 {
		return errors.New("index input queue full")
	}
	// Own the validated data until commit; callers cannot modify a queued batch.
	events := make([]LogEvent, len(q.Events))
	for n, event := range q.Events {
		events[n] = LogEvent{Sequence: event.Sequence, Record: cloneRecord(event.Record)}
	}
	q.Events = events
	// The accepted epoch is rechecked in the durable commit transaction.
	in.queue[q.Source] = &queuedDelivery{Delivery: q, bytes: len(raw), admitted: time.Now()}
	in.bytes += len(raw)
	in.cluster.observations.admissions.Add(1)
	in.notify()
	return nil
}

// Combine only work already queued; never delay admission to fill a batch.
// Retain source rotation and bound both construction work and encoded input.
const indexBatchRecords = dispatchBatchSize

func (in *routedInput) takeBatch() []*queuedDelivery {
	in.mu.Lock()
	defer in.mu.Unlock()
	var batch []*queuedDelivery
	records, size, start := 0, 0, in.lastSource
	for n := 0; n < in.cluster.layout.DataPartitions; n++ {
		d := DataPartitionID((start + 1 + n) % in.cluster.layout.DataPartitions)
		if q := in.queue[d]; q != nil {
			if len(batch) > 0 && (records+len(q.Events) > indexBatchRecords || size+q.bytes > dispatchBytes) {
				break
			}
			delete(in.queue, d)
			in.bytes -= q.bytes
			in.lastSource = int(d)
			batch = append(batch, q)
			records += len(q.Events)
			size += q.bytes
		}
	}
	return batch
}
func (g *graphReplica) commitRouted(ctx context.Context, s *Service, h GraphHead) (err error) {
	batch := g.input.takeBatch()
	if len(batch) == 0 {
		return nil
	}
	// Wake every consumed source on failures, including validation failures
	// before graph construction. A hint never substitutes for durable ACK reads.
	defer func() {
		if err != nil {
			for _, q := range batch {
				g.input.cluster.signalSource(q.Source, "retry", false)
			}
			g.input.notify()
		}
	}()
	p := &g.input.cluster.observations.pipeline
	partition := string(g.journal.id)
	count := 0
	for _, q := range batch {
		count += len(q.Events)
		if !q.admitted.IsZero() {
			p.observe("index_queue_wait", partition, time.Since(q.admitted), nil)
		}
	}
	defer p.track("routed_commit", partition)(&err)
	defer func() {
		p.init()
		p.batch.WithLabelValues(partition, metricOutcome(err)).Observe(float64(count))
	}()
	var tx transactionAPI
	var e error
	if !g.fastAppend {
		tx, e = g.repo.begin()
		if e != nil {
			return e
		}
		defer tx.Rollback()
	}
	var inputs []Delivery
	var records []Record
	for _, q := range batch {
		if g.fastAppend {
			e = validateAdmitted(h, q.Delivery)
		} else {
			e = g.input.cluster.validateDelivery(ctx, tx, g.journal.id, q.Delivery, h)
		}
		if e != nil {
			return e
		}
		if q.Through <= h.Sources[fmt.Sprint(q.Source)] {
			continue
		}
		inputs = append(inputs, q.Delivery)
		for _, event := range q.Events {
			records = append(records, event.Record)
		}
	}
	if len(inputs) == 0 {
		return nil
	}
	started := make([]time.Time, 0, len(batch))
	for _, q := range batch {
		started = append(started, q.admitted)
	}
	return g.commitRecordBatch(ctx, s, h, records, inputs, started...)
}
func (g *graphReplica) commitRecords(ctx context.Context, s *Service, h GraphHead, records []Record, q *Delivery) (err error) {
	var inputs []Delivery
	if q != nil {
		inputs = []Delivery{*q}
	}
	return g.commitRecordBatch(ctx, s, h, records, inputs)
}

func (g *graphReplica) commitRecordBatch(ctx context.Context, s *Service, h GraphHead, records []Record, inputs []Delivery, admitted ...time.Time) (err error) {
	defer func() {
		if err != nil {
			g.input.notify()
		}
	}()
	// Multiple revisions of one ID in a single input batch collapse to
	// the last revision; native deltas contain one new active node per ID.
	latest := map[string]int{}
	for i, r := range records {
		if n, ok := latest[r.ID]; !ok || r.Revision >= records[n].Revision {
			latest[r.ID] = i
		}
	}
	compact := make([]Record, 0, len(latest))
	for i, r := range records {
		if latest[r.ID] == i {
			compact = append(compact, r)
		}
	}
	for _, started := range admitted {
		if !started.IsZero() {
			g.input.cluster.observations.pipeline.observe("index_queue_to_build", string(g.journal.id), time.Since(started), nil)
		}
	}
	build := g.track("graph_build")
	delta, e := s.Index.GenerateDelta(compact)
	build(&e)
	if e != nil {
		return e
	}
	return g.appendDelta(ctx, s, h, h.Committed.DataOffset+1, delta, inputs)
}

// Scan authoritative records in bounded pages. Only remove stale local versions
// when the latest revision is deleted or belongs elsewhere. Changes are serialized
// with ingestion and replicated as graph deltas, never edited on one replica only.
func (g *graphReplica) scrub(ctx context.Context, s *Service) error {
	c := g.input.cluster
	h, e := g.readHead(ctx)
	if e != nil {
		return e
	}
	plan, e := g.journal.c.Plan(ctx, g.journal.id)
	if e != nil {
		return e
	}
	if plan.Publisher != g.journal.owner.Partition {
		return nil
	}
	if e = g.sync(ctx, s, false); e != nil {
		return e
	}
	h, e = g.readHead(ctx)
	if e != nil {
		return e
	}
	if h.Committed != h.Visible {
		return nil
	}
	page, e := c.ListRecords(ctx, ListOptions{Limit: 128, Cursor: g.input.scrubCursor, IncludeDeleted: true})
	if e != nil {
		return e
	}
	var removes []Record
	c.observations.scrubScanned.Add(uint64(len(page.Records)))
	s.Index.mu.RLock()
	for _, r := range page.Records {
		old, ok := s.Index.records[r.ID]
		if !ok || old.Deleted || old.Revision >= r.Revision {
			continue
		}
		if r.Deleted || c.layout.Routing.target(r.Vector) != g.journal.id {
			removes = append(removes, Record{ID: r.ID, Revision: r.Revision, Deleted: true})
		}
	}
	s.Index.mu.RUnlock()
	if len(removes) > 0 {
		if g.writer != nil {
			g.writer.op.Lock()
			g.writer.invalidate()
		}
		e = g.commitRecords(ctx, s, h, removes, nil)
		if g.writer != nil {
			g.writer.op.Unlock()
			g.input.notify()
		}
		if e != nil {
			return e
		}
		c.observations.scrubRemoved.Add(uint64(len(removes)))
		if e = g.sync(ctx, s, false); e != nil {
			return e
		}
	}
	g.input.scrubCursor = page.NextCursor
	return nil
}
