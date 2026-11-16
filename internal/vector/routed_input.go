package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// One bounded in-flight batch per source. Only the graph leader drains it;
// enqueue never waits for construction, object uploads or graph publication.
type routedInput struct {
	cluster     *ClusterService
	mu          sync.Mutex
	queue       map[DataPartitionID]*queuedDelivery
	bytes       int
	lastSource  int
	scrubCursor string
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
		}
	}()
	tx, e := g.repo.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = g.journal.c.ReplicaFence(g.journal.id, g.journal.owner)(ctx, tx); e != nil {
		return e
	}
	h, e := g.journal.headIn(ctx, tx)
	if e != nil {
		return e
	}
	if e = in.cluster.validateDelivery(ctx, tx, g.journal.id, q, h.Sources[fmt.Sprint(q.Source)]); e != nil {
		return e
	}
	if q.Through <= h.Sources[fmt.Sprint(q.Source)] {
		in.cluster.observations.duplicates.Add(1)
		return nil
	}
	if e = in.cluster.validateDeliveryEvents(ctx, tx, g.journal.id, q); e != nil {
		return e
	}
	raw, e := json.Marshal(q)
	if e != nil {
		return e
	}
	if len(raw) > dispatchBytes+(64<<10) {
		return errors.New("delivery exceeds byte budget")
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if old := in.queue[q.Source]; old != nil {
		if sameOwner(old.Owner, q.Owner) && old.Previous == q.Previous && old.Through == q.Through {
			in.cluster.observations.duplicates.Add(1)
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
	// Queue ownership checks are repeated in the durable commit transaction.
	in.queue[q.Source] = &queuedDelivery{Delivery: q, bytes: len(raw), admitted: time.Now()}
	in.bytes += len(raw)
	in.cluster.observations.admissions.Add(1)
	return nil
}
func (in *routedInput) take() *queuedDelivery {
	in.mu.Lock()
	defer in.mu.Unlock()
	for n := 0; n < in.cluster.layout.DataPartitions; n++ {
		d := DataPartitionID((in.lastSource + 1 + n) % in.cluster.layout.DataPartitions)
		if q := in.queue[d]; q != nil {
			delete(in.queue, d)
			in.bytes -= q.bytes
			in.lastSource = int(d)
			return q
		}
	}
	return nil
}
func (g *graphReplica) commitRouted(ctx context.Context, s *Service, h GraphHead) (err error) {
	q := g.input.take()
	if q == nil {
		return nil
	}
	p := &g.input.cluster.observations.pipeline
	partition := string(g.journal.id)
	if !q.admitted.IsZero() {
		p.observe("index_queue_wait", partition, time.Since(q.admitted), nil)
	}
	defer p.track("routed_commit", partition)(&err)
	defer func() {
		p.init()
		p.batch.WithLabelValues(partition, metricOutcome(err)).Observe(float64(len(q.Events)))
	}()
	tx, e := g.repo.begin()
	if e != nil {
		return e
	}
	e = g.input.cluster.validateDelivery(ctx, tx, g.journal.id, q.Delivery, h.Sources[fmt.Sprint(q.Source)])
	_ = tx.Rollback()
	if e != nil {
		return e
	}
	if q.Through <= h.Sources[fmt.Sprint(q.Source)] {
		return nil
	}
	records := make([]Record, 0, len(q.Events))
	for _, event := range q.Events {
		records = append(records, event.Record)
	}
	return g.commitRecords(ctx, s, h, records, &q.Delivery)
}
func (g *graphReplica) commitRecords(ctx context.Context, s *Service, h GraphHead, records []Record, q *Delivery) error {
	// Multiple revisions of one ID in a single unpublished batch collapse to
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
	build := g.track("graph_build")
	delta, e := s.Index.GenerateDelta(compact)
	build(&e)
	if e != nil {
		return e
	}
	encode := g.track("graph_encode")
	raw, e := json.Marshal(delta)
	encode(&e)
	if e != nil {
		return e
	}
	m, e := g.saveBlob(ctx, deltaFormat, raw)
	if e != nil {
		return e
	}
	commit := g.track("graph_commit")
	_, e = g.journal.commit(ctx, h, h.Committed.DataOffset+1, "delta", m, q, g.input.cluster)
	commit(&e)
	return e
}

// Scan authoritative records in bounded pages. Only remove stale local versions
// when the latest revision is deleted or belongs elsewhere. Changes are serialized
// with ingestion and replicated as graph deltas, never edited on one replica only.
func (g *graphReplica) scrub(ctx context.Context, s *Service) error {
	c := g.input.cluster
	h, e := g.journal.Head(ctx)
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
	h, e = g.journal.Head(ctx)
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
		if e = g.commitRecords(ctx, s, h, removes, nil); e != nil {
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
