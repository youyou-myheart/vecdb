package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const dispatchBatchSize = 32
const dispatchBytes = 2 << 20

// Previous is a coverage boundary, not necessarily the previous data LSN:
// intervening events can belong to other indexes. A skip beyond the receiver's
// commit is allowed only through the source's durable completed prefix.
type Delivery struct {
	Source       DataPartitionID `json:"source"`
	Owner        Ownership       `json:"owner"`
	RouteVersion uint64          `json:"route_version"`
	Previous     uint64          `json:"previous"`
	Through      uint64          `json:"through"`
	Events       []LogEvent      `json:"events"`
}
type dataDispatcher struct {
	deadline  time.Time
	lastError string
	writer    *mutationGroup
	owner     Ownership
	completed uint64
	recovered bool
	window    []LogEvent
}

func completedKey(r *Repository) []byte { return []byte(r.Prefix + "dispatch-completed") }
func (c *ClusterService) completedIn(ctx context.Context, tx transactionAPI, d DataPartitionID) (uint64, error) {
	return c.data[d].readCounter(ctx, tx, "dispatch-completed")
}
func (c *ClusterService) eventTargets(e LogEvent) []IndexPartitionID {
	if !e.Record.Deleted {
		return []IndexPartitionID{c.layout.Routing.target(e.Record.Vector)}
	}
	return c.router.QueryIndexes(Search{})
}
func completedPrefix(events []LogEvent, start uint64, targets func(LogEvent) []IndexPartitionID, commits map[IndexPartitionID]uint64) uint64 {
	for _, e := range events {
		if e.Sequence != start+1 {
			break
		}
		done := true
		for _, id := range targets(e) {
			if commits[id] < e.Sequence {
				done = false
				break
			}
		}
		if !done {
			break
		}
		start = e.Sequence
	}
	return start
}
func (c *ClusterService) dispatchLoop(ctx context.Context) {
	defer close(c.dispatchDone)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	if c.worker != nil {
		c.dispatchTick(ctx)
	}
	retry := initialIndexRetry
	for {
		select {
		case <-ctx.Done():
			c.releaseDispatchers()
			return
		case <-ticker.C:
			if c.worker != nil {
				c.dispatchTick(ctx)
			}
		case <-c.dispatchSignals.wake:
			ids := c.dispatchSignals.take()
			if c.worker != nil && c.dispatchSelected(ctx, ids, false) {
				if !waitIndexRetry(ctx, retry) {
					c.releaseDispatchers()
					return
				}
				retry = min(2*retry, maxIndexRetry)
			} else {
				retry = initialIndexRetry
			}
		}
	}
}
func (c *ClusterService) releaseDispatchers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for id, d := range c.dispatchers {
		c.retireWriter(id)
		_, _ = c.coordinator.Update(ctx, d.owner, false, true)
	}
}
func (c *ClusterService) dispatchTick(ctx context.Context) {
	ids := make([]DataPartitionID, c.layout.DataPartitions)
	for i := range ids {
		ids[i] = DataPartitionID(i)
	}
	c.dispatchSelected(ctx, ids, true)
}

func (c *ClusterService) dispatchSelected(ctx context.Context, ids []DataPartitionID, control bool) bool {
	defer c.observations.pipeline.track("dispatch_tick", "")(nil)
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	var wg sync.WaitGroup
	failed := make([]bool, len(ids))
	for n, id := range ids {
		state := c.dispatchers[id]
		if control || (state != nil && state.writer.lease.uncertain.Load()) {
			state = c.reconcileDispatcher(ctx, id, state)
		}
		// A wake is a hint. Only the control loop grants or extends authority.
		if state == nil || c.draining.Load() || !time.Now().Before(state.deadline) {
			continue
		}
		wg.Add(1)
		go func(n int, id DataPartitionID, s *dataDispatcher) {
			defer wg.Done()
			err := c.dispatchSource(ctx, id, s)
			s.lastError = ""
			if err != nil {
				s.lastError = err.Error()
				failed[n] = true
				c.signalSource(id, "retry", true)
			}
		}(n, id, state)
	}
	wg.Wait()
	for _, failure := range failed {
		if failure {
			return true
		}
	}
	return false
}

// Each source has exactly one dispatcher goroutine in a round. Reconciliation
// runs under dispatchMu between rounds, so status and lease observations agree.
func (c *ClusterService) reconcileDispatcher(ctx context.Context, id DataPartitionID, s *dataDispatcher) *dataDispatcher {
	a, err := c.coordinator.Assignment(ctx, sourceID(id))
	if err != nil {
		return nil
	}
	assigned := a.Node == c.node.ID && a.Session == c.node.Session && !c.draining.Load()
	if s != nil && (!assigned || s.writer.lease.uncertain.Load()) {
		c.retireWriter(id)
		_, _ = c.coordinator.Update(ctx, s.owner, false, true)
		delete(c.dispatchers, id)
		s = nil
	}
	if !assigned {
		return nil
	}
	if s == nil {
		o, err := c.coordinator.Acquire(ctx, sourceID(id), c.node)
		if err != nil {
			return nil
		}
		c.observations.acquisitions.Add(1)
		writer, err := c.recoverWriter(ctx, id, o)
		if err != nil {
			_, _ = c.coordinator.update(ctx, o, false, true, func(o *Ownership) { o.LastError = err.Error() })
			return nil
		}
		s = &dataDispatcher{owner: o, writer: writer}
	}
	started := time.Now()
	o, err := c.coordinator.update(ctx, s.owner, true, false, func(o *Ownership) {
		if s.recovered {
			o.Offset = s.completed
		}
		o.Phase = "Dispatching"
		o.LastError = s.lastError
	})
	if err != nil {
		c.retireWriter(id)
		delete(c.dispatchers, id)
		return nil
	}
	s.owner = o
	s.deadline = leaseDeadline(started, time.Duration(c.coordinator.ttl)*time.Millisecond)
	s.writer.lease.expires.Store(o.Expires)
	c.dispatchers[id] = s
	c.writerMu.Lock()
	c.writers[id] = s.writer
	c.writerMu.Unlock()
	return s
}
func (c *ClusterService) dispatchSource(ctx context.Context, id DataPartitionID, s *dataDispatcher) (err error) {
	started := time.Now()
	defer c.observations.pipeline.track("dispatch_source", string(sourceID(id)))(&err)
	defer func() {
		c.observations.dispatches.Add(1)
		c.observations.dispatchNanos.Add(uint64(time.Since(started)))
		if err != nil {
			c.observations.dispatchErrors.Add(1)
		}
	}()
	if err = c.loadDispatchWindow(ctx, id, s); err != nil {
		return err
	}
	done := s.completed
	if len(s.window) == 0 {
		return nil
	}
	// Each graph tail carries cumulative ACKs only for the events routed there.
	// A delete broadcasts to all indexes, so its proof still requires all of them.
	byIndex := make(map[IndexPartitionID][]LogEvent)
	for _, event := range s.window {
		for _, target := range c.eventTargets(event) {
			byIndex[target] = append(byIndex[target], event)
		}
	}
	targets := make([]IndexPartitionID, 0, len(byIndex))
	for _, binding := range c.layout.Bindings {
		if len(byIndex[binding.Index]) != 0 {
			targets = append(targets, binding.Index)
		}
	}
	commits, e := c.sourceCommits(ctx, id, targets)
	if e != nil {
		return e
	}
	next := completedPrefix(s.window, done, c.eventTargets, commits)
	if next > done {
		if e = c.advanceCompleted(ctx, id, s.owner, done, next); e != nil {
			// Reconstruct the bounded window at the durable cursor on the next attempt.
			s.recovered = false
			s.window = nil
			return e
		}
		clear(s.window[:int(next-done)])
		s.window = s.window[int(next-done):]
		s.completed = next
		done = next
		if len(s.window) == 0 {
			// The previous window is durably complete. Read the next bounded
			// window immediately, without waiting for another control tick.
			c.signalSource(id, "window_drained", true)
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, len(targets))
	for n, target := range targets {
		previous := max(commits[target], done)
		events := byIndex[target]
		for len(events) > 0 && events[0].Sequence <= previous {
			events = events[1:]
		}
		if len(events) == 0 {
			continue
		}
		q := Delivery{Source: id, Owner: s.owner, RouteVersion: c.layout.Epoch, Previous: previous, Through: events[len(events)-1].Sequence, Events: events}
		wg.Add(1)
		go func(n int, target IndexPartitionID, q Delivery) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if e := c.deliver(callCtx, target, q); e != nil {
				errs[n] = fmt.Errorf("%s: %w", target, e)
			}
		}(n, target, q)
	}
	wg.Wait()
	return errors.Join(errs...)
}
func (c *ClusterService) loadDispatchWindow(ctx context.Context, id DataPartitionID, s *dataDispatcher) error {
	var head uint64
	if s.writer != nil {
		head = s.writer.lease.head.Load()
	}
	if s.recovered && (len(s.window) > 0 || (s.writer != nil && s.completed == head)) {
		return nil
	}
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	done := s.completed
	if !s.recovered {
		done, e = c.completedIn(ctx, tx, id)
		if e != nil {
			return e
		}
		s.completed = done
	}
	// Commit can succeed in TiKV even when its response is lost. Reconcile the
	// cached window against the durable prefix during recovery.
	for len(s.window) > 0 && s.window[0].Sequence <= done {
		s.window[0] = LogEvent{}
		s.window = s.window[1:]
	}
	if len(s.window) > 0 && s.window[0].Sequence != done+1 {
		// The durable log is authoritative; reconstruct a discontinuous cache.
		s.window = nil
	}
	if len(s.window) == 0 {
		read := c.observations.pipeline.track("dispatch_read_log", string(sourceID(id)))
		repo := c.data[id]
		var batch LogBatch
		var err error
		if s.writer != nil {
			batch, err = repo.readEventsAtHead(ctx, tx, done, 0, head, dispatchBatchSize, dispatchBytes)
		} else {
			batch, err = repo.readEventsIn(ctx, tx, done, 0, dispatchBatchSize, dispatchBytes)
		}
		if err == nil && !s.recovered && batch.Head > batch.Floor {
			// On takeover also verify the first retained key, even if completed
			// is already far ahead. Both endpoints and the cursor share one snapshot.
			var first []byte
			first, err = tx.FirstKey(ctx, []byte(repo.Prefix+"log/"), []byte(repo.Prefix+"log0"))
			if err == nil && !bytes.Equal(first, repo.logKey(batch.Floor+1)) {
				err = fmt.Errorf("data log gap at retained lower bound %d", batch.Floor+1)
			}
		}
		read(&err)
		if err != nil {
			return err
		}
		s.recovered = true
		s.window = batch.Events
	}
	return nil
}
func (c *ClusterService) sourceCommits(ctx context.Context, id DataPartitionID, targets []IndexPartitionID) (_ map[IndexPartitionID]uint64, err error) {
	defer c.observations.pipeline.track("dispatch_poll_commits", string(sourceID(id)))(&err)
	defer func() {
		c.observations.pipeline.observeBatch("source_heads", string(sourceID(id)), len(targets), err)
	}()
	tx, err := c.root.begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	keys := make([][]byte, 0, len(targets))
	for _, target := range targets {
		keys = append(keys, (graphJournal{c: c.coordinator, id: target}).key("head"))
	}
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	cached := prefetchedTransaction{transactionAPI: tx, values: values}
	commits := map[IndexPartitionID]uint64{}
	for _, target := range targets {
		h, err := (graphJournal{c: c.coordinator, id: target}).headIn(ctx, cached)
		if err != nil {
			return nil, err
		}
		commits[target] = h.Sources[fmt.Sprint(id)]
	}
	return commits, nil
}

func (c *ClusterService) advanceCompleted(ctx context.Context, id DataPartitionID, owner Ownership, done, next uint64) (err error) {
	defer c.observations.pipeline.track("dispatch_advance_completed", string(sourceID(id)))(&err)
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = c.coordinator.Fence(owner)(ctx, tx); e != nil {
		return e
	}
	old, err := c.completedIn(ctx, tx, id)
	if err != nil {
		return err
	}
	if old != done {
		return ErrConflict
	}
	if e = putJSON(tx, completedKey(c.data[id]), next); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	return nil
}

func (c *ClusterService) deliver(ctx context.Context, id IndexPartitionID, q Delivery) (err error) {
	defer c.observations.pipeline.track("dispatch_deliver", string(id))(&err)
	defer func() {
		c.observations.deliveries.Add(1)
		if err != nil {
			c.observations.deliveryErrors.Add(1)
		}
	}()
	plan, e := c.coordinator.Plan(ctx, id)
	if e != nil {
		return e
	}
	owner, e := c.coordinator.Resolve(ctx, plan.Publisher)
	if e != nil {
		return e
	}
	req := partitionRPC{Namespace: c.root.Prefix, Partition: owner.Partition, Epoch: owner.Epoch, Operation: "deliver", Delivery: &q}
	raw, e := json.Marshal(req)
	if e != nil {
		return e
	}
	request, e := http.NewRequestWithContext(ctx, "POST", owner.URL+"/internal/partition", bytes.NewReader(raw))
	if e != nil {
		return e
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	response, e := c.client.Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		var reply partitionReply
		if json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&reply) == nil && reply.Code == "fenced" {
			if reply.SourceEpoch != nil {
				return reply.SourceEpoch
			}
			return ErrFenced
		}
		return ErrPartitionUnavailable
	}
	return nil
}

func (c *ClusterService) validateDelivery(ctx context.Context, tx transactionAPI, id IndexPartitionID, q Delivery, h GraphHead) error {
	if err := checkSourceEpoch(h, q); err != nil {
		return err
	}
	return c.validateDeliveryBounds(ctx, tx, id, q, h.Sources[fmt.Sprint(q.Source)])
}

func (c *ClusterService) validateDeliveryBounds(ctx context.Context, tx transactionAPI, id IndexPartitionID, q Delivery, committed uint64) error {
	if q.Source < 0 || int(q.Source) >= c.layout.DataPartitions || q.RouteVersion != c.layout.Epoch || q.Owner.Partition != sourceID(q.Source) || q.Owner.Epoch == 0 {
		return ErrFenced
	}
	if q.Through <= committed {
		return nil
	}
	if q.Previous < committed {
		return ErrConflict
	}
	if q.Previous > committed {
		done, e := c.completedIn(ctx, tx, q.Source)
		if e != nil {
			return e
		}
		if q.Previous > done {
			return errors.New("source coverage gap")
		}
	}
	if len(q.Events) == 0 || len(q.Events) > dispatchBatchSize || q.Through <= q.Previous || q.Through-q.Previous > dispatchBatchSize || q.Through != q.Events[len(q.Events)-1].Sequence {
		return errors.New("invalid delivery bounds")
	}
	last := q.Previous
	for _, event := range q.Events {
		if event.Sequence <= last || c.router.DataForID(event.Record.ID) != q.Source {
			return errors.New("unordered delivery")
		}
		if e := validateBatch([]Record{event.Record}, c.dimension); e != nil {
			return e
		}
		if !event.Record.Deleted && c.layout.Routing.target(event.Record.Vector) != id {
			return errors.New("incorrect clustered destination")
		}
		last = event.Sequence
	}
	return nil
}

// Verify coverage once on admission, at the same TiKV snapshot as the graph
// watermark. A strictly ordered subset alone does not prove that no event for
// this index was omitted. The immutable routing version makes replay sufficient;
// no per-event routing journal is needed. Commit rechecks the Index-local epoch
// and cursors; its Index fence rejects a stale snapshot after concurrent append/GC.
func (c *ClusterService) validateDeliveryEvents(ctx context.Context, tx transactionAPI, id IndexPartitionID, q Delivery) error {
	b, e := c.data[q.Source].readEventsAtHead(ctx, tx, q.Previous, q.Through, q.Through, dispatchBatchSize, dispatchBytes)
	if e != nil {
		return e
	}
	if len(b.Events) == 0 || b.Events[len(b.Events)-1].Sequence != q.Through {
		return errors.New("delivery exceeds durable source range")
	}
	n := 0
	for _, event := range b.Events {
		if !event.Record.Deleted && c.layout.Routing.target(event.Record.Vector) != id {
			continue
		}
		if n >= len(q.Events) || q.Events[n].Sequence != event.Sequence || !sameRecord(q.Events[n].Record, event.Record) {
			return errors.New("delivery does not cover durable source events")
		}
		n++
	}
	if n != len(q.Events) {
		return errors.New("delivery includes unexpected events")
	}
	return nil
}
