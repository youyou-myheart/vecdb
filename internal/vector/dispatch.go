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
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"example.com/vector-storage/internal/lease"
	"example.com/vector-storage/internal/shardlog"
)

// Read the available prefix immediately. A larger window amortizes a graph
// commit across records that hash to this source and route to the same index.
const dispatchBatchSize = 512

// A live TxnKV owner can certify sparse projections across this wider range.
// A single RPC and a single graph build still contain at most 512 records.
const dispatchLookahead = 4 * dispatchBatchSize
const dispatchBytes = 2 << 20

// Previous is a coverage boundary, not necessarily the previous data LSN:
// intervening events can belong to other indexes. A skip beyond the receiver's
// commit is allowed only through the source's durable completed prefix.
type Delivery struct {
	proof        []byte
	verified     bool
	Source       DataPartitionID `json:"source"`
	Owner        Ownership       `json:"owner"`
	RouteVersion uint64          `json:"route_version"`
	Previous     uint64          `json:"previous"`
	Through      uint64          `json:"through"`
	Events       []LogEvent      `json:"events"`
}
type dataDispatcher struct {
	lastError    atomic.Pointer[string]
	wake         chan struct{}
	cancel       context.CancelFunc
	stopped      chan struct{}
	writer       *mutationGroup
	owner        Ownership
	completed    uint64
	persisted    uint64
	recoveryHead uint64 // WAL tail recovered before accepting new requests
	checkpointAt time.Time
	windows      int
	recovered    bool
	window       []LogEvent
	projection   dispatchProjection
}

// The routing epoch is fixed for this dispatcher. While graph ACKs arrive, a
// burst of data writes can wake the same window repeatedly. Cache its verified
// projection and drop just the prefix that became durable on every destination.
type dispatchProjection struct {
	first, last uint64
	byIndex     map[IndexPartitionID][]LogEvent
	targets     []IndexPartitionID
	routed      [][]IndexPartitionID
}

func (p *dispatchProjection) update(c *ClusterService, events []LogEvent) {
	first, last := events[0].Sequence, events[len(events)-1].Sequence
	if p.first <= first && p.last == last && p.byIndex != nil {
		if p.first == first {
			return
		}
		p.routed = p.routed[first-p.first:]
		p.first = first
		active := p.targets[:0]
		for _, target := range p.targets {
			rows := p.byIndex[target]
			for len(rows) > 0 && rows[0].Sequence < first {
				rows[0] = LogEvent{}
				rows = rows[1:]
			}
			p.byIndex[target] = rows
			if len(rows) > 0 {
				active = append(active, target)
			}
		}
		p.targets = active
		return
	}
	*p = dispatchProjection{first: first, last: last, byIndex: make(map[IndexPartitionID][]LogEvent), routed: make([][]IndexPartitionID, len(events))}
	for n, event := range events {
		p.routed[n] = c.eventTargets(event)
		for _, target := range p.routed[n] {
			p.byIndex[target] = append(p.byIndex[target], event)
		}
	}
	for _, binding := range c.layout.Bindings {
		if len(p.byIndex[binding.Index]) != 0 {
			p.targets = append(p.targets, binding.Index)
		}
	}
}

func (p *dispatchProjection) destinations(event LogEvent) []IndexPartitionID {
	return p.routed[event.Sequence-p.first]
}

func completedKey(r *Repository) []byte { return []byte(r.Prefix + "dispatch-completed") }
func (c *ClusterService) completedIn(ctx context.Context, tx dataReader, d DataPartitionID) (uint64, error) {
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
	ticker := time.NewTicker(dispatchControlInterval)
	defer ticker.Stop()
	if c.worker != nil {
		c.dispatchTick(ctx)
	}
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
			for _, id := range c.dispatchSignals.take() {
				c.wakeDispatcher(id)
			}
		}
	}
}

func (c *ClusterService) releaseDispatchers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for id, d := range c.dispatchers {
		d.stop()
		c.retireWriter(id)
		_ = c.coordinator.releaseOwner(ctx, d.owner, nil)
	}
}

func (s *dataDispatcher) stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.stopped
	}
}

func (c *ClusterService) wakeDispatcher(id DataPartitionID) {
	s := c.dispatchers[id]
	if s == nil || c.draining.Load() || !s.writer.lease.InLeaderLease(nil) {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// One goroutine owns each source window. Slow deliveries cannot delay another
// source or the control loop that keeps the writers' leases alive.
func (c *ClusterService) runDispatcher(ctx context.Context, id DataPartitionID, s *dataDispatcher) {
	defer close(s.stopped)
	retry := initialIndexRetry
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		for !c.draining.Load() && s.writer.lease.InLeaderLease(nil) && ctx.Err() == nil {
			err := c.dispatchSource(ctx, id, s)
			if err == nil {
				s.lastError.Store(nil)
				retry = initialIndexRetry
				break
			}
			msg := err.Error()
			s.lastError.Store(&msg)
			if !waitIndexRetry(ctx, retry) {
				return
			}
			retry = min(2*retry, maxIndexRetry)
		}
	}
}

func (c *ClusterService) dispatchTick(ctx context.Context) {
	defer c.observations.pipeline.track("dispatch_tick", "")(nil)
	assignments, err := c.placementValues(ctx, true)
	if err != nil {
		return
	}
	// Each partition has its own lease transaction. Bound concurrent TiKV
	// requests while keeping ownership of the dispatcher map in this loop.
	next := make([]*dataDispatcher, c.layout.DataPartitions)
	var tasks errgroup.Group
	tasks.SetLimit(4)
	for n := range next {
		id := DataPartitionID(n)
		var a Assignment
		if value, ok := assignments[string(c.coordinator.assignmentKey(sourceID(id)))]; ok {
			if err := json.Unmarshal(value, &a); err != nil || a.Partition != sourceID(id) {
				next[n] = c.dispatchers[id]
				continue
			}
		}
		current := c.dispatchers[id]
		tasks.Go(func() error {
			next[id] = c.reconcileDispatcher(ctx, id, current, a)
			return nil
		})
	}
	_ = tasks.Wait()
	for n, s := range next {
		id := DataPartitionID(n)
		if s == nil {
			delete(c.dispatchers, id)
		} else {
			c.dispatchers[id] = s
			c.wakeDispatcher(id)
		}
	}
}

func (c *ClusterService) reconcileDispatcher(ctx context.Context, id DataPartitionID, s *dataDispatcher, a Assignment) *dataDispatcher {
	assigned := a.Node == c.node.ID && a.Session == c.node.Session && !c.draining.Load()
	if s != nil && (!assigned || s.writer.lease.uncertain.Load()) {
		s.stop()
		c.retireWriter(id)
		_ = c.coordinator.releaseOwner(ctx, s.owner, nil)
		s = nil
	}
	if !assigned {
		return nil
	}
	var current Ownership
	var state *appendLease
	if s != nil {
		state = s.writer.lease
		if grant, live := s.writer.lease.Snapshot(); live && time.Until(grant.Deadline) > time.Duration(c.coordinator.ttl)*time.Millisecond/2 {
			return s
		}
		current = s.owner
	} else {
		state = &appendLease{}
	}
	var elected Ownership
	err := c.data[id].dataLog(&state.Node, nil, nil, false).Campaign(ctx, func(ctx context.Context) (lease.Grant, time.Time, error) {
		o, started, err := c.coordinator.campaignOwner(ctx, c.node, sourceID(id), current, func(o Ownership) error {
			c.observations.acquisitions.Add(1)
			writer, err := c.recoverWriterWithLease(ctx, id, o, state)
			if err != nil {
				return err
			}
			s = &dataDispatcher{owner: o, writer: writer, recoveryHead: writer.lease.head.Load()}
			return nil
		}, func(o *Ownership) {
			o.Offset = max(o.Offset, s.writer.lease.indexed.Load())
			o.Phase = "Dispatching"
			o.LastError = ""
			if last := s.lastError.Load(); last != nil {
				o.LastError = *last
			}
		})
		elected = o
		return lease.Grant{Epoch: o.Epoch, Expires: o.Expires, Remaining: time.Duration(c.coordinator.ttl) * time.Millisecond}, started, err
	})
	if err != nil {
		state.StepDown()
		if s != nil {
			s.stop()
		}
		c.retireWriter(id)
		return nil
	}
	if s.wake == nil {
		s.owner = elected
		var work context.Context
		work, s.cancel = context.WithCancel(ctx)
		s.wake, s.stopped = make(chan struct{}, 1), make(chan struct{})
		s.writer.dispatchWake = s.wake
		go c.runDispatcher(work, id, s)
	}
	c.writerMu.Lock()
	c.writers[id] = s.writer
	c.writerMu.Unlock()
	if s.writer.rawWriter != nil {
		s.writer.repo.raw.local.Store(s.writer.repo.Prefix, s.writer)
	}
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
		s.projection = dispatchProjection{}
		return c.flushCompleted(ctx, id, s, false)
	}
	// Each graph tail carries cumulative ACKs only for the events routed there.
	// A delete broadcasts to all indexes, so its proof still requires all of them.
	s.projection.update(c, s.window)
	targets, byIndex := s.projection.targets, s.projection.byIndex
	commits, e := c.dispatchCommits(ctx, id, s, targets, byIndex)
	if e != nil {
		return e
	}
	next := completedPrefix(s.window, done, s.projection.destinations, commits)
	if next > done {
		s.completed = next
		if s.writer != nil {
			s.writer.lease.indexed.Store(next)
		}
		s.windows++
		if e = c.flushCompleted(ctx, id, s, s.writer == nil); e != nil {
			return e
		}
		clear(s.window[:int(next-done)])
		s.window = s.window[int(next-done):]
		done = next
		if len(s.window) == 0 {
			// All its target graphs confirmed the window. Read the next bounded
			// window immediately, without waiting for another control tick.
			c.signalSource(id, "window_drained", true)
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, len(targets))
	signWindow := s.writer != nil && s.writer.rawWriter == nil && c.token != ""
	for n, target := range targets {
		previous := max(commits[target], s.persisted)
		// The live owner built byIndex from a complete, contiguous durable WAL
		// window. Its completed prefix is backed by durable graph ACKs, even
		// when the source's recovery checkpoint has not been flushed yet.
		if signWindow {
			previous = max(commits[target], done)
		}
		events := byIndex[target]
		for len(events) > 0 && events[0].Sequence <= previous {
			events = events[1:]
		}
		if len(events) == 0 {
			continue
		}
		events = events[:min(len(events), dispatchBatchSize)]
		// A long sparse skip needs a durable completed checkpoint for old peers.
		if !signWindow && events[len(events)-1].Sequence-previous > dispatchBatchSize {
			if e = c.flushCompleted(ctx, id, s, true); e != nil {
				wg.Wait()
				return e
			}
			previous = max(commits[target], done)
		}
		q := Delivery{Source: id, Owner: s.owner, RouteVersion: c.layout.Epoch, Previous: previous, Through: events[len(events)-1].Sequence, Events: events}
		if s.writer != nil && s.writer.proofs.recentlyAdmitted(target, q.Through) {
			continue
		}
		if signWindow {
			var liveEpoch uint64
			if !s.writer.lease.InLeaderLease(&liveEpoch) || liveEpoch != s.owner.Epoch {
				wg.Wait()
				return ErrFenced
			}
			q.proof, e = c.deliveryMAC(target, q, events)
			if e != nil {
				wg.Wait()
				return e
			}
		} else if s.writer != nil && s.writer.rawWriter != nil && c.token != "" {
			var err error
			q.proof, err = c.signRawDelivery(target, q, s.writer, s.persisted)
			if err != nil {
				wg.Wait()
				return err
			}
		}
		if s.writer != nil {
			s.writer.proofs.noteDelivery(target, q.Through)
		}
		wg.Add(1)
		go func(n int, target IndexPartitionID, q Delivery) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if e := c.deliver(callCtx, target, q); e != nil {
				errs[n] = fmt.Errorf("%s: %w", target, e)
			} else if s.writer != nil {
				s.writer.proofs.admitted(target, q.Through)
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
	var tx dataReader
	var e error
	if s.recovered {
		tx, e = c.data[id].beginDataRead(ctx)
	} else {
		tx, e = c.root.begin()
	}
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
		s.completed, s.persisted = done, done
		if s.writer != nil {
			s.writer.lease.indexed.Store(done)
		}
		s.windows = 0
		s.checkpointAt = time.Now()
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
		limit := dispatchBatchSize
		var knownHead *uint64
		if s.writer != nil {
			knownHead = &head
			if s.writer.rawWriter == nil && c.token != "" {
				limit = dispatchLookahead
			}
		}
		var reader shardlog.Reader[LogBatch] = c.data[id].dataLog(nil, tx, knownHead, !s.recovered)
		batch, err := reader.ReadCommitted(ctx, shardlog.Window{After: done, Limit: limit, ByteLimit: dispatchBytes})
		read(&err)
		if err != nil {
			return err
		}
		s.recovered = true
		s.window = batch.Events
	}
	return nil
}
func (c *ClusterService) flushCompleted(ctx context.Context, id DataPartitionID, s *dataDispatcher, force bool) error {
	if s.completed == s.persisted || !force && s.windows < 100 && time.Since(s.checkpointAt) < 500*time.Millisecond {
		return nil
	}
	if err := c.advanceCompleted(ctx, id, s.owner, s.persisted, s.completed); err != nil {
		s.recovered = false
		s.window = nil
		return err
	}
	s.persisted = s.completed
	s.checkpointAt = time.Now()
	s.windows = 0
	return nil
}

// Recovery seeds new targets from the graph journal. Confirmed graph appends
// subsequently push cumulative progress. A lost notification costs one poll.
func (c *ClusterService) dispatchCommits(ctx context.Context, id DataPartitionID, s *dataDispatcher, targets []IndexPartitionID, events map[IndexPartitionID][]LogEvent) (map[IndexPartitionID]uint64, error) {
	if s.writer == nil {
		return c.sourceCommits(ctx, id, targets)
	}
	commits := make(map[IndexPartitionID]uint64, len(targets))
	var missing []IndexPartitionID
	for _, target := range targets {
		proof := s.writer.proofs.read(target)
		commits[target] = proof.through
		// Old-epoch WAL may already be indexed beyond the durable source
		// checkpoint. New WAL gets its first delivery without a graph read.
		recovering := proof.checked.IsZero() && events[target][0].Sequence <= s.recoveryHead
		stalled := proof.through < proof.offered && time.Since(proof.checked) >= 500*time.Millisecond
		if recovering || stalled {
			missing = append(missing, target)
		}
	}
	if len(missing) > 0 {
		result, err := c.sourceCommits(ctx, id, missing)
		if err != nil {
			return nil, err
		}
		for target, n := range result {
			s.writer.proofs.record(target, n, true)
			commits[target] = max(n, commits[target])
		}
	}
	return commits, nil
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
	commit := tx.Commit
	if native, ok := nativeTiKV(tx); ok && c.coordinator.holdReleasedLeases {
		grant, err := c.coordinator.liveOwner(ctx, tx, owner)
		if err != nil {
			return err
		}
		// The successor cannot take ownership until grant.Expires. Bound the
		// only mutation (the cursor CAS) at TiKV as well as checking this snapshot.
		commit = func(ctx context.Context) error { return native.commitOnePhaseBefore(ctx, grant.Expires) }
	} else if e = c.coordinator.Fence(owner)(ctx, tx); e != nil {
		return e
	}
	old, err := c.completedIn(ctx, tx, id)
	if err != nil {
		return err
	}
	if old != done || next < old {
		return ErrConflict
	}
	if e = putJSON(tx, completedKey(c.data[id]), next); e != nil {
		return e
	}
	return commit(ctx)
}

func (c *ClusterService) deliver(ctx context.Context, id IndexPartitionID, q Delivery) (err error) {
	defer c.observations.pipeline.track("dispatch_deliver", string(id))(&err)
	defer func() {
		c.observations.deliveries.Add(1)
		if err != nil {
			c.observations.deliveryErrors.Add(1)
		}
	}()
	if c.routeHintsEnabled() {
		p := remotePartition{cluster: c, id: id}
		_, err = p.cachedRoutedCall(ctx, "deliver", Search{}, &q)
		return err
	}
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
	setDeliveryHeader(request, &q)
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
	if q.Previous > committed && !q.verified {
		done, e := c.completedIn(ctx, tx, q.Source)
		if e != nil {
			return e
		}
		if q.Previous > done {
			return errors.New("source coverage gap")
		}
	}
	maxSpan := uint64(dispatchBatchSize)
	if q.verified {
		maxSpan = dispatchLookahead
	}
	if len(q.Events) == 0 || len(q.Events) > dispatchBatchSize || q.Through <= q.Previous || q.Through-q.Previous > maxSpan || q.Through != q.Events[len(q.Events)-1].Sequence {
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
	if q.verified {
		return nil
	}
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
