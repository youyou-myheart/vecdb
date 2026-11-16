package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	owner     Ownership
	completed uint64
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
	for {
		select {
		case <-ctx.Done():
			c.releaseDispatchers()
			return
		case <-ticker.C:
			if c.worker != nil {
				c.dispatchTick(ctx)
			}
		}
	}
}
func (c *ClusterService) releaseDispatchers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, d := range c.dispatchers {
		_, _ = c.coordinator.Update(ctx, d.owner, false, true)
	}
}
func (c *ClusterService) dispatchTick(ctx context.Context) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	var wg sync.WaitGroup
	for d := 0; d < c.layout.DataPartitions; d++ {
		id := DataPartitionID(d)
		state := c.dispatchers[id]
		a, e := c.coordinator.Assignment(ctx, sourceID(id))
		if e != nil {
			continue
		}
		if a.Node != c.node.ID || a.Session != c.node.Session || c.draining.Load() {
			if state != nil {
				_, _ = c.coordinator.Update(ctx, state.owner, false, true)
				delete(c.dispatchers, id)
			}
			continue
		}
		if state == nil {
			o, err := c.coordinator.Acquire(ctx, sourceID(id), c.node)
			if err != nil {
				continue
			}
			state = &dataDispatcher{owner: o}
			c.dispatchers[id] = state
		}
		o, e := c.coordinator.Update(ctx, state.owner, true, false)
		if e != nil {
			delete(c.dispatchers, id)
			continue
		}
		state.owner = o
		wg.Add(1)
		go func(id DataPartitionID, s *dataDispatcher) {
			defer wg.Done()
			err := c.dispatchSource(ctx, id, s)
			c.reportDispatch(ctx, s, err)
		}(id, state)
	}
	wg.Wait()
}
func (c *ClusterService) dispatchSource(ctx context.Context, id DataPartitionID, s *dataDispatcher) error {
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	done, e := c.completedIn(ctx, tx, id)
	_ = tx.Rollback()
	if e != nil {
		return e
	}
	s.completed = done
	if len(s.window) == 0 {
		batch, err := c.data[id].readEvents(ctx, done, 0, dispatchBatchSize, dispatchBytes)
		if err != nil {
			return err
		}
		s.window = batch.Events
	}
	if len(s.window) == 0 {
		return nil
	}
	commits := map[IndexPartitionID]uint64{}
	for _, b := range c.layout.Bindings {
		h, err := (graphJournal{c: c.coordinator, id: b.Index}).Head(ctx)
		if err != nil {
			return err
		}
		commits[b.Index] = h.Sources[fmt.Sprint(id)]
	}
	next := completedPrefix(s.window, done, c.eventTargets, commits)
	if next > done {
		tx, e = c.root.begin()
		if e != nil {
			return e
		}
		defer tx.Rollback()
		if e = c.coordinator.Fence(s.owner)(ctx, tx); e != nil {
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
		s.window = s.window[int(next-done):]
		s.completed = next
		done = next
	}
	var wg sync.WaitGroup
	errs := make([]error, len(c.layout.Bindings))
	for n, b := range c.layout.Bindings {
		q := Delivery{Source: id, Owner: s.owner, RouteVersion: c.layout.Epoch, Previous: max(commits[b.Index], done)}
		for _, event := range s.window {
			if event.Sequence <= q.Previous {
				continue
			}
			for _, target := range c.eventTargets(event) {
				if target == b.Index {
					q.Events = append(q.Events, event)
					q.Through = event.Sequence
					break
				}
			}
		}
		if len(q.Events) == 0 {
			continue
		}
		wg.Add(1)
		go func(n int, target IndexPartitionID, q Delivery) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if e := c.deliver(callCtx, target, q); e != nil {
				errs[n] = fmt.Errorf("%s: %w", target, e)
			}
		}(n, b.Index, q)
	}
	wg.Wait()
	return errors.Join(errs...)
}
func (c *ClusterService) deliver(ctx context.Context, id IndexPartitionID, q Delivery) error {
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
		return ErrPartitionUnavailable
	}
	return nil
}

func (c *ClusterService) validateDelivery(ctx context.Context, tx transactionAPI, id IndexPartitionID, q Delivery, committed uint64) error {
	if q.Source < 0 || int(q.Source) >= c.layout.DataPartitions || q.RouteVersion != c.layout.Epoch || q.Owner.Partition != sourceID(q.Source) {
		return ErrFenced
	}
	if e := c.coordinator.Fence(q.Owner)(ctx, tx); e != nil {
		return e
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
	if len(q.Events) == 0 || len(q.Events) > dispatchBatchSize || q.Through != q.Events[len(q.Events)-1].Sequence {
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
	// The authenticated source owns routing; transport retries cannot change its
	// immutable route version. No new per-event routing records are persisted.
	return nil
}

func (c *ClusterService) reportDispatch(ctx context.Context, s *dataDispatcher, cause error) {
	tx, e := c.root.begin()
	if e != nil {
		return
	}
	defer tx.Rollback()
	o, e := c.coordinator.readOwner(ctx, tx, s.owner.Partition)
	if e != nil || !sameOwner(o, s.owner) || o.Expires <= txnMillis(tx) {
		return
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	if o.Offset == s.completed && o.LastError == message {
		return
	}
	o.Offset = s.completed
	o.LastError = message
	o.Phase = "Dispatching"
	if e = putJSON(tx, c.coordinator.ownerKey(o.Partition), o); e == nil {
		_ = tx.Commit(ctx)
	}
}
