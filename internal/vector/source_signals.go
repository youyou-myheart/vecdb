package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

// Bounded by configured data partitions, not the number of writes. A token
// says to inspect the set; coalescing never discards a dirty partition.
type sourceSignals struct {
	mu   sync.Mutex
	ids  map[DataPartitionID]struct{}
	acks map[DataPartitionID]map[IndexPartitionID]graphCommitAck
	wake chan struct{}
}

func newSourceSignals() *sourceSignals {
	return &sourceSignals{ids: map[DataPartitionID]struct{}{}, acks: make(map[DataPartitionID]map[IndexPartitionID]graphCommitAck), wake: make(chan struct{}, 1)}
}

func (s *sourceSignals) add(id DataPartitionID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.ids[id] = struct{}{}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Each proof names the data-owner incarnation that supplied the committed
// input. Delivery admission alone never generates a proof.
type graphCommitAck struct {
	Index   IndexPartitionID `json:"index"`
	Session string           `json:"session"`
	Epoch   uint64           `json:"epoch"`
	Through uint64           `json:"through"`
}
type graphCommitProof struct {
	through         uint64
	offered         uint64
	checked         time.Time
	admittedThrough uint64
	admittedAt      time.Time
}

// Admission suppresses redundant sends while the leader is building. Only a
// durable ACK advances through. Lost volatile queues are retried after a tick.
func (p *graphCommitProofs) recentlyAdmitted(id IndexPartitionID, through uint64) bool {
	old := p.read(id)
	return old.admittedThrough == through && time.Since(old.admittedAt) < dispatchControlInterval
}

func (p *graphCommitProofs) admitted(id IndexPartitionID, through uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.byIndex[id] // noteDelivery creates the map before the RPC
	old.admittedThrough, old.admittedAt = through, time.Now()
	p.byIndex[id] = old
}

type graphCommitProofs struct {
	mu      sync.RWMutex
	byIndex map[IndexPartitionID]graphCommitProof
}

func (p *graphCommitProofs) record(id IndexPartitionID, n uint64, polled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	old, found := p.byIndex[id]
	if found && n <= old.through && !polled {
		return
	}
	if p.byIndex == nil {
		p.byIndex = make(map[IndexPartitionID]graphCommitProof)
	}
	old.through = max(n, old.through)
	old.checked = time.Now()
	p.byIndex[id] = old
}

// Start the lost-ACK timer when a target actually has work in flight.
// A retry of the same delivery must not postpone reconciliation.
func (p *graphCommitProofs) noteDelivery(id IndexPartitionID, through uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	proof := p.byIndex[id]
	if through <= max(proof.through, proof.offered) {
		return
	}
	if proof.offered <= proof.through {
		proof.checked = time.Now()
	}
	proof.offered = through
	if p.byIndex == nil {
		p.byIndex = make(map[IndexPartitionID]graphCommitProof)
	}
	p.byIndex[id] = proof
}
func (p *graphCommitProofs) read(id IndexPartitionID) graphCommitProof {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.byIndex[id]
}
func (s *sourceSignals) addAck(id DataPartitionID, ack graphCommitAck) {
	s.mu.Lock()
	acks := s.acks[id]
	if acks == nil {
		acks = make(map[IndexPartitionID]graphCommitAck)
		s.acks[id] = acks
	}
	old := acks[ack.Index]
	if ack.Epoch > old.Epoch || old.Epoch == ack.Epoch && ack.Through > old.Through {
		acks[ack.Index] = ack
	}
	s.mu.Unlock()
}
func (s *sourceSignals) takeAcks(id DataPartitionID) []graphCommitAck {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]graphCommitAck, 0, len(s.acks[id]))
	for _, a := range s.acks[id] {
		out = append(out, a)
	}
	delete(s.acks, id)
	return out
}

func (c *ClusterService) receiveGraphAcks(id DataPartitionID, acks []graphCommitAck) {
	for _, ack := range acks {
		group, err := c.localDataWriter(id, ack.Epoch, ack.Session)
		if err == nil {
			c.recordGraphAck(group, ack)
		}
	}
}

func (c *ClusterService) recordGraphAck(group *mutationGroup, ack graphCommitAck) {
	if _, known := c.indexes[ack.Index]; known && ack.Through <= group.lease.head.Load() {
		group.proofs.record(ack.Index, ack.Through, false)
	}
}

// Deliver the result directly to a co-located source while its incarnation
// still holds the lease. A retired group cannot advance its successor's proofs.
func (c *ClusterService) signalGraphCommit(index IndexPartitionID, q Delivery, commitErr error) {
	if c.outboundSignals == nil {
		return
	}
	ack := graphCommitAck{Index: index, Epoch: q.Owner.Epoch, Session: q.Owner.Session, Through: q.Through}
	if q.Owner.Node == c.node.ID && q.Owner.Session == c.node.Session {
		group, err := c.localDataWriter(q.Source, ack.Epoch, ack.Session)
		if err == nil && sameOwner(group.lease.owner, q.Owner) {
			if commitErr == nil {
				c.recordGraphAck(group, ack)
			}
			c.signalSource(q.Source, "graph_commit", true)
			return
		}
	}
	if commitErr == nil {
		c.outboundSignals.addAck(q.Source, ack)
	}
	c.signalSource(q.Source, "graph_commit", false)
}

func (s *sourceSignals) take() []DataPartitionID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]DataPartitionID, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	clear(s.ids)
	return ids
}

func (c *ClusterService) signalSource(id DataPartitionID, reason string, local bool) {
	if c.outboundSignals == nil || id < 0 || int(id) >= c.layout.DataPartitions {
		return
	}
	p := &c.observations.pipeline
	p.init()
	p.dispatchWake.WithLabelValues(string(sourceID(id)), reason).Inc()
	if local {
		c.writerMu.RLock()
		group := c.writers[id]
		c.writerMu.RUnlock()
		if group != nil && group.dispatchWake != nil {
			// The dispatcher checks its own incarnation before doing any work.
			// Wake it directly even while unrelated leases are being renewed.
			select {
			case group.dispatchWake <- struct{}{}:
			default:
			}
			return
		}
		c.dispatchSignals.add(id)
	} else {
		c.outboundSignals.add(id)
	}
}

func (c *ClusterService) notifySourceLoop(ctx context.Context) {
	defer close(c.notifyDone)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-c.outboundSignals.wake:
		}
		var wg sync.WaitGroup
		for _, id := range c.outboundSignals.take() {
			wg.Add(1)
			go func(id DataPartitionID) {
				defer wg.Done()
				callCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				defer cancel()
				measure := c.observations.pipeline.track("dispatch_notify", string(sourceID(id)))
				err := c.notifySource(callCtx, id)
				measure(&err)
			}(id)
		}
		wg.Wait()
	}
}

func (c *ClusterService) notifySource(ctx context.Context, id DataPartitionID) (err error) {
	acks := c.outboundSignals.takeAcks(id)
	var owner Ownership
	var state *ClusterStatus
	defer func() {
		if err != nil && state != nil {
			c.expireSearchRoutes(state)
		}
	}()
	if c.routeHintsEnabled() {
		state, err = c.cachedSearchRoutes(ctx, false)
		if err == nil {
			owner = state.Owners[sourceID(id)]
			if owner.Epoch == 0 {
				c.expireSearchRoutes(state)
				return ErrPartitionUnavailable
			}
		}
	} else {
		owner, err = c.coordinator.Resolve(ctx, sourceID(id))
	}
	if err != nil {
		return err
	}
	if owner.Node == c.node.ID && owner.Session == c.node.Session {
		c.receiveGraphAcks(id, acks)
		c.signalSource(id, "received", true)
		return nil
	}
	body, err := json.Marshal(partitionRPC{Namespace: c.root.Prefix, Partition: sourceID(id), Operation: "wake_source", Source: &id, Acks: acks})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", owner.URL+"/internal/partition", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusOK {
		return ErrPartitionUnavailable
	}
	return nil
}
