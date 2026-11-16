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
	wake chan struct{}
}

func newSourceSignals() *sourceSignals {
	return &sourceSignals{ids: map[DataPartitionID]struct{}{}, wake: make(chan struct{}, 1)}
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

func (c *ClusterService) notifySource(ctx context.Context, id DataPartitionID) error {
	owner, err := c.coordinator.Resolve(ctx, sourceID(id))
	if err != nil {
		return err
	}
	if owner.Node == c.node.ID && owner.Session == c.node.Session {
		c.signalSource(id, "received", true)
		return nil
	}
	body, err := json.Marshal(partitionRPC{Namespace: c.root.Prefix, Partition: sourceID(id), Operation: "wake_source", Source: &id})
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
