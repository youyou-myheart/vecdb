package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Hints only. Worker read leases, never this cache, authorize a search.
type searchRouteCache struct {
	mu       sync.Mutex
	refresh  chan struct{}
	state    *ClusterStatus
	deadline time.Time
}

func scanRouteJSON[T any](ctx context.Context, tx transactionAPI, prefix string, limit int) (map[string]T, error) {
	it, err := tx.Iter([]byte(prefix), []byte(prefix+"\xff"))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := map[string]T{}
	for it.Valid() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if len(out) >= limit {
			return nil, errors.New("route registry limit")
		}
		var value T
		if err = json.Unmarshal(it.Value(), &value); err != nil {
			return nil, err
		}
		out[strings.TrimPrefix(string(it.Key()), prefix)] = value
		if err = it.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// A single snapshot and three bounded prefix scans; no data frontiers, graph
// heads, progress logs, or per-partition transactions from the status endpoint.
func (c *ClusterService) readSearchRoutes(ctx context.Context) (_ ClusterStatus, err error) {
	defer c.observations.pipeline.track("search_route_snapshot", "")(&err)
	tx, err := c.root.begin()
	if err != nil {
		return ClusterStatus{}, err
	}
	defer tx.Rollback()
	nodes, err := c.coordinator.nodesIn(ctx, tx)
	if err != nil {
		return ClusterStatus{}, err
	}
	plans, err := scanRouteJSON[ReplicaPlan](ctx, tx, c.root.Prefix+"cluster/replica-plans/", 64)
	if err != nil {
		return ClusterStatus{}, err
	}
	owners, err := scanRouteJSON[Ownership](ctx, tx, c.root.Prefix+"cluster/owners/", 4096)
	if err != nil {
		return ClusterStatus{}, err
	}
	// Plans are keyed by index, not self-identifying. Derive logical IDs from
	// serving replica IDs and keep only immutable configured index partitions.
	allowed := map[IndexPartitionID]bool{}
	for _, b := range c.layout.Bindings {
		allowed[b.Index] = true
	}
	serving := map[IndexPartitionID]bool{}
	if c.replicas == 1 {
		for id := range allowed {
			serving[id] = true
		}
	}
	for _, p := range plans {
		for _, a := range p.Serving {
			serving[a.Partition] = true
		}
	}
	nodeByID := map[string]ClusterNode{}
	for _, n := range nodes {
		nodeByID[n.ID] = n
	}
	state := ClusterStatus{Routing: c.layout.Routing, Nodes: nodes, ReplicaOwners: map[IndexPartitionID]ReplicaOwners{}, ReplicaPlans: map[IndexPartitionID]ReplicaPlan{}, Owners: map[IndexPartitionID]Ownership{}}
	for id, plan := range plans {
		if allowed[IndexPartitionID(id)] {
			state.ReplicaPlans[IndexPartitionID(id)] = plan
		}
	}
	now := txnMillis(tx)
	for _, o := range owners {
		id := IndexPartitionID(strings.SplitN(string(o.Partition), "~", 2)[0])
		n, exists := nodeByID[o.Node]
		if !o.Ready || o.Expires <= now || !exists || n.Session != o.Session {
			continue
		}
		if strings.HasPrefix(string(o.Partition), "data-") {
			for d := range c.data {
				if o.Partition == sourceID(d) {
					state.Owners[o.Partition] = o
					break
				}
			}
			continue
		}
		if !allowed[id] || !serving[o.Partition] {
			continue
		}
		r := state.ReplicaOwners[id]
		r.Owners = append(r.Owners, o)
		state.ReplicaOwners[id] = r
	}
	return state, nil
}

func (c *ClusterService) cachedSearchRoutes(ctx context.Context, force bool) (*ClusterStatus, error) {
	cache := &c.searchRoutes
	cache.mu.Lock()
	if cache.refresh == nil {
		cache.refresh = make(chan struct{}, 1)
	}
	flight := cache.refresh
	state, until := cache.state, cache.deadline
	cache.mu.Unlock()
	if !force && state != nil && time.Now().Before(until) {
		return state, nil
	}
	select {
	case flight <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-flight }()
	cache.mu.Lock()
	state, until = cache.state, cache.deadline
	cache.mu.Unlock()
	if !force && state != nil && time.Now().Before(until) {
		return state, nil
	}
	started := time.Now()
	fresh, err := c.readSearchRoutes(ctx)
	if err != nil {
		return nil, err
	}
	ttl := min(2*time.Second, time.Duration(c.coordinator.ttl)*time.Millisecond/3)
	cache.mu.Lock()
	cache.state = &fresh
	cache.deadline = started.Add(ttl)
	cache.mu.Unlock()
	return &fresh, nil
}

// SearchRouting returns a detached public hint snapshot: callers cannot mutate
// the Gateway's cached owner/session/epoch addresses.
func (c *ClusterService) SearchRouting(ctx context.Context) (ClusterStatus, error) {
	state, err := c.cachedSearchRoutes(ctx, false)
	if err != nil {
		return ClusterStatus{}, err
	}
	b, err := json.Marshal(state)
	if err != nil {
		return ClusterStatus{}, err
	}
	var out ClusterStatus
	err = json.Unmarshal(b, &out)
	return out, err
}

// Address hints never replace receiver-side leases or transaction fences.
// Delivery/checkpoint/prune/rebuild are sent once: a response can follow a commit.
// Maintenance retains Worker before/after ownership checks and commit fences;
// only address discovery uses this cache.
func (p *remotePartition) cachedRoutedCall(ctx context.Context, op string, q Search, delivery *Delivery) (partitionReply, error) {
	if !cachedRouteOperation(op) && op != "follower_search" && op != "deliver" {
		return partitionReply{}, errors.New("cached routing is limited to reads and fenced admission")
	}
	if op == "search" || op == "follower_search" || op == "sync" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	var through uint64
	if op == "search" && p.cluster.hotReplicas() && !p.cluster.graphReplication {
		var err error
		through, err = p.cluster.partitionHead(ctx, p.id)
		if err != nil {
			return partitionReply{}, err
		}
	}
	rounds := 1
	if op == "search" || op == "follower_search" || op == "sync" {
		rounds = 2
		if p.cluster.graphReplication {
			rounds = 10
		}
	}
	var last error = ErrPartitionUnavailable
	for round := 0; round < rounds; round++ {
		if round > 0 && p.cluster.graphReplication {
			timer := time.NewTimer(time.Duration(min(round, 5)) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, ctx.Err())
			case <-timer.C:
			}
		}
		state, err := p.cluster.cachedSearchRoutes(ctx, false)
		if err != nil {
			return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, err)
		}
		publisher := state.ReplicaPlans[p.id].Publisher
		if p.cluster.replicas == 1 {
			publisher = p.id
		}
		owners := []Ownership{}
		for _, o := range state.ReplicaOwners[p.id].Owners {
			if op == "follower_search" {
				if publisher != "" && o.Partition != publisher {
					owners = append(owners, o)
				}
			} else if op == "search" || o.Partition == publisher {
				owners = append(owners, o)
			}
		}
		if len(owners) == 0 {
			p.cluster.expireSearchRoutes(state)
			continue
		}
		start := int(p.rotation.Add(1) % uint64(len(owners)))
		for i := range owners {
			o := owners[(start+i)%len(owners)]
			attempt, done := context.WithCancel(ctx)
			if op == "search" {
				done()
				attempt, done = context.WithTimeout(ctx, 2*time.Second)
			}
			payload := partitionRPC{Namespace: p.cluster.root.Prefix, Partition: o.Partition, Epoch: o.Epoch, Session: o.Session, Operation: op, Query: q, ReadThrough: through, Delivery: delivery}
			timer := p.cluster.observations.pipeline.track("replica_rpc_"+op, string(p.id))
			reply, callErr := p.cluster.sendPartition(attempt, o, payload)
			timer(&callErr)
			done()
			if callErr == nil {
				return reply, nil
			}
			var epochErr *SourceEpochError
			if errors.As(callErr, &epochErr) {
				return reply, callErr
			}
			last = callErr
			if op == "follower_search" && (errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, ErrFollowerQueueFull)) {
				return reply, callErr
			}
			if ctx.Err() != nil {
				return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, ctx.Err())
			}
		}
		p.cluster.expireSearchRoutes(state)
	}
	return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, last)
}

func (c *ClusterService) expireSearchRoutes(failed *ClusterStatus) {
	c.searchRoutes.mu.Lock()
	defer c.searchRoutes.mu.Unlock()
	if c.searchRoutes.state == failed {
		c.searchRoutes.deadline = time.Time{}
	}
}
