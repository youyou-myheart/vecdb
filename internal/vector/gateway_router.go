package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

type remotePartition struct {
	rotation atomic.Uint64
	plans    routePlanCache
	cluster  *ClusterService
	id       IndexPartitionID
}

func (p *remotePartition) call(ctx context.Context, op string, q Search) (partitionReply, error) {
	c := p.cluster
	if c.routeHintsEnabled() && cachedRouteOperation(op) {
		return p.cachedRoutedCall(ctx, op, q, nil)
	}
	if c.replicas > 1 {
		return p.replicaCall(ctx, op, q)
	}
	o, e := c.coordinator.Resolve(ctx, p.id)
	if e != nil {
		return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, e)
	}
	return p.callOwner(ctx, op, q, o)
}
func (p *remotePartition) callOwner(ctx context.Context, op string, q Search, o Ownership) (partitionReply, error) {
	return p.callOwnerThrough(ctx, op, q, o, 0)
}
func (p *remotePartition) callOwnerThrough(ctx context.Context, op string, q Search, o Ownership, through uint64) (partitionReply, error) {
	c := p.cluster
	payload := partitionRPC{ReadThrough: through, Namespace: c.root.Prefix, Partition: o.Partition, Epoch: o.Epoch, Operation: op, Query: q}
	payload.Session = o.Session
	return c.sendPartition(ctx, o, payload)
}
func (c *ClusterService) sendPartition(ctx context.Context, o Ownership, payload partitionRPC) (partitionReply, error) {
	if err := ctx.Err(); err != nil {
		return partitionReply{}, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		payload.TimeoutMS = max(1, time.Until(deadline).Milliseconds())
	}
	if o.Node == c.node.ID && o.Session == c.node.Session {
		return c.execute(ctx, payload)
	}
	b, e := json.Marshal(payload)
	if e != nil {
		return partitionReply{}, e
	}
	req, e := http.NewRequestWithContext(ctx, "POST", o.URL+"/internal/partition", bytes.NewReader(b))
	if e != nil {
		return partitionReply{}, e
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	setDeliveryHeader(req, payload.Delivery)
	response, e := c.client.Do(req)
	if e != nil {
		return partitionReply{}, e
	}
	defer response.Body.Close()
	var out partitionReply
	if e = json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&out); e != nil {
		return out, e
	}
	if response.StatusCode != 200 {
		switch out.Code {
		case "conflict":
			return partitionReply{}, ErrConflict
		case "deadline_exceeded":
			return partitionReply{}, context.DeadlineExceeded
		case "canceled":
			return partitionReply{}, context.Canceled
		case "resource_exhausted":
			return partitionReply{}, ErrFollowerQueueFull
		}
		if out.Code == "fenced" {
			if out.SourceEpoch != nil {
				return out, out.SourceEpoch
			}
			return partitionReply{}, ErrFenced
		}
		if out.Code == "not_visible" {
			return partitionReply{}, ErrNotVisible
		}
		return partitionReply{}, fmt.Errorf("%w: %s", ErrPartitionUnavailable, out.Error)
	}
	if payload.Operation == "search" && out.AppliedOffset < payload.ReadThrough {
		return partitionReply{}, ErrNotVisible
	}
	return out, nil
}
func (p *remotePartition) Search(q Search) ([]Hit, error) {
	return p.SearchContext(context.Background(), q)
}
func (p *remotePartition) SearchContext(ctx context.Context, q Search) ([]Hit, error) {
	r, e := p.call(ctx, "search", q)
	return r.Hits, e
}
func (p *remotePartition) Sync(ctx context.Context) error {
	_, e := p.call(ctx, "sync", Search{})
	return e
}
func (p *remotePartition) Checkpoint(ctx context.Context) (Manifest, error) {
	r, e := p.call(ctx, "checkpoint", Search{})
	return r.Manifest, e
}
func (p *remotePartition) Prune(ctx context.Context) (uint64, error) {
	r, e := p.call(ctx, "prune", Search{})
	return r.Floor, e
}
func (p *remotePartition) Rebuild(ctx context.Context) error {
	_, e := p.call(ctx, "rebuild", Search{})
	return e
}
func (p *remotePartition) MaybeRebuild(ctx context.Context) error { return nil }

func (p *remotePartition) replicaCall(ctx context.Context, op string, q Search) (partitionReply, error) {
	if p.cluster.routeHintsEnabled() && cachedRouteOperation(op) {
		return p.cachedRoutedCall(ctx, op, q, nil)
	}
	if p.cluster.graphReplication && (op == "search" || op == "sync") {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	last := error(ErrPartitionUnavailable)
	hot := p.cluster.hotReplicas()
	var through uint64
	if hot && op == "search" && !p.cluster.graphReplication {
		var e error
		through, e = p.cluster.partitionHead(ctx, p.id)
		if e != nil {
			return partitionReply{}, e
		}
	}
	// Graph promotion and retirement can span more than two metadata snapshots.
	// Retry only idempotent reads/synchronization, within a bounded routing window.
	rounds := 2
	if p.cluster.graphReplication && (op == "search" || op == "sync") {
		rounds = 10
	}
	retryUntil := time.Now().Add(5 * time.Second)
	for round := 0; round < rounds; round++ {
		if round > 0 && p.cluster.graphReplication {
			if time.Now().After(retryUntil) {
				break
			}
			timer := time.NewTimer(time.Duration(min(round, 5)) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return partitionReply{}, ctx.Err()
			case <-timer.C:
			}
		}
		plan, e := p.routingPlan(ctx, op)
		if e != nil {
			return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, e)
		}
		start := 0
		if op == "search" && len(plan.Serving) > 1 {
			start = int(p.rotation.Add(1) % uint64(len(plan.Serving)))
		}
		passes := 1
		read := op == "search" || op == "sync"
		if hot && read {
			passes = 2
		}
		tried := map[IndexPartitionID]bool{}
		for pass := 0; pass < passes; pass++ {
			for i := range plan.Serving {
				a := plan.Serving[(start+i)%len(plan.Serving)]
				if (op != "search" && op != "sync" || op == "sync" && p.cluster.graphReplication) && a.Partition != plan.Publisher {
					continue
				}
				ownerTimer := p.cluster.observations.pipeline.track("route_owner", string(p.id))
				o, err := p.cluster.coordinator.Resolve(ctx, a.Partition)
				ownerTimer(&err)
				if err != nil {
					last = err
					continue
				}
				if hot && read {
					if tried[a.Partition] {
						continue
					}
					cooling := p.cluster.routeCooling(o)
					if pass == 0 && cooling || pass == 1 && !cooling {
						continue
					}
					tried[a.Partition] = true
				}
				budget := 2 * time.Second
				if op != "search" && op != "sync" {
					budget = 30 * time.Second
				}
				attempt, cancel := context.WithTimeout(ctx, budget)
				rpcTimer := p.cluster.observations.pipeline.track("replica_rpc_"+op, string(p.id))
				result, err := p.callOwnerThrough(attempt, op, q, o, through)
				rpcTimer(&err)
				cancel()
				if err == nil {
					if hot && read {
						p.cluster.markRoute(o, false)
					}
					return result, nil
				}
				last = err
				if hot && read && ctx.Err() == nil && !errors.Is(err, ErrNotVisible) {
					p.cluster.markRoute(o, true)
				}
				if ctx.Err() != nil {
					return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, ctx.Err())
				}
			}
		}
		// Maintenance can have committed despite a transport timeout: never retry it.
		if op != "search" && op != "sync" {
			break
		}
	}
	return partitionReply{}, fmt.Errorf("%w: %w", ErrPartitionUnavailable, last)
}
