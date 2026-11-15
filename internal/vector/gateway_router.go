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
	cluster  *ClusterService
	id       IndexPartitionID
}

func (p *remotePartition) call(ctx context.Context, op string, q Search) (partitionReply, error) {
	c := p.cluster
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
		if out.Code == "not_visible" {
			return partitionReply{}, ErrNotVisible
		}
		return partitionReply{}, fmt.Errorf("%w: %s", ErrPartitionUnavailable, out.Error)
	}
	if op == "search" && out.AppliedOffset < through {
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
	// Refresh once if membership changed while an earlier route was in flight.
	for round := 0; round < 2; round++ {
		plan, e := p.cluster.coordinator.Plan(ctx, p.id)
		if e != nil {
			return partitionReply{}, fmt.Errorf("%w: %v", ErrPartitionUnavailable, e)
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
				o, err := p.cluster.coordinator.Resolve(ctx, a.Partition)
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
				result, err := p.callOwnerThrough(attempt, op, q, o, through)
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
