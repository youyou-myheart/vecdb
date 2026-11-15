package vector

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

type partitionRPC struct {
	ReadThrough uint64           `json:"read_through,omitempty"`
	Namespace   string           `json:"namespace"`
	Partition   IndexPartitionID `json:"partition"`
	Epoch       uint64           `json:"epoch"`
	Operation   string           `json:"operation"`
	Query       Search           `json:"query"`
}
type partitionReply struct {
	GraphPosition *GraphPosition `json:"graph_position,omitempty"`
	AppliedOffset uint64         `json:"applied_offset,omitempty"`
	Hits          []Hit          `json:"hits,omitempty"`
	Manifest      Manifest       `json:"manifest,omitempty"`
	Floor         uint64         `json:"floor,omitempty"`
	Error         string         `json:"error,omitempty"`
	Code          string         `json:"code,omitempty"`
}

func (c *ClusterService) InternalHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+c.token)) != 1 {
			w.WriteHeader(401)
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		var req partitionRPC
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if e := decoder.Decode(&req); e != nil {
			w.WriteHeader(400)
			return
		}
		var extra any
		if e := decoder.Decode(&extra); e != io.EOF {
			w.WriteHeader(400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		response, e := c.execute(ctx, req)
		if e != nil {
			response.Error = e.Error()
			response.Code = "unavailable"
			if errors.Is(e, ErrNotVisible) {
				response.Code = "not_visible"
			}
			if errors.Is(e, ErrFenced) {
				response.Code = "fenced"
			}
			w.WriteHeader(503)
		}
		_ = json.NewEncoder(w).Encode(response)
	})
}
func (c *ClusterService) execute(ctx context.Context, req partitionRPC) (partitionReply, error) {
	var reply partitionReply
	if req.Namespace != c.root.Prefix {
		return reply, ErrFenced
	}
	c.mu.Lock()
	entry := c.locals[req.Partition]
	c.mu.Unlock()
	if entry == nil || entry.owner.Epoch != req.Epoch {
		return reply, ErrFenced
	}
	if e := c.coordinator.Check(ctx, entry.owner); e != nil {
		return reply, e
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if entry.service == nil {
		return reply, ErrPartitionUnavailable
	}
	s := entry.service
	var e error
	switch req.Operation {
	case "search":
		started := time.Now()
		defer func() { entry.queryNanos.Add(time.Since(started).Nanoseconds()) }()
		if s.graphReplica != nil {
			var pos GraphPosition
			reply.Hits, pos, e = s.searchGraphLocal(ctx, req.Query)
			reply.GraphPosition = &pos
			reply.AppliedOffset = pos.DataOffset
			break
		}
		if (c.replicas > 1 && req.Query.MinVisible != nil) || s.Cursor() < req.ReadThrough {
			if e = s.Sync(ctx); e != nil {
				return reply, e
			}
		}
		reply.AppliedOffset = s.Cursor()
		if reply.AppliedOffset < req.ReadThrough {
			return partitionReply{}, ErrNotVisible
		}
		reply.Hits, e = s.Search(req.Query)
	case "sync":
		if s.graphReplica != nil {
			e = s.syncGraphThrough(ctx)
		} else {
			e = s.Sync(ctx)
		}
	case "checkpoint":
		reply.Manifest, e = s.Checkpoint(ctx)
	case "prune":
		reply.Floor, e = s.Prune(ctx)
	case "rebuild":
		e = s.Rebuild(ctx)
	default:
		e = errors.New("unknown partition operation")
	}
	if e != nil {
		return partitionReply{}, e
	}
	if e = c.coordinator.Check(ctx, entry.owner); e != nil {
		return partitionReply{}, e
	}
	return reply, nil
}
