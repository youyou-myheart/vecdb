package vector

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

type partitionRPC struct {
	Acks        []graphCommitAck `json:"graph_acks,omitempty"`
	ID          string           `json:"id,omitempty"`
	Mutation    *Mutation        `json:"mutation,omitempty"`
	Deleted     bool             `json:"deleted,omitempty"`
	Session     string           `json:"session,omitempty"`
	TimeoutMS   int64            `json:"timeout_ms,omitempty"`
	Source      *DataPartitionID `json:"source,omitempty"`
	Delivery    *Delivery        `json:"delivery,omitempty"`
	ReadThrough uint64           `json:"read_through,omitempty"`
	Namespace   string           `json:"namespace"`
	Partition   IndexPartitionID `json:"partition"`
	Epoch       uint64           `json:"epoch"`
	Operation   string           `json:"operation"`
	Query       Search           `json:"query"`
}
type partitionReply struct {
	DataProgress  *DataProgress     `json:"data_progress,omitempty"`
	Record        *Record           `json:"record,omitempty"`
	FollowerRead  *FollowerReadInfo `json:"follower_read,omitempty"`
	SourceEpoch   *SourceEpochError `json:"source_epoch,omitempty"`
	GraphPosition *GraphPosition    `json:"graph_position,omitempty"`
	AppliedOffset uint64            `json:"applied_offset,omitempty"`
	Hits          []Hit             `json:"hits,omitempty"`
	Manifest      Manifest          `json:"manifest,omitempty"`
	Floor         uint64            `json:"floor,omitempty"`
	Error         string            `json:"error,omitempty"`
	Code          string            `json:"code,omitempty"`
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
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
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
		if proof := r.Header.Get(deliveryProofHeader); proof != "" && req.Delivery != nil {
			var err error
			req.Delivery.proof, err = hex.DecodeString(proof)
			if err != nil || len(req.Delivery.proof) != 32 {
				w.WriteHeader(400)
				return
			}
		}
		budget := 30 * time.Second
		if req.TimeoutMS > 0 && req.TimeoutMS < budget.Milliseconds() {
			budget = time.Duration(req.TimeoutMS) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(r.Context(), budget)
		defer cancel()
		// Opt-in diagnostics for acceptance probes. Never log the authorization
		// header, query vectors, or payload. A fixed-size hex ID is log-safe.
		probeID := r.Header.Get("X-Vecdb-Probe-ID")
		if len(probeID) != 32 {
			probeID = ""
		} else if _, err := hex.DecodeString(probeID); err != nil {
			probeID = ""
		}
		if probeID != "" {
			w.Header().Set("X-Vecdb-Probe-ID", probeID)
		}
		response, e := c.execute(ctx, req)
		if e != nil {
			response.Error = e.Error()
			response.Code = "unavailable"
			if errors.Is(e, context.DeadlineExceeded) {
				response.Code = "deadline_exceeded"
			}
			if errors.Is(e, context.Canceled) {
				response.Code = "canceled"
			}
			if errors.Is(e, ErrFollowerQueueFull) {
				response.Code = "resource_exhausted"
			}
			if req.Operation == "mutate" && errors.Is(e, ErrConflict) {
				response.Code = "conflict"
			}
			if errors.Is(e, ErrNotVisible) {
				response.Code = "not_visible"
			}
			if errors.Is(e, ErrFenced) {
				response.Code = "fenced"
				errors.As(e, &response.SourceEpoch)
			}
			if req.Operation == "search" && probeID != "" {
				partition := string(req.Partition)
				if len(partition) > 128 {
					partition = partition[:128] + "..."
				}
				log.Printf("partition_rpc_probe request_id=%s partition=%q epoch=%d status=503 code=%s error=%q", probeID, partition, req.Epoch, response.Code, response.Error)
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
	if req.Operation == "data_progress" {
		if req.Source == nil || req.Partition != sourceID(*req.Source) {
			return reply, ErrFenced
		}
		group, err := c.localDataWriter(*req.Source, req.Epoch, req.Session)
		if err != nil {
			return reply, err
		}
		progress, err := group.dataProgress()
		if err != nil {
			return reply, err
		}
		reply.DataProgress = &progress
		return reply, nil
	}
	if req.Operation == "get_owned" {
		if req.Source == nil || *req.Source != c.router.DataForID(req.ID) || req.Partition != sourceID(*req.Source) {
			return reply, ErrFenced
		}
		group, err := c.localDataWriter(*req.Source, req.Epoch, req.Session)
		if err != nil {
			return reply, err
		}
		record, err := group.readOwnedRecord(req.ID)
		if errors.Is(err, ErrNotFound) {
			return reply, nil
		}
		if err != nil {
			return reply, err
		}
		reply.Record = &record
		return reply, nil
	}
	if req.Operation == "mutate" {
		if req.Source == nil || req.Partition != sourceID(*req.Source) || req.Mutation == nil {
			return reply, ErrFenced
		}
		record, err := c.MutateOwned(ctx, req.Namespace, *req.Source, req.Epoch, req.Session, req.ID, *req.Mutation, req.Deleted)
		if err == nil {
			reply.Record = &record
		}
		return reply, err
	}
	if req.Operation == "prune_data" {
		if req.Source == nil || req.Partition != sourceID(*req.Source) {
			return reply, ErrFenced
		}
		group, err := c.localDataWriter(*req.Source, req.Epoch, req.Session)
		if err != nil {
			return reply, err
		}
		reply.Floor, err = c.pruneData(ctx, *req.Source, group.lease.owner)
		if err == nil {
			for old := group.lease.floor.Load(); old < reply.Floor; old = group.lease.floor.Load() {
				if group.lease.floor.CompareAndSwap(old, reply.Floor) {
					break
				}
			}
		}
		var after uint64
		if err == nil && (!group.lease.InLeaderLease(&after) || req.Epoch != after) {
			return partitionReply{}, ErrFenced
		}
		return reply, err
	}
	if req.Operation == "wake_source" {
		if !c.EventDrivenIndexing() || c.worker == nil || req.Source == nil || *req.Source < 0 || int(*req.Source) >= c.layout.DataPartitions || req.Partition != sourceID(*req.Source) {
			return reply, ErrFenced
		}
		if len(req.Acks) > len(c.layout.Bindings) {
			return reply, ErrFenced
		}
		c.receiveGraphAcks(*req.Source, req.Acks)
		c.signalSource(*req.Source, "received", true)
		return reply, nil
	}
	c.mu.Lock()
	entry := c.locals[req.Partition]
	c.mu.Unlock()
	if entry == nil {
		return reply, ErrFenced
	}
	if req.Operation == "applied_position" {
		if req.Session == "" || req.Session != entry.owner.Session {
			return reply, ErrFenced
		}
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		if entry.service == nil || entry.service.graphReplica == nil {
			return reply, ErrPartitionUnavailable
		}
		pos, err := entry.service.graphReplica.leaderApplied()
		return partitionReply{GraphPosition: &pos}, err
	}
	if entry.owner.Epoch != req.Epoch {
		return reply, ErrFenced
	}
	localOwner := c.localOwnerLeases || req.Operation == "search" && c.localReadLeases()
	if localOwner {
		if e := entry.checkReadLease(req); e != nil {
			return reply, e
		}
	} else if req.Operation != "follower_search" && !(c.coordinator.leaseReads && req.Operation == "deliver") {
		if e := c.coordinator.Check(ctx, entry.owner); e != nil {
			return reply, e
		}
	}
	if req.Operation == "follower_search" {
		reply, err := c.executeFollowerRead(ctx, entry, req.Query)
		if err == nil && localOwner {
			err = entry.checkReadLease(req)
		}
		if err != nil {
			return partitionReply{}, err
		}
		return reply, nil
	}
	if req.Operation == "search" && req.Query.MinVisible != nil && c.graphReplication && c.EventDrivenIndexing() {
		reply, err := c.searchVisibleReplica(ctx, entry, req.Query)
		if err != nil {
			return partitionReply{}, err
		}
		if localOwner {
			err = entry.checkReadLease(req)
		} else {
			err = c.coordinator.Check(ctx, entry.owner)
		}
		return reply, err
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if entry.service == nil {
		return reply, ErrPartitionUnavailable
	}
	s := entry.service
	var e error
	switch req.Operation {
	case "deliver":
		if req.Delivery == nil || s.graphReplica == nil || s.graphReplica.input == nil {
			return reply, ErrFenced
		}
		q := *req.Delivery
		if len(q.proof) != 0 {
			if !c.verifyDelivery(s.graphReplica.journal.id, q) {
				return reply, ErrFenced
			}
			q.verified = true
		}
		e = s.graphReplica.enqueue(ctx, q)
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
	if c.coordinator.leaseReads && req.Operation == "deliver" {
		if s.graphReplica.writer == nil {
			return partitionReply{}, ErrFenced
		}
		_, _, e = s.graphReplica.writer.snapshot()
		if e != nil {
			return partitionReply{}, e
		}
	}
	if localOwner {
		e = entry.checkReadLease(req)
	} else if !(c.coordinator.leaseReads && req.Operation == "deliver") {
		e = c.coordinator.Check(ctx, entry.owner)
	}
	if e != nil {
		return partitionReply{}, e
	}
	return reply, nil
}
