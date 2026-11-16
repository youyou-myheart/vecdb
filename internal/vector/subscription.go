package vector

import (
	"context"
	"errors"
	"fmt"
)

var ErrInvalidSubscription = errors.New("invalid change subscription")
var ErrSubscriptionUnavailable = errors.New("change stream unavailable")

// Cursors are exclusive sequence numbers within ONE namespace/partition/stream.
// A consumer persists Next only after durably applying every event in the page.
type ChangeRequest struct {
	Partition IndexPartitionID `json:"partition"`
	Stream    string           `json:"stream"`
	After     uint64           `json:"after"`
	Limit     int              `json:"limit"`
}
type ChangePage struct {
	Partition          IndexPartitionID `json:"partition"`
	Stream             string           `json:"stream"`
	Next               uint64           `json:"next"`
	Head               uint64           `json:"head"`
	Floor              uint64           `json:"floor"`
	ResetRequired      bool             `json:"reset_required"`
	Data               []LogEvent       `json:"data"`
	Graph              []GraphEntry     `json:"graph"`
	CheckpointPosition *GraphPosition   `json:"checkpoint_position,omitempty"`
	Checkpoint         *Manifest        `json:"checkpoint,omitempty"`
}

func validateChangeRequest(q *ChangeRequest) error {
	if q.Limit == 0 {
		q.Limit = 100
	}
	if q.Limit < 1 || q.Limit > LogBatchSize || (q.Stream != "data" && q.Stream != "graph") {
		return ErrInvalidSubscription
	}
	return nil
}
func (p *PartitionedService) changeSource(q ChangeRequest) (*Repository, error) {
	if p.layout.Routing != nil {
		return p.clusteredSource(q)
	}
	for _, b := range p.layout.Bindings {
		if b.Index == q.Partition {
			repo := *p.data[b.Data]
			if p.layout.DataPartitions > 1 {
				repo.IndexPrefix = repo.ReceiptPrefix + "index/" + string(b.Index) + "/"
			}
			return &repo, nil
		}
	}
	return nil, fmt.Errorf("%w: unknown partition", ErrInvalidSubscription)
}
func (p *PartitionedService) ReadChanges(ctx context.Context, q ChangeRequest) (ChangePage, error) {
	out := ChangePage{Partition: q.Partition, Stream: q.Stream, Next: q.After, Data: []LogEvent{}, Graph: []GraphEntry{}}
	if e := validateChangeRequest(&q); e != nil {
		return out, e
	}
	repo, e := p.changeSource(q)
	if e != nil {
		return out, e
	}
	if q.Stream != "data" {
		return out, ErrSubscriptionUnavailable
	}
	batch, e := repo.ReadEvents(ctx, q.After, 0, q.Limit)
	out.Head = batch.Head
	out.Floor = batch.Floor
	if errors.Is(e, ErrLogCompacted) {
		out.ResetRequired = true
		out.Checkpoint, e = repo.Manifest(ctx)
		return out, e
	}
	if errors.Is(e, ErrInvalidLogCursor) {
		return out, fmt.Errorf("%w: after exceeds head", ErrInvalidSubscription)
	}
	if e != nil {
		return out, e
	}
	out.Data = batch.Events
	if len(batch.Events) > 0 {
		out.Next = batch.Events[len(batch.Events)-1].Sequence
	}
	return out, nil
}
func (c *ClusterService) ReadChanges(ctx context.Context, q ChangeRequest) (ChangePage, error) {
	if q.Stream == "data" {
		return c.PartitionedService.ReadChanges(ctx, q)
	}
	out := ChangePage{Partition: q.Partition, Stream: q.Stream, Next: q.After, Data: []LogEvent{}, Graph: []GraphEntry{}}
	if e := validateChangeRequest(&q); e != nil {
		return out, e
	}
	if _, e := c.changeSource(q); e != nil {
		return out, e
	}
	if !c.graphReplication {
		return out, ErrSubscriptionUnavailable
	}
	j := graphJournal{c: c.coordinator, id: q.Partition}
	h, e := j.Head(ctx)
	if e != nil {
		return out, e
	}
	// Capture one published head. A committed but unpublished entry is invisible.
	out.Head = h.Visible.Sequence
	out.Floor = h.Floor.Sequence
	out.CheckpointPosition = &h.CheckpointPosition
	out.Checkpoint = h.Checkpoint
	if q.After < out.Floor {
		out.ResetRequired = true
		return out, nil
	}
	if q.After > out.Head {
		return out, fmt.Errorf("%w: after exceeds published head", ErrInvalidSubscription)
	}
	for n := q.After; n < out.Head && len(out.Graph) < q.Limit; {
		n++
		entry, e := j.Entry(ctx, n)
		if errors.Is(e, ErrLogCompacted) {
			fresh, err := j.Head(ctx)
			if err != nil {
				return out, err
			}
			out.Graph = []GraphEntry{}
			out.Next = q.After
			out.ResetRequired = true
			out.Floor = fresh.Floor.Sequence
			out.Head = fresh.Visible.Sequence
			out.Checkpoint = fresh.Checkpoint
			out.CheckpointPosition = &fresh.CheckpointPosition
			return out, nil
		}
		if e != nil {
			return out, e
		}
		if len(out.Graph) > 0 && entry.Previous != out.Graph[len(out.Graph)-1].Position {
			return out, errors.New("graph change stream discontinuity")
		}
		out.Graph = append(out.Graph, entry)
		out.Next = n
	}
	return out, nil
}
