package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
	"strings"
)

// Session is caller-generated and must change after expiry/release. Epoch fences
// delayed ACKs from an older process using the same logical consumer name.
type ConsumerRequest struct {
	Partition  IndexPartitionID `json:"partition"`
	Stream     string           `json:"stream"`
	Consumer   string           `json:"consumer"`
	Session    string           `json:"session"`
	Epoch      uint64           `json:"epoch"`
	Ack        uint64           `json:"ack"`
	TTLSeconds int              `json:"ttl_seconds"`
	Action     string           `json:"action"`
}
type ConsumerLease struct {
	Consumer string `json:"consumer"`
	Session  string `json:"session"`
	Epoch    uint64 `json:"epoch"`
	Ack      uint64 `json:"ack"`
	Expires  int64  `json:"expires_ms"`
}
type ConsumerState struct {
	Leases map[string]ConsumerLease `json:"leases"`
	Floor  uint64                   `json:"floor"`
	Head   uint64                   `json:"head"`
}

func (r *Repository) consumerKey(stream string) []byte {
	return []byte(r.Namespace() + "consumers/" + stream)
}
func (r *Repository) consumersIn(ctx context.Context, tx transactionAPI, stream string) (map[string]ConsumerLease, error) {
	leases := map[string]ConsumerLease{}
	b, e := tx.Get(ctx, r.consumerKey(stream))
	if tikverr.IsErrNotFound(e) {
		return leases, nil
	}
	if e != nil {
		return nil, e
	}
	e = json.Unmarshal(b, &leases)
	if leases == nil {
		leases = map[string]ConsumerLease{}
	}
	return leases, e
}
func (r *Repository) retainConsumers(ctx context.Context, tx transactionAPI, stream string, end uint64) (uint64, error) {
	leases, e := r.consumersIn(ctx, tx, stream)
	if e != nil {
		return 0, e
	}
	for _, l := range leases {
		if l.Expires > txnMillis(tx) && l.Ack < end {
			end = l.Ack
		}
	}
	// Serialize acquire/renew/ACK with compaction, including the empty registry.
	return end, putJSON(tx, r.consumerKey(stream), leases)
}
func validateConsumer(q *ConsumerRequest) error {
	if q.Stream != "data" && q.Stream != "graph" {
		return ErrInvalidSubscription
	}
	if q.Action == "list" {
		return nil
	}
	if q.Action != "acquire" && q.Action != "ack" && q.Action != "release" {
		return ErrInvalidSubscription
	}
	if len(q.Consumer) == 0 || len(q.Consumer) > 128 || len(q.Session) < 16 || len(q.Session) > 128 || strings.ContainsAny(q.Consumer, "/\x00") {
		return fmt.Errorf("%w: consumer 1..128 bytes, session 16..128 bytes required", ErrInvalidSubscription)
	}
	if q.TTLSeconds == 0 {
		q.TTLSeconds = 60
	}
	if q.TTLSeconds < 10 || q.TTLSeconds > 3600 {
		return fmt.Errorf("%w: TTL must be 10..3600 seconds", ErrInvalidSubscription)
	}
	return nil
}
func (p *PartitionedService) ManageConsumer(ctx context.Context, q ConsumerRequest) (ConsumerState, error) {
	return ConsumerState{}, ErrSubscriptionUnavailable
}
func (c *ClusterService) ManageConsumer(ctx context.Context, q ConsumerRequest) (ConsumerState, error) {
	if !c.consumerRetention {
		return ConsumerState{}, ErrSubscriptionUnavailable
	}
	var j *graphJournal
	if q.Stream == "graph" {
		if !c.graphReplication {
			return ConsumerState{}, ErrSubscriptionUnavailable
		}
		j = &graphJournal{c: c.coordinator, id: q.Partition}
	}
	return c.PartitionedService.manageConsumer(ctx, q, j)
}
func (p *PartitionedService) manageConsumer(ctx context.Context, q ConsumerRequest, j *graphJournal) (ConsumerState, error) {
	var out ConsumerState
	if e := validateConsumer(&q); e != nil {
		return out, e
	}
	r, e := p.changeSource(ChangeRequest{Partition: q.Partition, Stream: q.Stream})
	if e != nil {
		return out, e
	}
	if q.Stream == "graph" && j == nil {
		return out, ErrSubscriptionUnavailable
	}
	tx, e := r.begin()
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	if j != nil {
		h, err := j.headIn(ctx, tx)
		if err != nil {
			return out, err
		}
		out.Floor = h.Floor.Sequence
		out.Head = h.Visible.Sequence
	} else {
		out.Floor, e = r.readCounter(ctx, tx, "log-floor")
		if e != nil {
			return out, e
		}
		out.Head, e = r.readCounter(ctx, tx, "log-head")
		if e != nil {
			return out, e
		}
	}
	out.Leases, e = r.consumersIn(ctx, tx, q.Stream)
	if e != nil {
		return out, e
	}
	if q.Action == "list" {
		return out, nil
	}
	old, exists := out.Leases[q.Consumer]
	now := txnMillis(tx)
	switch q.Action {
	case "acquire":
		if old.Expires > now {
			if old.Session != q.Session {
				return out, ErrConflict
			}
			return out, nil
		}
		if exists && old.Session == q.Session {
			return out, ErrFenced
		}
		if !exists && len(out.Leases) >= 128 {
			return out, errors.New("consumer registry full; reuse a logical consumer name")
		}
		if old.Epoch == ^uint64(0) {
			return out, errors.New("consumer epoch exhausted")
		}
		if q.Ack < out.Floor {
			return out, ErrLogCompacted
		}
		if q.Ack > out.Head {
			return out, ErrInvalidSubscription
		}
		old = ConsumerLease{Consumer: q.Consumer, Session: q.Session, Epoch: old.Epoch + 1, Ack: q.Ack, Expires: now + int64(q.TTLSeconds)*1000}
	case "ack", "release":
		if !exists || old.Session != q.Session || old.Epoch != q.Epoch || old.Expires <= now {
			return out, ErrFenced
		}
		if q.Action == "release" {
			old.Expires = 0
		} else {
			if q.Ack < old.Ack || q.Ack > out.Head {
				return out, ErrInvalidSubscription
			}
			old.Ack = q.Ack
			old.Expires = now + int64(q.TTLSeconds)*1000
		}
	}
	out.Leases[q.Consumer] = old
	if e = putJSON(tx, r.consumerKey(q.Stream), out.Leases); e != nil {
		return out, e
	}
	return out, tx.Commit(ctx)
}
