package vector

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
)

const deliveryProofHeader = "X-Vecdb-Delivery-Proof"

func setDeliveryHeader(r *http.Request, q *Delivery) {
	if q != nil && len(q.proof) > 0 {
		r.Header.Set(deliveryProofHeader, hex.EncodeToString(q.proof))
	}
}

// The signature binds a complete projected WAL range to its source incarnation
// and destination. Headers let an older receiver verify by replaying its WAL.
func (c *ClusterService) deliveryMAC(id IndexPartitionID, q Delivery, events []LogEvent) ([]byte, error) {
	mac := hmac.New(sha256.New, []byte(c.token))
	enc := json.NewEncoder(mac)
	if err := enc.Encode([]any{"vecdb-delivery-v1", c.root.Prefix, id, q.Source, q.Owner.Partition, q.Owner.Node, q.Owner.Session, q.Owner.Epoch, q.RouteVersion, q.Previous, q.Through}); err != nil {
		return nil, err
	}
	for _, event := range events {
		if err := enc.Encode(event); err != nil {
			return nil, err
		}
	}
	return mac.Sum(nil), nil
}

func (c *ClusterService) verifyDelivery(id IndexPartitionID, q Delivery) bool {
	if c.token == "" || len(q.Events) > dispatchBatchSize {
		return false
	}
	sum, err := c.deliveryMAC(id, q, q.Events)
	return err == nil && hmac.Equal(sum, q.proof)
}

func (c *ClusterService) signRawDelivery(id IndexPartitionID, q Delivery, b *mutationGroup, completed uint64) ([]byte, error) {
	snap, ok := b.confirmedSnapshot()
	if !ok || !sameOwner(b.lease.owner, q.Owner) {
		return nil, ErrFenced
	}
	if q.Through > snap.end || q.Previous >= q.Through || q.Through-q.Previous > dispatchBatchSize {
		return nil, errors.New("invalid WAL proof range")
	}
	if q.Previous > completed && q.Previous > b.proofs.read(id).through {
		return nil, errors.New("uncertified source coverage boundary")
	}
	// Project from the confirmed history independently of the dispatch queue.
	// Omitting a routed update or a broadcast delete invalidates the proof.
	var events []LogEvent
	for n := q.Previous; n < q.Through; n++ {
		entry, ok := snap.entry(n + 1)
		if !ok {
			return nil, errors.New("missing WAL entry for delivery proof")
		}
		if entry.Record.Deleted || c.layout.Routing.target(entry.Record.Vector) == id {
			events = append(events, LogEvent{Sequence: n + 1, Record: entry.Record})
		}
	}
	return c.deliveryMAC(id, q, events)
}
