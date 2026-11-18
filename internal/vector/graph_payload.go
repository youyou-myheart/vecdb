package vector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
)

const graphPayloadChunk = 256 << 10
const maxGraphPayload = 32 << 20

// Payload chunks and the immutable entry are committed in ONE TiKV transaction.
// Keep the entry small: head reads should never fetch a large graph patch.
type GraphPayload struct {
	Size   int    `json:"size"`
	Chunks int    `json:"chunks"`
	SHA256 string `json:"sha256"`
}

func (j graphJournal) payloadKey(sequence uint64, chunk int) []byte {
	return j.key(fmt.Sprintf("payload/%020d/%04d", sequence, chunk))
}
func (j graphJournal) writePayload(tx transactionAPI, sequence uint64, data []byte) (*GraphPayload, error) {
	if len(data) == 0 || len(data) > maxGraphPayload {
		return nil, errors.New("invalid graph payload size")
	}
	p := &GraphPayload{Size: len(data), Chunks: (len(data) + graphPayloadChunk - 1) / graphPayloadChunk, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	for n := 0; n < p.Chunks; n++ {
		if err := tx.Set(j.payloadKey(sequence, n), data[n*graphPayloadChunk:min(len(data), (n+1)*graphPayloadChunk)]); err != nil {
			return nil, err
		}
	}
	return p, nil
}
func (j graphJournal) loadPayload(ctx context.Context, e GraphEntry) ([]byte, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return j.loadPayloadIn(ctx, tx, e)
}
func (j graphJournal) loadPayloadIn(ctx context.Context, tx transactionAPI, e GraphEntry) ([]byte, error) {
	p := e.Payload
	if p == nil || p.Size <= 0 || p.Size > maxGraphPayload || p.Chunks != (p.Size+graphPayloadChunk-1)/graphPayloadChunk {
		return nil, errors.New("invalid graph payload descriptor")
	}
	keys := make([][]byte, p.Chunks)
	for n := range keys {
		keys[n] = j.payloadKey(e.Position.Sequence, n)
	}
	values, err := batchGet(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, p.Size)
	for n, key := range keys {
		part, ok := values[string(key)]
		if !ok || len(part) != min(graphPayloadChunk, p.Size-n*graphPayloadChunk) {
			return nil, errors.New("missing graph payload chunk")
		}
		data = append(data, part...)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != p.SHA256 {
		return nil, errors.New("graph payload checksum mismatch")
	}
	return data, nil
}

func (g *graphReplica) loadReferenceEntry(ctx context.Context, e GraphEntry) (*GraphDelta, error) {
	// Payload and source events share a single MVCC snapshot, including during GC.
	tx, err := g.repo.begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	measure := g.track("graph_payload_load")
	data, err := g.journal.loadPayloadIn(ctx, tx, e)
	measure(&err)
	if err != nil {
		return nil, err
	}
	var wire ReferenceDelta
	if err = json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	if wire.Format != referenceDeltaFormat || wire.Delta.Records != nil {
		return nil, errors.New("invalid reference delta format")
	}
	keys := [][]byte{}
	for _, ch := range wire.Changes {
		if ch.Ref != nil {
			r := g.journal.c.data[ch.Ref.Source]
			if r == nil {
				return nil, errors.New("unknown reference source")
			}
			keys = append(keys, r.logKey(ch.Ref.LSN))
		}
	}
	measure = g.track("graph_data_load")
	values, err := batchGet(ctx, tx, keys)
	measure(&err)
	if err != nil {
		return nil, err
	}
	d := wire.Delta
	for _, ch := range wire.Changes {
		r := ch.Tombstone
		if ch.Ref != nil {
			ref := ch.Ref
			repo := g.journal.c.data[ref.Source]
			var event LogEvent
			if err = json.Unmarshal(values[string(repo.logKey(ref.LSN))], &event); err != nil {
				return nil, fmt.Errorf("missing or invalid data log reference: %w", err)
			}
			if event.Sequence != ref.LSN || event.Record.ID != ref.ID || event.Record.Revision != ref.Revision || recordHash(event.Record) != ref.Hash {
				return nil, errors.New("data log reference mismatch")
			}
			r = event.Record
		} else if !r.Deleted || len(r.Vector) != 0 || len(r.Payload) != 0 {
			return nil, errors.New("invalid inline tombstone")
		}
		d.Records = append(d.Records, RecordChange{After: r})
	}
	d.references = wire.Changes
	if d.references == nil {
		d.references = []ReferenceChange{}
	}
	return &d, nil
}
