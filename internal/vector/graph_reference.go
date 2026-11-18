package vector

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"example.com/vector-storage/internal/ann"
	"fmt"
)

const referenceDeltaFormat = "hnsw-data-reference-v1"

type DataReference struct {
	Source   DataPartitionID `json:"source"`
	LSN      uint64          `json:"lsn"`
	ID       string          `json:"id"`
	Revision uint64          `json:"revision"`
	Hash     string          `json:"hash"`
}
type ReferenceChange struct {
	BeforeHash string         `json:"before_hash"`
	Ref        *DataReference `json:"ref,omitempty"`
	Tombstone  Record         `json:"tombstone,omitempty"`
}
type ReferenceDelta struct {
	Format  string            `json:"format"`
	Delta   GraphDelta        `json:"delta"`
	Changes []ReferenceChange `json:"changes"`
}

func recordHash(r Record) string { b, _ := json.Marshal(r); return fmt.Sprintf("%x", sha256.Sum256(b)) }

func referenceDelta(d GraphDelta, inputs []Delivery) ([]byte, error) {
	refs := map[string]DataReference{}
	for _, q := range inputs {
		for _, e := range q.Events {
			if old, ok := refs[e.Record.ID]; !ok || e.Record.Revision > old.Revision {
				refs[e.Record.ID] = DataReference{q.Source, e.Sequence, e.Record.ID, e.Record.Revision, recordHash(e.Record)}
			}
		}
	}
	w := ReferenceDelta{Format: referenceDeltaFormat, Delta: d, Changes: make([]ReferenceChange, 0, len(d.Records))}
	w.Delta.Records = nil
	var err error
	w.Delta.Native, err = ann.CompactPatch(d.Native, d.Dimension)
	if err != nil {
		return nil, err
	}
	for _, ch := range d.Records {
		change := ReferenceChange{BeforeHash: recordHash(ch.Before)}
		if ch.After.Deleted {
			// Scrub removes misplaced records without inventing a source event.
			change.Tombstone = Record{ID: ch.After.ID, Revision: ch.After.Revision, Deleted: true}
		} else {
			ref, ok := refs[ch.After.ID]
			if !ok || ref.Revision != ch.After.Revision || ref.Hash != recordHash(ch.After) {
				return nil, errors.New("graph mutation lacks an exact data log reference")
			}
			change.Ref = &ref
		}
		w.Changes = append(w.Changes, change)
	}
	return json.Marshal(w)
}

// Called with Index.mu held, immediately before ordered apply. Old labels remain
// in the native graph even after record updates/deletes, so their immutable
// vector bytes are available without retaining historical record maps.
func (i *HNSWIndex) expandReferenceDelta(d GraphDelta) (GraphDelta, error) {
	if len(d.references) != len(d.Records) {
		return d, errors.New("reference change count mismatch")
	}
	records := map[string]Record{}
	for n, ch := range d.references {
		r := d.Records[n].After
		before := i.records[r.ID]
		if recordHash(before) != ch.BeforeHash {
			return d, errors.New("reference delta base record mismatch")
		}
		d.Records[n].Before = cloneRecord(before)
		records[r.ID] = r
	}
	cache := map[uint64][]float32{}
	native, err := ann.ExpandPatch(d.Native, d.Dimension, func(label uint64) ([]float32, error) {
		if v, ok := cache[label]; ok {
			return v, nil
		}
		var v []float32
		if meta, ok := d.Nodes[label]; ok {
			r, ok := records[meta.ID]
			if !ok || r.Revision != meta.Revision || r.Deleted {
				return nil, errors.New("reference node mismatch")
			}
			v = make([]float32, len(r.Vector))
			for n, x := range r.Vector {
				v[n] = float32(x)
			}
		} else {
			var e error
			v, e = i.graph.Vector(label)
			if e != nil {
				return nil, e
			}
		}
		cache[label] = v
		return v, nil
	})
	if err != nil {
		return d, err
	}
	d.Native = native
	d.references = nil
	return d, nil
}
