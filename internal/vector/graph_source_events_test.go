package vector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestRawGraphSourceEventsPreservePinnedHistory(t *testing.T) {
	ctx := context.Background()
	h := newRawShardHistory()
	appendRecord := func(seq uint64, record Record) {
		t.Helper()
		if err := h.extend([]mutationLogBatch{{Number: seq, First: seq, Entries: []mutationLogEntry{{RequestID: fmt.Sprint(seq), Digest: "digest", Record: record}}}}); err != nil {
			t.Fatal(err)
		}
	}
	appendRecord(1, Record{ID: "same", Revision: 1, Vector: []float64{1, 2}, Payload: map[string]string{"old": "value"}})
	repo := rawTestRepo(0)
	tx := &rawSnapshotTransaction{raw: newRawDataStore(nil, rawTestNamespace), pinned: map[string]*rawShardSnapshot{repo.Prefix: {history: h, end: 1}}}
	appendRecord(2, Record{ID: "same", Revision: 2, Vector: []float64{3, 4}})
	// Pin a second source with the same sequence; references must keep their source.
	other := rawTestRepo(1)
	h2 := newRawShardHistory()
	if err := h2.extend([]mutationLogBatch{{Number: 1, First: 1, Entries: []mutationLogEntry{{RequestID: "other", Digest: "d", Record: Record{ID: "other", Revision: 1, Deleted: true}}}}}); err != nil {
		t.Fatal(err)
	}
	tx.pinned[other.Prefix] = &rawShardSnapshot{history: h2, end: 1}
	keys := [][]byte{repo.logKey(1), other.logKey(1), repo.logKey(1)}
	check := func() {
		t.Helper()
		got, err := readGraphSourceEvents(ctx, tx, keys)
		want, oldErr := readJSONGraphSourceEvents(ctx, tx, keys)
		if err != nil || oldErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(got, want, err, oldErr)
		}
		got[0].Record.Vector[0] = 99
		got[0].Record.Payload["old"] = "changed"
		if got[2].Record.Vector[0] != 1 || got[2].Record.Payload["old"] != "value" {
			t.Fatal("duplicate references share mutable contents")
		}
		again, err := readGraphSourceEvents(ctx, tx, keys)
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatal("caller mutated history", err)
		}
		if _, err := readGraphSourceEvents(ctx, tx, [][]byte{repo.logKey(2)}); err == nil {
			t.Fatal("read past pinned end")
		}
	}
	check()
	// Move both versions out of memory, retaining the already pinned snapshot.
	if err := h.spillLocked(rawHistoryHotEntries, 0); err != nil {
		t.Fatal(err)
	}
	if h.spilled != 2 {
		t.Fatal("did not spill", h.spilled)
	}
	t.Cleanup(func() { _ = h.archive.Close() })
	check()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := readGraphSourceEvents(canceled, tx, keys); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	tx.pinned[repo.Prefix].floor = 1
	if _, err := readGraphSourceEvents(ctx, tx, keys); err == nil {
		t.Fatal("read below floor")
	}
	tx.pinned[repo.Prefix].floor = 0
	if _, err := h.archive.WriteAt([]byte("bad"), h.locations[0].offset); err != nil {
		t.Fatal(err)
	}
	if _, err := readGraphSourceEvents(ctx, tx, keys); err == nil {
		t.Fatal("ignored corrupt history file")
	}
}

func TestRawGraphRecordCopyMatchesJSON(t *testing.T) {
	for _, record := range []Record{
		{ID: "deleted", Revision: math.MaxUint64, Deleted: true},
		{ID: "empty", Vector: []float64{}, Payload: map[string]string{}},
		{ID: "向量", Vector: []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64}, Payload: map[string]string{"<>&\x00": "\n\""}},
		{ID: "bad\xff", Payload: map[string]string{"bad\xff": "\xfe", "bad\xfe": "second"}},
		{ID: "nan", Vector: []float64{math.NaN()}},
		{ID: "inf", Vector: []float64{math.Inf(1)}},
	} {
		t.Run(record.ID, func(t *testing.T) {
			raw, oldErr := json.Marshal(record)
			var want Record
			if oldErr == nil {
				oldErr = json.Unmarshal(raw, &want)
			}
			got, err := copyRawGraphRecord(record)
			if (err == nil) != (oldErr == nil) {
				t.Fatal(err, oldErr)
			}
			if err == nil {
				if !reflect.DeepEqual(got, want) {
					t.Fatal(got, want)
				}
				for i, v := range got.Vector {
					if math.Float64bits(v) != math.Float64bits(want.Vector[i]) {
						t.Fatal("float bits changed")
					}
				}
			}
		})
	}
}

func TestRawGraphReferenceValidation(t *testing.T) {
	ctx := context.Background()
	root, _ := newFakeRepo()
	root.Prefix = rawTestNamespace
	store := newRawDataStore(newFakeRawData(), rawTestNamespace)
	root.raw = store
	repo := rawTestRepo(0)
	w := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	rawTestAccept(t, ctx, w, rawTestRequest(t, repo, "old", "first"))
	rec := Record{ID: "old", Revision: 1, Vector: []float64{1, 2}}
	g := &graphReplica{repo: root, journal: graphJournal{c: &Coordinator{root: root, data: map[DataPartitionID]*Repository{0: repo}}}}
	for _, damage := range []string{"none", "id", "revision", "hash", "sequence", "source"} {
		t.Run(damage, func(t *testing.T) {
			ref := DataReference{Source: 0, LSN: 1, ID: rec.ID, Revision: rec.Revision, Hash: recordHash(rec)}
			switch damage {
			case "id":
				ref.ID = "wrong"
			case "revision":
				ref.Revision++
			case "hash":
				ref.Hash = "wrong"
			case "sequence":
				ref.LSN++
			case "source":
				ref.Source = 99
			}
			wire := ReferenceDelta{Format: referenceDeltaFormat, Changes: []ReferenceChange{{Tombstone: Record{ID: "deleted", Revision: 1, Deleted: true}}, {Ref: &ref}, {Ref: &ref}}}
			data, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			entry := GraphEntry{Payload: &GraphPayload{Size: len(data), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}, PayloadData: data}
			delta, err := g.loadReferenceEntry(ctx, entry)
			if damage != "none" {
				if err == nil || delta != nil {
					t.Fatal("accepted invalid reference", damage)
				}
				return
			}
			if err != nil || len(delta.Records) != 3 || !delta.Records[0].After.Deleted || !reflect.DeepEqual(delta.Records[1].After, rec) || !reflect.DeepEqual(delta.Records[2].After, rec) {
				t.Fatal(delta, err)
			}
		})
	}
}

func BenchmarkGraphSourceEvents(b *testing.B) {
	for _, dimension := range []int{64, 768} {
		repo := rawTestRepo(0)
		h := newRawShardHistory()
		keys := make([][]byte, 256)
		entries := make([]mutationLogEntry, len(keys))
		for i := range keys {
			v := make([]float64, dimension)
			for j := range v {
				v[j] = math.Sin(float64(i + j + 1))
			}
			entries[i] = mutationLogEntry{RequestID: fmt.Sprint(i), Digest: "d", Record: Record{ID: fmt.Sprint(i), Revision: 1, Vector: v, Payload: map[string]string{"group": "one"}}}
			keys[i] = repo.logKey(uint64(i + 1))
		}
		for first := 0; first < len(entries); first += mutationGroupRecords {
			if err := h.extend([]mutationLogBatch{{Number: uint64(first/mutationGroupRecords + 1), First: uint64(first + 1), Entries: entries[first:min(first+mutationGroupRecords, len(entries))]}}); err != nil {
				b.Fatal(err)
			}
		}
		tx := &rawSnapshotTransaction{raw: newRawDataStore(nil, rawTestNamespace), pinned: map[string]*rawShardSnapshot{repo.Prefix: {history: h, end: uint64(len(keys))}}}
		for _, spilled := range []bool{false, true} {
			if spilled {
				if err := h.spillLocked(rawHistoryHotEntries, 0); err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = h.archive.Close() })
			}
			for _, direct := range []bool{false, true} {
				b.Run(fmt.Sprintf("dim%d/file%v/direct%v", dimension, spilled, direct), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var events []LogEvent
						var err error
						if direct {
							events, err = readGraphSourceEvents(context.Background(), tx, keys)
						} else {
							events, err = readJSONGraphSourceEvents(context.Background(), tx, keys)
						}
						if err != nil || len(events) != len(keys) {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
