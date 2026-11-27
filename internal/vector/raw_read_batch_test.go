package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func rawReadLog(t *testing.T, slots, records int, payload func(int) string) (rawShardStore, *fakeRawData, *rawSnapshotProbe, *[]int) {
	t.Helper()
	client := newFakeRawData()
	probe := &rawSnapshotProbe{rawDataClient: client}
	store := rawShardStore{client: probe, prefix: "read-batch-test/"}
	var widths []int
	probe.afterBatch = func(keys, _ [][]byte) { widths = append(widths, len(keys)) }
	for n := 1; n <= slots; n++ {
		batch := mutationLogBatch{Number: uint64(n), First: uint64((n-1)*records + 1)}
		for i := 0; i < records; i++ {
			id := fmt.Sprint((n-1)*records + i + 1)
			batch.Entries = append(batch.Entries, mutationLogEntry{RequestID: id, Digest: "test-digest", Record: Record{ID: id, Revision: 1, Vector: []float64{1, 2}, Payload: map[string]string{"value": payload(n)}}})
		}
		data, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.put(context.Background(), store.batchKey(1, uint64(n)), data); err != nil {
			t.Fatal(err)
		}
	}
	return store, client, probe, &widths
}

func TestRawReadBatchesSmallValuesAndKeepsLegacyRecords(t *testing.T) {
	for _, records := range []int{1, mutationGroupRecords} {
		t.Run(fmt.Sprint(records), func(t *testing.T) {
			const slots = 260
			s, _, _, widths := rawReadLog(t, slots, records, func(int) string { return "" })
			h := newRawShardHistory()
			after, err := s.readEpoch(context.Background(), h, 1, 0, slots)
			if err != nil || after != slots || h.end() != slots*uint64(records) {
				t.Fatal(after, h.end(), err)
			}
			if len(*widths) >= slots/6 {
				t.Fatal("small logs still require too many reads", *widths)
			}
			for _, width := range *widths {
				if width > rawReadMaxSlots {
					t.Fatal("read window exceeded", width)
				}
			}
			snap := rawShardSnapshot{history: h, end: h.end()}
			for _, seq := range []uint64{1, h.end()} {
				e, ok, err := snap.entry(seq)
				if err != nil || !ok || e.Record.ID != fmt.Sprint(seq) {
					t.Fatal(e, ok, err)
				}
			}
		})
	}
}

func TestRawReadShrinksAfterLargeValues(t *testing.T) {
	for _, tc := range []struct {
		size, limit int
		mixed       bool
	}{{300 << 10, 4, false}, {300 << 10, 4, true}, {100 << 10, 8, false}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			payload := strings.Repeat("x", tc.size)
			s, _, _, widths := rawReadLog(t, 60, 1, func(slot int) string {
				if !tc.mixed || slot == 5 {
					return payload
				}
				return ""
			})
			after, err := s.readEpoch(context.Background(), newRawShardHistory(), 1, 0, 60)
			if err != nil || after != 60 {
				t.Fatal(after, err)
			}
			if tc.mixed {
				if len(*widths) < 3 || (*widths)[0] != 4 || (*widths)[1] != 8 || (*widths)[2] != 4 {
					t.Fatal("did not adapt to large values", *widths)
				}
			} else {
				for _, width := range *widths {
					if width > tc.limit || width%rawReadDecodeSlots != 0 {
						t.Fatal("large values widened the read", *widths)
					}
				}
			}
		})
	}
}

func TestRawReadStopsAtHoleAndResumesAfterRepair(t *testing.T) {
	s, client, _, _ := rawReadLog(t, 80, 1, func(int) string { return "" })
	key := string(s.batchKey(1, 9))
	saved := client.data[key]
	delete(client.data, key)
	h := newRawShardHistory()
	after, err := s.readEpoch(context.Background(), h, 1, 0, 80)
	if err != nil || after != 8 || h.end() != 8 {
		t.Fatal("read crossed a gap", after, h.end(), err)
	}
	pinned := rawShardSnapshot{history: h, end: h.end()}
	client.data[key] = saved
	after, err = s.readEpoch(context.Background(), h, 1, after, 80)
	if err != nil || after != 80 || h.end() != 80 {
		t.Fatal("failed to resume", after, h.end(), err)
	}
	if _, ok, err := pinned.record("80"); ok || err != nil {
		t.Fatal("old snapshot changed", ok, err)
	}
}

func TestRawReadCorruptChunkPreservesAcceptedCursor(t *testing.T) {
	for _, kind := range []string{"json", "number", "revision"} {
		t.Run(kind, func(t *testing.T) {
			s, client, _, _ := rawReadLog(t, 80, 1, func(int) string { return "" })
			key := string(s.batchKey(1, 11))
			saved := client.data[key]
			var batch mutationLogBatch
			if err := json.Unmarshal(saved, &batch); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "json":
				client.data[key] = []byte("invalid")
			case "number":
				batch.Number++
			case "revision":
				batch.Entries[0].Record.Revision++
			}
			if kind != "json" {
				client.data[key], _ = json.Marshal(batch)
			}
			h := newRawShardHistory()
			after, err := s.readEpoch(context.Background(), h, 1, 0, 80)
			if err == nil || after != 8 || h.end() != 8 {
				t.Fatal("corrupt chunk advanced history or lost the accepted cursor", after, h.end(), err)
			}
			client.data[key] = saved
			after, err = s.readEpoch(context.Background(), h, 1, after, 80)
			if err != nil || after != 80 || h.end() != 80 {
				t.Fatal("retry replayed or skipped accepted records", after, h.end(), err)
			}
		})
	}
}

func TestRawReadCancellationPreservesAcceptedCursor(t *testing.T) {
	s, _, probe, _ := rawReadLog(t, 80, 1, func(int) string { return "" })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe.afterBatch = func(_, _ [][]byte) {
		if probe.batches == 3 {
			cancel()
		}
	}
	h := newRawShardHistory()
	after, err := s.readEpoch(ctx, h, 1, 0, 80)
	if !errors.Is(err, context.Canceled) || after != 12 || h.end() != 12 {
		t.Fatal(after, h.end(), err)
	}
	probe.afterBatch = nil
	after, err = s.readEpoch(context.Background(), h, 1, after, 80)
	if err != nil || after != 80 || h.end() != 80 {
		t.Fatal(after, h.end(), err)
	}
}
