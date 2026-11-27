package vector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRawHistorySpillPreservesSnapshotsRetriesAndRecovery(t *testing.T) {
	ctx := context.Background()
	repo := rawTestRepo(0)
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	w := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	first := rawTestRequest(t, repo, "original", "first")
	rawTestAccept(t, ctx, w, first)
	pinned := &rawShardSnapshot{history: w.history, end: 1}
	for i := 0; i < rawHistoryHotEntries+10; i++ {
		id := fmt.Sprint(i)
		rawTestAccept(t, ctx, w, rawTestRequest(t, repo, id, id))
	}
	h := w.history
	if h.spilled < 512 || h.entries[0] != nil || len(h.entries)-h.spilled > rawHistoryHotEntries {
		t.Fatal("old contents retained in memory")
	}
	if _, err := os.Stat(h.archive.Name()); !os.IsNotExist(err) {
		t.Fatal("cache file was not unlinked", err)
	}
	// Update the same record after its original version has left memory.
	result, batch, err := w.prepare([]*mutationRequest{rawTestRequest(t, repo, "original", "update")})
	if err != nil || result[0].record.Revision != 2 {
		t.Fatal(result, err)
	}
	if _, err = w.write(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err = w.accept(batch); err != nil {
		t.Fatal(err)
	}
	if r, found, err := pinned.record("original"); err != nil || !found || r.Revision != 1 {
		t.Fatal("pinned read changed", r, err)
	}
	if _, found, err := pinned.record("0"); err != nil || found {
		t.Fatal("pinned read saw future record", err)
	}
	result, batch, err = w.prepare([]*mutationRequest{first})
	if err != nil || result[0].err != nil || result[0].record.Revision != 1 || batch.Number != 0 {
		t.Fatal("old retry changed", result, err)
	}
	// Discard this process's cache entirely. The WAL alone must restore it.
	if err = h.archive.Close(); err != nil {
		t.Fatal(err)
	}
	next := rawTestRecover(t, ctx, newRawDataStore(client, rawTestNamespace), repo, rawTestOwner(0, 2))
	t.Cleanup(func() {
		if next.history.archive != nil {
			_ = next.history.archive.Close()
		}
	})
	result, batch, err = next.prepare([]*mutationRequest{first})
	if err != nil || result[0].err != nil || result[0].record.Revision != 1 || batch.Number != 0 {
		t.Fatal("recovery lost old receipt", result, err)
	}
}

func TestRawHistoryPinnedReadsDuringSpill(t *testing.T) {
	h := newRawShardHistory()
	appendVersion := func(seq uint64) {
		t.Helper()
		err := h.extend([]mutationLogBatch{{Number: seq, First: seq, Entries: []mutationLogEntry{{RequestID: fmt.Sprint(seq), Digest: "digest", Record: Record{ID: "same", Revision: seq, Vector: []float64{float64(seq)}}}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	appendVersion(1)
	snap := &rawShardSnapshot{history: h, end: 1}
	stop, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				finished <- nil
				return
			case <-tick.C:
				r, found, err := snap.record("same")
				if err != nil || !found || r.Revision != 1 || r.Vector[0] != 1 {
					finished <- fmt.Errorf("pinned read changed: %v %v", r, err)
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		if err := <-finished; err != nil {
			t.Error(err)
		}
		if h.archive != nil {
			_ = h.archive.Close()
		}
	})
	for seq := uint64(2); seq <= 2*rawHistoryHotEntries; seq++ {
		appendVersion(seq)
	}
	if h.spilled == 0 {
		t.Fatal("test did not spill")
	}
}

func TestRawHistorySpillErrorsAreNotMissingRecords(t *testing.T) {
	h := newRawShardHistory()
	// Exercise the byte bound with fewer than 4096 records.
	for i := 1; i <= 150; i++ {
		id := fmt.Sprint(i)
		err := h.extend([]mutationLogBatch{{Number: uint64(i), First: uint64(i), Entries: []mutationLogEntry{{RequestID: id, Digest: id, Record: Record{ID: id, Revision: 1, Payload: map[string]string{"body": strings.Repeat("x", 64<<10)}}}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if h.spilled == 0 || h.hotBytes > rawHistoryHotBytes {
		t.Fatal("byte bound not enforced")
	}
	t.Cleanup(func() { _ = h.archive.Close() })
	snap := &rawShardSnapshot{history: h, end: h.end()}
	if _, err := h.archive.WriteAt([]byte("!"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := marshalRawRecord(snap, "1"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatal("corruption hidden", err)
	}
	if _, err := marshalRawEvent(snap, 1); err == nil {
		t.Fatal("corrupt log accepted")
	}
	if _, _, err := h.receipt("1"); err == nil {
		t.Fatal("corrupt receipt accepted")
	}
	// A failed spill must not append a decision or alter record indexes.
	_ = h.archive.Close()
	before := h.end()
	h.hotBytes = rawHistoryHotBytes // force the next append to try spilling
	err := h.extend([]mutationLogBatch{{Number: before + 1, First: before + 1, Entries: []mutationLogEntry{{RequestID: "failed", Digest: "failed", Record: Record{ID: "failed", Revision: 1}}}}})
	if err == nil || h.end() != before || h.revision("failed") != 0 || h.receipts["failed"] != 0 {
		t.Fatal("failed spill accepted new decision", err)
	}
}
