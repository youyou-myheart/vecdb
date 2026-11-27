package vector

import (
	"context"
	"fmt"
	"testing"
)

func TestRawWriterReleasesConfirmedProjectionAndPreservesPendingRevision(t *testing.T) {
	ctx := context.Background()
	repo := rawTestRepo(0)
	store := newRawDataStore(newFakeRawData(), rawTestNamespace)
	w := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	requests := []*mutationRequest{rawTestRequest(t, repo, "same", "first"), rawTestRequest(t, repo, "same", "second")}
	var batches []mutationLogBatch
	for i, q := range requests {
		results, batch, err := w.prepare([]*mutationRequest{q})
		if err != nil || results[0].err != nil || results[0].record.Revision != uint64(i+1) {
			t.Fatal(results, err)
		}
		batches = append(batches, batch)
	}
	if _, err := w.write(ctx, batches...); err != nil {
		t.Fatal(err)
	}
	if err := w.accept(batches[0]); err != nil {
		t.Fatal(err)
	}
	if len(w.receipts) != 1 || w.records["same"] != 2 {
		t.Fatal("lost pending revision", w.mutationBatchState)
	}
	for i, q := range requests {
		results, batch, err := w.prepare([]*mutationRequest{q})
		if err != nil || results[0].err != nil || batch.Number != 0 || results[0].record.Revision != uint64(i+1) {
			t.Fatal("retry changed pending or confirmed result", results, batch, err)
		}
	}
	if err := w.accept(batches[1]); err != nil {
		t.Fatal(err)
	}
	if len(w.records) != 0 || len(w.receipts) != 0 {
		t.Fatal("confirmed projection retained")
	}
	results, _, err := w.prepare([]*mutationRequest{rawTestRequest(t, repo, "same", "third")})
	if err != nil || results[0].record.Revision != 3 {
		t.Fatal("forgot confirmed revision", results, err)
	}
}

func TestRawWriterProjectionDoesNotGrowWithConfirmedHistory(t *testing.T) {
	ctx := context.Background()
	repo := rawTestRepo(0)
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	w := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	for i := 0; i < 2048; i++ {
		id := fmt.Sprint(i)
		rawTestAccept(t, ctx, w, rawTestRequest(t, repo, id, id))
		if len(w.records) != 0 || len(w.receipts) != 0 {
			t.Fatal("confirmed request kept in projection", i)
		}
	}
	// A fresh process reconstructs the shared history, without making a
	// second permanent receipt/revision map for its writer.
	w = rawTestRecover(t, ctx, newRawDataStore(client, rawTestNamespace), repo, rawTestOwner(0, 2))
	if w.history.end() != 2048 || len(w.records) != 0 || len(w.receipts) != 0 {
		t.Fatal("recovery duplicated history")
	}
	w.store.client = nil // Retrying old requests must not read TiKV.
	for _, id := range []string{"0", "1024", "2047"} {
		results, batch, err := w.prepare([]*mutationRequest{rawTestRequest(t, repo, id, id)})
		if err != nil || results[0].err != nil || batch.Number != 0 || results[0].record.Revision != 1 {
			t.Fatal(results, batch, err)
		}
	}
}
