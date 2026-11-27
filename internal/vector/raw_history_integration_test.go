package vector

import (
	"fmt"
	"testing"
)

func TestRawTiKVHistorySpillRecoversWithoutOldCache(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	store, err := openRawDataStore(f.ctx, f.r.pd, f.r.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	w := rawTestRecover(t, f.ctx, store, f.r, rawTestOwner(0, 1))
	const count = rawHistoryHotEntries + 64
	// Grouping also covers older WAL values containing several decisions.
	for first := 0; first < count; first += mutationGroupRecords {
		var requests []*mutationRequest
		for i := first; i < min(first+mutationGroupRecords, count); i++ {
			id := fmt.Sprint(i)
			requests = append(requests, rawTestRequest(t, f.r, id, id))
		}
		_, batch, err := w.prepare(requests)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.write(f.ctx, batch); err != nil {
			t.Fatal(err)
		}
		if err = w.accept(batch); err != nil {
			t.Fatal(err)
		}
	}
	if w.history.spilled == 0 {
		t.Fatal("test did not spill")
	}
	_ = w.history.archive.Close()
	// A new store/writer has no access to the closed old cache file.
	next := rawTestRecover(t, f.ctx, newRawDataStore(store.client, f.r.Prefix), f.r, rawTestOwner(0, 2))
	t.Cleanup(func() {
		if next.history.archive != nil {
			_ = next.history.archive.Close()
		}
	})
	if next.history.end() != count || next.history.spilled == 0 {
		t.Fatal("incomplete recovery")
	}
	next.store.client = nil
	result, batch, err := next.prepare([]*mutationRequest{rawTestRequest(t, f.r, "0", "0")})
	if err != nil || result[0].err != nil || result[0].record.Revision != 1 || batch.Number != 0 {
		t.Fatal("recovery lost old request", result, batch, err)
	}
}
