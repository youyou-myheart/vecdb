package vector

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestRawSnapshotPinsDataAndKeepsMetadataTransaction(t *testing.T) {
	ctx := context.Background()
	store := newRawDataStore(newFakeRawData(), rawTestNamespace)
	root, db := newFakeRepo()
	root.Prefix, root.raw = rawTestNamespace, store
	repo := rawTestRepo(1)
	repo.beginTxn, repo.raw = root.beginTxn, store
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(1, 1))
	rawTestAccept(t, ctx, writer, rawTestRequest(t, repo, "a", "one"))

	tx, err := root.begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if head, err := repo.logHead(ctx, tx); err != nil || head != 1 {
		t.Fatalf("unexpected initial head %d, %v", head, err)
	}
	rawTestAccept(t, ctx, writer, rawTestRequest(t, repo, "ab", "two"))
	if record, err := repo.Get(ctx, "ab"); err != nil || record.ID != "ab" {
		t.Fatalf("fresh reader saw %+v, %v", record, err)
	}
	if head, err := repo.logHead(ctx, tx); err != nil || head != 1 {
		t.Fatalf("pinned log head changed to %d, %v", head, err)
	}
	rows, err := repo.scanTxn(ctx, tx, "record")
	if err != nil || len(rows) != 1 || rows[0].ID != "a" {
		t.Fatalf("pinned records changed to %+v, %v", rows, err)
	}
	meta := []byte(repo.Prefix + "dispatch-completed")
	db.data[string(meta)] = []byte("1")
	values, err := batchGet(ctx, tx, [][]byte{meta, repo.key("record", "a"), repo.key("record", "ab")})
	if err != nil || len(values) != 2 || string(values[string(meta)]) != "1" || len(values[string(repo.key("record", "a"))]) == 0 {
		t.Fatalf("mixed read returned %+v, %v", values, err)
	}
	if err := tx.Delete(repo.logKey(1)); err == nil {
		t.Fatal("metadata transaction deleted a raw log entry")
	}

	page, err := repo.ListRecords(ctx, ListOptions{Limit: 1})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "a" || page.NextCursor == "" {
		t.Fatalf("unexpected first page %+v, %v", page, err)
	}
	page, err = repo.ListRecords(ctx, ListOptions{Limit: 2, Cursor: page.NextCursor})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "ab" {
		t.Fatalf("exclusive cursor returned %+v, %v", page, err)
	}
	batch, err := repo.ReadEvents(ctx, 1, 0, 1)
	if err != nil || batch.Head != 2 || len(batch.Events) != 1 || batch.Events[0].Sequence != 2 || batch.Events[0].Record.ID != "ab" {
		t.Fatalf("fresh log read returned %+v, %v", batch, err)
	}
}

// A reader and writer share storage but only the reader's IO is counted.
// Hooks can change the data or epoch between a point probe and validation.
type rawSnapshotProbe struct {
	rawDataClient
	headKey    string
	heads      int
	batches    int
	afterBatch func([][]byte, [][]byte)
}

func (p *rawSnapshotProbe) Get(ctx context.Context, key []byte) ([]byte, error) {
	if string(key) == p.headKey {
		p.heads++
	}
	return p.rawDataClient.Get(ctx, key)
}

func (p *rawSnapshotProbe) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	p.batches++
	values, err := p.rawDataClient.BatchGet(ctx, keys)
	if err == nil && p.afterBatch != nil {
		p.afterBatch(keys, values)
	}
	return values, err
}

func TestRawFallbackPointProbesAndValidatesOneHead(t *testing.T) {
	ctx := context.Background()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo := rawTestRepo(0)
	shard := store.shard(repo.Prefix)
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	probe := &rawSnapshotProbe{rawDataClient: client, headKey: string(shard.headKey())}
	repo.raw = newRawDataStore(probe, rawTestNamespace)
	for i := 0; i < 2; i++ {
		probe.batches, probe.heads = 0, 0
		if _, err := repo.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if probe.batches != 1 || probe.heads != 1 {
			t.Fatalf("empty read %d: %d WAL probes, %d heads", i, probe.batches, probe.heads)
		}
	}
	rawTestAccept(t, ctx, writer, rawTestRequest(t, repo, "a", "1"))
	probe.heads = 0
	if r, err := repo.Get(ctx, "a"); err != nil || r.Revision != 1 || probe.heads != 1 {
		t.Fatal(r, err, probe.heads)
	}
	probe.batches, probe.heads = 0, 0
	if _, err := repo.Get(ctx, "a"); err != nil || probe.batches != 1 || probe.heads != 1 {
		t.Fatal(err, probe.batches, probe.heads)
	}
}

func TestRawFallbackDropsLateOldTailAtFinalFence(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprint(malformed), func(t *testing.T) {
			ctx := context.Background()
			client := newFakeRawData()
			store := newRawDataStore(client, rawTestNamespace)
			repo := rawTestRepo(0)
			shard := store.shard(repo.Prefix)
			old := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
			rawTestAccept(t, ctx, old, rawTestRequest(t, repo, "base", "1"))
			probe := &rawSnapshotProbe{rawDataClient: client, headKey: string(shard.headKey())}
			reader := newRawDataStore(probe, rawTestNamespace)
			pinned, err := reader.snapshot(ctx, repo.Prefix)
			if err != nil {
				t.Fatal(err)
			}
			// Let the old WAL probe race the takeover. This Region returns an
			// old slot written after the successor sealed its first slot.
			probe.afterBatch = func(keys, values [][]byte) {
				probe.afterBatch = nil
				next := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 2))
				if malformed {
					if err := shard.put(ctx, shard.batchKey(1, 2), []byte("invalid")); err != nil {
						t.Fatal(err)
					}
				} else {
					rawTestAccept(t, ctx, old, rawTestRequest(t, repo, "late", "late-request"))
				}
				rawTestAccept(t, ctx, next, rawTestRequest(t, repo, "replacement", "2"))
				var err error
				values[0], err = client.Get(ctx, keys[0])
				if err != nil {
					t.Fatal(err)
				}
			}
			view, err := reader.snapshot(ctx, repo.Prefix)
			if err != nil || view.end != 2 {
				t.Fatal(view, err)
			}
			if _, ok := view.record("replacement"); !ok {
				t.Fatal("successor WAL missing")
			}
			if _, ok := view.record("late"); ok {
				t.Fatal("read obsolete tail")
			}
			if pinned.end != 1 {
				t.Fatal("previous view changed", pinned.end)
			}
			if _, ok := pinned.record("replacement"); ok {
				t.Fatal("previous view saw the new epoch")
			}
		})
	}
}

func TestRawFallbackRechecksPartialPageAcrossRegions(t *testing.T) {
	ctx := context.Background()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo := rawTestRepo(0)
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	probe := &rawSnapshotProbe{rawDataClient: client, headKey: string(store.shard(repo.Prefix).headKey())}
	reader := newRawDataStore(probe, rawTestNamespace)
	if _, err := reader.snapshot(ctx, repo.Prefix); err != nil {
		t.Fatal(err)
	}
	var data []mutationLogBatch
	for _, id := range []string{"a", "b"} {
		_, batch, err := writer.prepare([]*mutationRequest{rawTestRequest(t, repo, id, id)})
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, batch)
	}
	// Simulate the second Region observing slot 2 before either write,
	// then slot 2 and slot 1 landing in that order. At no time was slot 1
	// the last contiguous WAL slot.
	probe.afterBatch = func(keys, values [][]byte) {
		probe.afterBatch = nil
		for _, batch := range []mutationLogBatch{data[1], data[0]} {
			if err := writer.write(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		values[0], err = client.Get(ctx, keys[0])
		if err != nil {
			t.Fatal(err)
		}
	}
	view, err := reader.snapshot(ctx, repo.Prefix)
	if err != nil || view.end != 2 {
		t.Fatal("torn prefix", view, err)
	}
}

func TestRawFallbackHonorsLegacyPublicationDuringTailRead(t *testing.T) {
	ctx := context.Background()
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo := rawTestRepo(0)
	shard := store.shard(repo.Prefix)
	writer := rawTestRecover(t, ctx, store, repo, rawTestOwner(0, 1))
	probe := &rawSnapshotProbe{rawDataClient: client, headKey: string(shard.headKey())}
	reader := newRawDataStore(probe, rawTestNamespace)
	if _, err := reader.snapshot(ctx, repo.Prefix); err != nil {
		t.Fatal(err)
	}
	publish := func(n uint64) {
		h, prev, err := shard.head(ctx)
		if err != nil {
			t.Fatal(err)
		}
		h.Published = n
		if ok, err := shard.swapHead(ctx, prev, h); err != nil || !ok {
			t.Fatal("legacy publish", ok, err)
		}
	}
	probe.afterBatch = func(_, _ [][]byte) {
		probe.afterBatch = nil
		rawTestAccept(t, ctx, writer, rawTestRequest(t, repo, "a", "1"))
		publish(1)
	}
	view, err := reader.snapshot(ctx, repo.Prefix)
	if err != nil || view.end != 1 {
		t.Fatal("racing publication", view, err)
	}
	publish(2)
	if _, err := reader.snapshot(ctx, repo.Prefix); err == nil {
		t.Fatal("accepted a missing published batch")
	}
}
