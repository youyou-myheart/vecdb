package vector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func replayFixture(t testing.TB, count int) (*graphFixture, []GraphEntry) {
	t.Helper()
	f := newGraphFixture(t)
	a := f.services[0]
	var entries []GraphEntry
	for n := 0; n < count; n++ {
		f.write(t, fmt.Sprint(n), false)
		if err := a.Sync(f.ctx); err != nil {
			t.Fatal(err)
		}
		entry, err := a.graphReplica.journal.Entry(f.ctx, uint64(n+1))
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	return f, entries
}

type latencyReplayBlobs struct{ blobAPI }

func (b latencyReplayBlobs) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	return b.blobAPI.Get(ctx, bucket, key)
}

// Same real native graph patches and checksummed objects on both paths; only
// the object store's one-millisecond latency is synthetic. Native onebox is
// measured separately, so this benchmark diagnoses replay concurrency only.
func BenchmarkGraphReplayBacklog(b *testing.B) {
	f, entries := replayFixture(b, 32)
	f.objects.Client = latencyReplayBlobs{f.objects.Client}
	target := entries[len(entries)-1].Position
	for _, serial := range []bool{true, false} {
		b.Run(fmt.Sprintf("serial_%v", serial), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				idx, err := NewHNSWIndex(2, DefaultIndexConfig())
				if err != nil {
					b.Fatal(err)
				}
				g := &graphReplica{journal: f.services[1].graphReplica.journal, repo: f.r, objects: f.objects}
				s := &Service{Index: idx, graphReplica: g}
				b.StartTimer()
				if serial {
					for seq := uint64(1); seq <= target.Sequence; seq++ {
						entry, e := g.journal.Entry(f.ctx, seq)
						if e != nil {
							b.Fatal(e)
						}
						delta, state, e := g.loadEntry(f.ctx, entry)
						if e != nil {
							b.Fatal(e)
						}
						if e = g.apply(s, delta, state, entry.Position); e != nil {
							b.Fatal(e)
						}
					}
				} else if err = g.catchUp(f.ctx, s, target); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if g.position != target || len(idx.Snapshot()) != 32 {
					b.Fatal("incomplete replay")
				}
				idx.graph.Close()
				b.StartTimer()
			}
		})
	}
}

type replayTestBlobs struct {
	blobAPI
	blocked, failed    string
	entered, second    chan struct{}
	release            chan struct{}
	active, peak, gets atomic.Int64
}

func (b *replayTestBlobs) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	b.gets.Add(1)
	n := b.active.Add(1)
	defer b.active.Add(-1)
	for old := b.peak.Load(); n > old; old = b.peak.Load() {
		if b.peak.CompareAndSwap(old, n) {
			break
		}
	}
	if key == b.blocked {
		select {
		case b.entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.release:
		}
	} else if b.second != nil {
		select {
		case b.second <- struct{}{}:
		default:
		}
	}
	if key == b.failed {
		return nil, injected
	}
	return b.blobAPI.Get(ctx, bucket, key)
}

func TestGraphReplayParallelFetchOrderedApplyAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			f, entries := replayFixture(t, 9)
			follower := f.services[1]
			blobs := &replayTestBlobs{blobAPI: f.objects.Client, blocked: entries[0].Object.Key,
				entered: make(chan struct{}, 1), second: make(chan struct{}, 16), release: make(chan struct{})}
			f.objects.Client = blobs
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- follower.Sync(ctx) }()
			receiveConsumer(t, blobs.entered)
			receiveConsumer(t, blobs.second) // Cannot happen with serial downloads.
			hits, pos, err := follower.searchGraphLocal(f.ctx, Search{Vector: []float64{1, 2}, K: 10})
			if err != nil || pos.Sequence != 0 || len(hits) != 0 {
				t.Fatal("out-of-order visibility", hits, pos, err)
			}
			if canceled {
				cancel()
			} else {
				close(blobs.release)
			}
			err = receiveConsumer(t, done)
			if canceled {
				if !errors.Is(err, context.Canceled) || follower.Cursor() != 0 {
					t.Fatal(err, follower.Cursor())
				}
				close(blobs.release)
			} else if err != nil {
				t.Fatal(err)
			}
			if blobs.active.Load() != 0 || blobs.peak.Load() > graphReplayConcurrency {
				t.Fatal("loader leak or unbounded concurrency", blobs.active.Load(), blobs.peak.Load())
			}
			if err = follower.Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			equalGraph(t, f.services[0].Index, follower.Index)
		})
	}
}

func TestGraphReplayFailureRetainsOnlyContinuousPrefix(t *testing.T) {
	f, entries := replayFixture(t, 5)
	follower := f.services[1]
	blobs := &replayTestBlobs{blobAPI: f.objects.Client, failed: entries[1].Object.Key}
	f.objects.Client = blobs
	if err := follower.Sync(f.ctx); !errors.Is(err, injected) || follower.Cursor() != 1 {
		t.Fatal(err, follower.Cursor())
	}
	if blobs.active.Load() != 0 {
		t.Fatal("loader leak")
	}
	blobs.failed = ""
	if err := follower.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, f.services[0].Index, follower.Index)
}

func TestGraphReplayBatchMetadataBoundsAndChainValidation(t *testing.T) {
	for _, scenario := range []string{"count", "bytes", "oversized", "corrupt", "gap", "target"} {
		t.Run(scenario, func(t *testing.T) {
			r, db := newFakeRepo()
			j := graphJournal{c: &Coordinator{root: r}, id: "index-0000"}
			previous := GraphPosition{}
			for n := 1; n <= 20; n++ {
				size := int64(100)
				if scenario == "bytes" {
					size = graphReplayBytes / 2
				}
				if scenario == "oversized" {
					size = graphReplayBytes + 1
				}
				entry := GraphEntry{Previous: previous, Position: GraphPosition{Sequence: uint64(n), DataOffset: uint64(n)}, Kind: "delta", Object: Manifest{Size: size}}
				entry.Position.Hash = entryHash(entry)
				previous = entry.Position
				if scenario == "corrupt" && n == 3 {
					entry.Object.Size++
				}
				if scenario == "gap" && n == 3 {
					continue
				}
				seed(db, j.key(fmt.Sprintf("entries/%020d", n)), entry)
			}
			seed(db, j.key("head"), GraphHead{Committed: previous, Visible: previous})
			begin := r.beginTxn
			calls := 0
			r.beginTxn = func() (transactionAPI, error) { tx, e := begin(); return countedBatchGetTx{tx, &calls}, e }
			target := previous
			if scenario == "target" {
				target.Sequence = 1
				target.Hash = "wrong"
			}
			entries, err := j.replayEntries(context.Background(), GraphPosition{}, target)
			if scenario == "corrupt" || scenario == "gap" || scenario == "target" {
				if err == nil {
					t.Fatal("invalid chain accepted")
				}
				return
			}
			want := graphReplayEntries
			if scenario == "bytes" {
				want = 2
			}
			if scenario == "oversized" {
				want = 1
			}
			if err != nil || len(entries) != want || calls != 1 {
				t.Fatal(len(entries), err, calls)
			}
		})
	}
}

func TestRoutedLeaderReusesVerifiedBuiltDelta(t *testing.T) {
	f, _, owner := routedFixture(t)
	f.write(t, "v", false)
	g := f.services[0].graphReplica
	if err := g.enqueue(f.ctx, deliveryFor(t, f, owner, 0)); err != nil {
		t.Fatal(err)
	}
	blobs := &replayTestBlobs{blobAPI: f.objects.Client}
	f.objects.Client = blobs
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if blobs.gets.Load() != 1 {
		t.Fatal("leader redownloaded its verified delta", blobs.gets.Load())
	}
	if err := f.services[1].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if blobs.gets.Load() != 2 {
		t.Fatal("follower must verify persisted object", blobs.gets.Load())
	}
	equalGraph(t, f.services[0].Index, f.services[1].Index)
}
