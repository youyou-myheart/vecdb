package vector

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func rawWindowTestGroup(t *testing.T) (context.Context, *mutationGroup, *fakeRawData) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	client := newFakeRawData()
	store := newRawDataStore(client, rawTestNamespace)
	repo, owner := rawTestRepo(0), rawTestOwner(0, 1)
	g := newMutationGroup(ctx, repo, &pipelineMetrics{}, string(owner.Partition))
	g.lease = &appendLease{owner: owner}
	confirmAppendGrant(t, g.lease, owner.Epoch, 1<<60, time.Minute)
	g.rawWriter = rawTestRecover(t, ctx, store, repo, owner)
	t.Cleanup(func() { cancel(); g.close() })
	return ctx, g, client
}

func submitRawWindow(ctx context.Context, g *mutationGroup, id, payload string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := g.mutate(ctx, id, Mutation{RequestID: id, Vector: []float64{1, 2}, Payload: map[string]string{"value": payload}}, false)
		done <- err
	}()
	return done
}

func waitRawWindowQueue(t *testing.T, g *mutationGroup, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		n := len(g.queue)
		g.mu.Unlock()
		if n == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("wanted %d queued requests, got %d", count, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRawWindowExpandsForFullQueuesWithoutEarlyACK(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprint("large=", large), func(t *testing.T) {
			ctx, g, client := rawWindowTestGroup(t)
			type call struct {
				count   int
				release chan struct{}
			}
			entered := make(chan call, 4)
			stored := make(chan struct{}, 4)
			client.beforeBatch = func(ctx context.Context, keys, _ [][]byte) (int, error) {
				c := call{len(keys), make(chan struct{})}
				entered <- c
				select {
				case <-c.release:
					return len(keys), nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			client.afterBatch = func([][]byte) { stored <- struct{}{} }
			var calls []call
			var results []<-chan error
			for n := 0; n < rawSmallWritesInFlight; n++ {
				results = append(results, submitRawWindow(ctx, g, fmt.Sprint("small-", n), ""))
				c := receiveConsumer(t, entered)
				if c.count != 1 {
					t.Fatal("small request did not start immediately", c.count)
				}
				calls = append(calls, c)
			}
			count, payload := mutationGroupRecords, ""
			if large {
				count, payload = 2, strings.Repeat("x", mutationGroupBytes/3)
			}
			for n := 0; n < count-1; n++ {
				results = append(results, submitRawWindow(ctx, g, fmt.Sprint("batch-", n), payload))
			}
			waitRawWindowQueue(t, g, count-1)
			select {
			case <-entered:
				t.Fatal("another small batch bypassed batching")
			case <-time.After(10 * time.Millisecond):
			}
			results = append(results, submitRawWindow(ctx, g, "last", payload))
			full := receiveConsumer(t, entered)
			if full.count != count {
				t.Fatal("full queue did not wake the writer", full.count, count)
			}
			close(full.release)
			receiveConsumer(t, stored)
			select {
			case err := <-results[len(results)-1]:
				t.Fatal("durable suffix bypassed the missing prefix", err)
			case <-time.After(10 * time.Millisecond):
			}
			for _, c := range calls {
				close(c.release)
			}
			for _, done := range results {
				if err := receiveConsumer(t, done); err != nil {
					t.Fatal(err)
				}
			}
			if got := g.lease.head.Load(); got != uint64(len(results)) {
				t.Fatal("wrong accepted prefix", got)
			}
		})
	}
}

func TestRawWindowFlushesSmallQueueAfterPrefixCompletes(t *testing.T) {
	ctx, g, client := rawWindowTestGroup(t)
	entered := make(chan chan struct{}, 4)
	client.beforeBatch = func(ctx context.Context, keys, _ [][]byte) (int, error) {
		release := make(chan struct{})
		entered <- release
		select {
		case <-release:
			return len(keys), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	var releases []chan struct{}
	var results []<-chan error
	for n := 0; n < rawSmallWritesInFlight; n++ {
		results = append(results, submitRawWindow(ctx, g, fmt.Sprint(n), ""))
		releases = append(releases, receiveConsumer(t, entered))
	}
	results = append(results, submitRawWindow(ctx, g, "queued", ""))
	waitRawWindowQueue(t, g, 1)
	close(releases[0])
	// Only one earlier write completes. A partial queue must now flush even
	// while the other earlier write remains blocked; no new arrival is needed.
	release := receiveConsumer(t, entered)
	close(release)
	for _, release := range releases[1:] {
		close(release)
	}
	for _, done := range results {
		if err := receiveConsumer(t, done); err != nil {
			t.Fatal(err)
		}
	}
}
