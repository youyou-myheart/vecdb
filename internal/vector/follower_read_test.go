package vector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func followerFixture(t *testing.T) (*graphFixture, *ClusterService, *Service, *Service, *atomic.Int64) {
	t.Helper()
	f, c, owner := leasedFixture(t)
	a, b := f.services[0], f.services[1]
	c.replicas, c.graphReplication, c.token = 2, true, "follower-unit-token"
	c.locals = map[IndexPartitionID]*localPartition{}
	for _, s := range f.services {
		o := s.graphReplica.journal.owner
		c.locals[o.Partition] = &localPartition{owner: o, service: s}
	}
	count := &atomic.Int64{}
	handler := c.InternalHandler()
	c.client = &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "unexpected") {
			t.Error("wrong destination")
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Result(), nil
	})}
	f.write(t, "point", false)
	if err := a.graphReplica.enqueue(f.ctx, deliveryFor(t, f, owner, 0)); err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	// Count actual leader capture RPCs without any query/Sync intervention.
	original := c.client.Transport
	c.client.Transport = routeTransport(func(r *http.Request) (*http.Response, error) {
		body, err := r.GetBody()
		if err != nil {
			return nil, err
		}
		var rpc partitionRPC
		err = json.NewDecoder(body).Decode(&rpc)
		body.Close()
		if err != nil {
			return nil, err
		}
		if rpc.Operation == "applied_position" {
			count.Add(1)
		}
		return original.RoundTrip(r)
	})
	t.Cleanup(func() {
		for _, s := range f.services {
			g := s.graphReplica
			g.viewMu.RLock()
			q := g.followerReads
			g.viewMu.RUnlock()
			if q != nil {
				q.close()
			}
		}
	})
	return f, c, a, b, count
}

func TestFollowerReadWaitsForApplyAndReturnsDiagnostics(t *testing.T) {
	f, c, a, b, captures := followerFixture(t)
	q := Search{Vector: []float64{1, 2}, K: 1, Exact: true}
	target, err := a.graphReplica.leaderApplied()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	type outcome struct {
		result FollowerSearchResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, e := c.FollowerSearch(ctx, q); done <- outcome{r, e} }()
	queue := b.graphReplica.readQueue()
	waitFollowerState(t, queue, func(q *followerReadQueue) bool {
		for r := range q.pending {
			if r.target != nil {
				return true
			}
		}
		return false
	})
	select {
	case r := <-done:
		t.Fatal("read completed before follower apply", r)
	default:
	}
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || len(r.result.Hits) != 1 || r.result.Hits[0].ID != "point" || len(r.result.FollowerReads) != 1 {
		t.Fatal(r)
	}
	info := r.result.FollowerReads[0]
	if info.Target != target || info.Applied.Sequence < target.Sequence || info.Replica != b.graphReplica.journal.owner.Partition || info.WaitApplyMS <= 0 || captures.Load() != 1 {
		t.Fatal(info, captures.Load())
	}
	metrics, err := c.LocalMetrics()
	if err != nil || !strings.Contains(metrics, "vecdb_follower_read_pending") || !strings.Contains(metrics, `stage="follower_wait_apply"`) {
		t.Fatal(metrics, err)
	}
}

func TestFollowerReadTimeoutDoesNotBlockLaterRead(t *testing.T) {
	f, c, _, b, _ := followerFixture(t)
	query := Search{Vector: []float64{1, 2}, K: 1}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(f.ctx, 15*time.Millisecond)
		_, err := c.FollowerSearch(ctx, query)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	queue := b.graphReplica.readQueue()
	waitFollowerState(t, queue, func(q *followerReadQueue) bool { return len(q.pending) == 0 })
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	r, err := c.FollowerSearch(f.ctx, query)
	if err != nil || len(r.Hits) != 1 {
		t.Fatal(r, err)
	}
}

func TestFollowerLeaderCaptureUsesPublishedViewAndLeaseWithoutKV(t *testing.T) {
	f, _, a, b, _ := followerFixture(t)
	g := a.graphReplica
	want := g.position
	g.writer.mu.Lock()
	g.writer.head.Committed.Sequence += 100
	g.writer.mu.Unlock()
	f.r.beginTxn = func() (transactionAPI, error) { t.Error("leader applied lookup accessed KV"); return nil, injected }
	got, err := g.leaderApplied()
	if err != nil || got != want {
		t.Fatal(got, want, err)
	}
	if _, err = b.graphReplica.leaderApplied(); !errors.Is(err, ErrFenced) {
		t.Fatal("follower issued leader barrier", err)
	}
	if err = g.followerAuthority(); !errors.Is(err, ErrFenced) {
		t.Fatal("leader accepted follower read", err)
	}
	g.writer.leaderLease.Retire()
	if _, err = g.leaderApplied(); !errors.Is(err, ErrFenced) {
		t.Fatal("retired leader issued barrier", err)
	}
}

func TestFollowerReadRejectsPromotionWhileWaiting(t *testing.T) {
	f, c, a, b, _ := followerFixture(t)
	o := b.graphReplica.journal.owner
	done := make(chan error, 1)
	go func() {
		_, err := c.execute(f.ctx, partitionRPC{Namespace: f.r.Prefix, Partition: o.Partition, Epoch: o.Epoch, Operation: "follower_search", Query: Search{Vector: []float64{1, 2}, K: 1}})
		done <- err
	}()
	queue := b.graphReplica.readQueue()
	waitFollowerState(t, queue, func(q *followerReadQueue) bool {
		for r := range q.pending {
			if r.target != nil {
				return true
			}
		}
		return false
	})
	b.graphReplica.writer.mu.Lock()
	b.graphReplica.writer.publisher = o.Partition
	b.graphReplica.writer.mu.Unlock()
	queue.applied(a.graphReplica.position)
	if err := <-done; !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
}

func TestFollowerRouteNeverFallsBackToPublisher(t *testing.T) {
	f, c, _, _, _ := followerFixture(t)
	plan, err := f.c.Plan(f.ctx, "index-0000")
	if err != nil {
		t.Fatal(err)
	}
	c.client.Transport = routeTransport(func(r *http.Request) (*http.Response, error) {
		var req partitionRPC
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Operation != "follower_search" || req.Partition == plan.Publisher {
			t.Error("routed query to leader", req)
		}
		return nil, errors.New("follower unreachable")
	})
	if _, err = c.FollowerSearch(f.ctx, Search{Vector: []float64{1, 2}, K: 1}); err == nil {
		t.Fatal("unavailable follower was hidden")
	}
}
