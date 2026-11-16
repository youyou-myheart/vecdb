package vector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testRoutePlan(version uint64) ReplicaPlan {
	return ReplicaPlan{Version: version, Serving: []Assignment{{Partition: "replica"}}}
}

func TestRoutePlanTimeoutUsesLastHint(t *testing.T) {
	var cache routePlanCache
	ctx := context.Background()
	if _, fallback, err := cache.read(ctx, func(context.Context) (ReplicaPlan, error) { return testRoutePlan(2), nil }); err != nil || fallback {
		t.Fatal(fallback, err)
	}
	plan, fallback, err := cache.read(ctx, func(ctx context.Context) (ReplicaPlan, error) {
		if _, bounded := ctx.Deadline(); !bounded {
			t.Fatal("cached plan read was not bounded")
		}
		<-ctx.Done()
		return ReplicaPlan{}, ctx.Err()
	})
	if err != nil || !fallback || plan.Version != 2 {
		t.Fatal(plan, fallback, err)
	}
	// A successful refresh replaces the hint; it is not a fixed route.
	_, _, _ = cache.read(ctx, func(context.Context) (ReplicaPlan, error) { return testRoutePlan(3), nil })
	if plan, ok := cache.hint(); !ok || plan.Version != 3 {
		t.Fatal(plan, ok)
	}
}

func TestRoutePlanRejectsInvalidHintsAndNonTimeoutErrors(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "cancelled", "empty"} {
		t.Run(kind, func(t *testing.T) {
			cache := routePlanCache{plan: testRoutePlan(2), readAt: time.Now()}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			expected := error(context.DeadlineExceeded)
			switch kind {
			case "missing":
				expected = ErrNotFound
			case "corrupt":
				expected = injected
			case "cancelled":
				cancel()
				expected = context.Canceled
			case "empty":
				_, _, _ = cache.read(ctx, func(context.Context) (ReplicaPlan, error) { return ReplicaPlan{Version: 3}, nil })
			}
			_, fallback, err := cache.read(ctx, func(context.Context) (ReplicaPlan, error) { return ReplicaPlan{}, expected })
			if fallback || !errors.Is(err, expected) {
				t.Fatal(fallback, err)
			}
			if kind != "cancelled" {
				if _, valid := cache.hint(); valid {
					t.Fatal("invalid hint retained")
				}
			}
		})
	}
}

func TestRoutePlanConcurrentRefreshDoesNotRegress(t *testing.T) {
	var cache routePlanCache
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = cache.read(context.Background(), func(context.Context) (ReplicaPlan, error) {
			close(started)
			<-release
			return testRoutePlan(1), nil
		})
	}()
	<-started
	_, _, _ = cache.read(context.Background(), func(context.Context) (ReplicaPlan, error) { return testRoutePlan(2), nil })
	close(release)
	<-done
	if plan, ok := cache.hint(); !ok || plan.Version != 2 {
		t.Fatal(plan, ok)
	}
}

type blockedRoutePlanTx struct {
	transactionAPI
	key string
}

func (t blockedRoutePlanTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	if string(key) == t.key {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return t.transactionAPI.Get(ctx, key)
}

func TestRoutePlanFallbackStillChecksOwnerAndNeverAuthorizesMaintenance(t *testing.T) {
	root, db := newFakeRepo()
	db.nowMillis = 1000
	coord := &Coordinator{root: root}
	id := IndexPartitionID("index-0000")
	plan := ReplicaPlan{Version: 2, Publisher: "a", Serving: []Assignment{{Partition: "a"}, {Partition: "b"}}}
	raw, _ := json.Marshal(plan)
	db.data[string(coord.planKey(id))] = raw
	for _, a := range plan.Serving {
		raw, _ = json.Marshal(Ownership{Partition: a.Partition, Node: string(a.Partition), Epoch: 3, Ready: true, Expires: 10000, URL: "http://" + string(a.Partition)})
		db.data[string(coord.ownerKey(a.Partition))] = raw
	}
	var visited []string
	client := &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		visited = append(visited, r.URL.Host)
		var req partitionRPC
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Epoch != 3 {
			t.Fatal(req, err)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
	})}
	p := &remotePartition{id: id, cluster: &ClusterService{root: root, coordinator: coord, replicas: 2, graphReplication: true, client: client}}
	if _, err := p.replicaCall(context.Background(), "search", Search{}); err != nil {
		t.Fatal(err)
	}
	// Address hints have no authority TTL of their own: even a long-idle hint
	// must route through fresh Resolve/epoch checks, as verified below.
	p.plans.readAt = time.Now().Add(-24 * time.Hour)
	original := root.beginTxn
	root.beginTxn = func() (transactionAPI, error) {
		tx, err := original()
		return blockedRoutePlanTx{tx, string(coord.planKey(id))}, err
	}
	// The hint still lists b, but authoritative ownership has been revoked.
	delete(db.data, string(coord.ownerKey("b")))
	p.rotation.Store(0)
	visited = nil
	if _, err := p.replicaCall(context.Background(), "search", Search{}); err != nil || len(visited) != 1 || visited[0] != "a" {
		t.Fatal(visited, err)
	}
	for _, op := range []string{"sync", "checkpoint", "prune", "rebuild", "search"} {
		if op == "search" {
			delete(db.data, string(coord.ownerKey("a")))
		}
		visited = nil
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		_, err := p.replicaCall(ctx, op, Search{})
		cancel()
		if err == nil || len(visited) != 0 {
			t.Fatal("cached plan bypassed authority", op, visited, err)
		}
	}
}
