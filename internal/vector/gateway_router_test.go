package vector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type routeTransport func(*http.Request) (*http.Response, error)

func (f routeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestReplicaRouteOrderAndMaintenanceNoRetry(t *testing.T) {
	ctx := context.Background()
	root, db := newFakeRepo()
	db.nowMillis = 1000
	coord := &Coordinator{root: root}
	id := IndexPartitionID("index-0000")
	plan := ReplicaPlan{Publisher: "replica-a", Serving: []Assignment{{Partition: "replica-a"}, {Partition: "replica-b"}, {Partition: "replica-c"}}}
	raw, _ := json.Marshal(plan)
	db.data[string(coord.planKey(id))] = raw
	for _, a := range plan.Serving {
		raw, _ = json.Marshal(Ownership{Partition: a.Partition, Node: string(a.Partition), URL: "http://" + string(a.Partition), Ready: true, Expires: 10000})
		db.data[string(coord.ownerKey(a.Partition))] = raw
	}
	var visited []string
	fail := false
	client := &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		visited = append(visited, r.URL.Host)
		if fail {
			return nil, errors.New("response lost")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
	})}
	p := &remotePartition{id: id, cluster: &ClusterService{root: root, coordinator: coord, replicas: 2, client: client}}
	for _, op := range []string{"search", "sync", "search", "search", "checkpoint"} {
		if _, err := p.replicaCall(ctx, op, Search{}); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"replica-b", "replica-a", "replica-c", "replica-a", "replica-a"}
	if !reflect.DeepEqual(visited, want) {
		t.Fatalf("got %v want %v", visited, want)
	}
	fail = true
	visited = nil
	if _, err := p.replicaCall(ctx, "checkpoint", Search{}); err == nil {
		t.Fatal("lost response accepted")
	}
	if !reflect.DeepEqual(visited, []string{"replica-a"}) {
		t.Fatalf("maintenance retried: %v", visited)
	}
	visited = nil
	if _, err := p.replicaCall(ctx, "search", Search{}); err == nil {
		t.Fatal("all replicas failed")
	}
	want = []string{"replica-b", "replica-c", "replica-a", "replica-c", "replica-a", "replica-b"}
	if !reflect.DeepEqual(visited, want) {
		t.Fatalf("retry order %v", visited)
	}
}

func TestGraphRouteWaitsForStableOwnership(t *testing.T) {
	root, db := newFakeRepo()
	db.nowMillis = 1000
	coord := &Coordinator{root: root}
	id := IndexPartitionID("index-0000")
	raw, _ := json.Marshal(ReplicaPlan{Publisher: "r", Serving: []Assignment{{Partition: "r"}}})
	db.data[string(coord.planKey(id))] = raw
	raw, _ = json.Marshal(Ownership{Partition: "r", Node: "worker", Ready: true, Expires: 10000, URL: "http://worker"})
	db.data[string(coord.ownerKey("r"))] = raw
	calls := 0
	alwaysFail := false
	client := &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		code := 200
		body := "{}"
		if alwaysFail || calls <= 2 {
			code = 503
			body = `{"code":"fenced"}`
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	p := &remotePartition{id: id, cluster: &ClusterService{graphReplication: true, root: root, coordinator: coord, replicas: 2, client: client}}
	if _, e := p.replicaCall(context.Background(), "search", Search{}); e != nil || calls != 3 {
		t.Fatal(calls, e)
	}
	alwaysFail = true
	calls = 0
	if _, e := p.replicaCall(context.Background(), "checkpoint", Search{}); e == nil || calls != 1 {
		t.Fatal("maintenance retried", calls, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := p.replicaCall(ctx, "search", Search{}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
