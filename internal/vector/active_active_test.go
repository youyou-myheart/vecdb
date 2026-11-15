package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFailureDomainPlacement(t *testing.T) {
	workers := []ClusterNode{{ID: "a", Session: "s", HostID: "h1", Zone: "z1"}, {ID: "b", Session: "s", HostID: "h1", Zone: "z2"}, {ID: "c", Session: "s", HostID: "h2", Zone: "z1"}, {ID: "d", Session: "s", HostID: "h3", Zone: "z2"}}
	layout, _ := newLayout(32)
	for _, domain := range []string{"host", "zone"} {
		snap := PlacementSnapshot{FailureDomain: domain, Workers: workers, Plans: map[IndexPartitionID]ReplicaPlan{}, Owners: map[IndexPartitionID]Ownership{}}
		d := planPlacement(layout, snap, 2)
		for _, b := range layout.Bindings {
			for _, targets := range [][]ClusterNode{d.Targets[b.Index], replicaChoices(b.Index, workers, 2, domain)} {
				if len(targets) != 2 || !canPlaceReplica(targets[:1], targets[1], domain) {
					t.Fatal(domain, b.Index, targets)
				}
			}
		}
		snap.Workers = workers[:2]
		d = planPlacement(layout, snap, 2)
		for _, targets := range d.Targets {
			if len(targets) != 1 {
				t.Fatal("same physical host accepted", targets)
			}
		}
		if len(replicaChoices("index-0000", workers[:2], 2, domain)) != 1 {
			t.Fatal("hash placement colocated replicas")
		}
	}
	if canPlaceReplica(nil, ClusterNode{ID: "missing"}, "host") {
		t.Fatal("missing host accepted")
	}
	// A voluntary move may replace a replica within its own host, but cannot
	// move it onto the other replica's physical host.
	targets := []ClusterNode{workers[0], workers[2]}
	if !canReplaceReplica(targets, workers[1], "host", 0) || canReplaceReplica(targets, workers[1], "host", 1) {
		t.Fatal("replacement domain check")
	}
}
func TestDomainProtocolAndImmutableLabels(t *testing.T) {
	ctx := context.Background()
	root, _ := newFakeRepo()
	if e := root.ensureTopologyMode(ctx, "load", "host"); e != nil {
		t.Fatal(e)
	}
	if e := root.ensureLoadMode(ctx); e == nil {
		t.Fatal("old protocol accepted host namespace")
	}
	if e := root.ensureTopologyMode(ctx, "load", "zone"); e == nil {
		t.Fatal("mixed domains accepted")
	}
	c := &Coordinator{root: root, ttl: 6000}
	n := ClusterNode{ID: "a", Session: "s", HostID: "h1", URL: "http://a", Roles: []string{RoleWorker}}
	if e := c.Heartbeat(ctx, n); e != nil {
		t.Fatal(e)
	}
	n.HostID = "h2"
	if e := c.Heartbeat(ctx, n); e == nil {
		t.Fatal("session changed physical host")
	}
}
func TestHotReplicaWatermarkAndCircuit(t *testing.T) {
	ctx := context.Background()
	root, db := newFakeRepo()
	db.nowMillis = 1000
	coord := &Coordinator{root: root, failureDomain: "host"}
	layout, _ := newLayout(1)
	id := layout.Bindings[0].Index
	c := &ClusterService{root: root, coordinator: coord, replicas: 2, PartitionedService: &PartitionedService{layout: layout, data: map[DataPartitionID]*Repository{layout.Bindings[0].Data: root}}}
	plan := ReplicaPlan{Serving: []Assignment{{Partition: "a"}, {Partition: "b"}}}
	raw, _ := json.Marshal(plan)
	db.data[string(coord.planKey(id))] = raw
	db.data[root.Prefix+"log-head"] = []byte("7")
	for _, name := range []string{"a", "b"} {
		raw, _ = json.Marshal(Ownership{Partition: IndexPartitionID(name), Node: name, Session: "s", URL: "http://" + name, Ready: true, Expires: 10000})
		db.data[string(coord.ownerKey(IndexPartitionID(name)))] = raw
	}
	calls := []string{}
	through := []uint64{}
	c.client = &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		var req partitionRPC
		_ = json.NewDecoder(r.Body).Decode(&req)
		calls = append(calls, r.URL.Host)
		through = append(through, req.ReadThrough)
		if r.URL.Host == "b" {
			db.data[root.Prefix+"log-head"] = []byte("8")
			return nil, errors.New("host disconnected")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"applied_offset":%d}`, req.ReadThrough))), Header: make(http.Header)}, nil
	})}
	p := &remotePartition{cluster: c, id: id}
	if _, e := p.replicaCall(ctx, "search", Search{}); e != nil {
		t.Fatal(e)
	}
	if strings.Join(calls, ",") != "b,a" || through[0] != 7 || through[1] != 7 {
		t.Fatal("retry changed query watermark", calls, through)
	}
	for i := 0; i < 4; i++ {
		if _, e := p.replicaCall(ctx, "search", Search{}); e != nil {
			t.Fatal(e)
		}
	}
	for _, host := range calls[2:] {
		if host != "a" {
			t.Fatal("failed host was repeatedly retried", calls)
		}
	}
	if c.routeCooling(Ownership{Node: "b", Session: "new"}) {
		t.Fatal("old session circuit affected replacement")
	}
	c.routingMu.Lock()
	c.unhealthy["b/s"] = time.Now().Add(-time.Second)
	c.routingMu.Unlock()
	if c.routeCooling(Ownership{Node: "b", Session: "s"}) {
		t.Fatal("expired circuit never recovered")
	}
}
func TestHotReplicaRejectsStaleSuccessfulResponse(t *testing.T) {
	c := &ClusterService{root: &Repository{Prefix: "test/"}, client: &http.Client{Transport: routeTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"applied_offset":6}`)), Header: make(http.Header)}, nil
	})}}
	p := &remotePartition{cluster: c}
	if _, e := p.callOwnerThrough(context.Background(), "search", Search{}, Ownership{Node: "remote", URL: "http://remote"}, 7); !errors.Is(e, ErrNotVisible) {
		t.Fatal("stale success accepted", e)
	}
}
func TestWorkerCatchesUpBeforeServingWatermark(t *testing.T) {
	ctx := context.Background()
	root, _ := newFakeRepo()
	coord := &Coordinator{root: root, ttl: 60000}
	node := ClusterNode{ID: "a", Session: "s", HostID: "h1", URL: "http://a", Roles: []string{RoleWorker}}
	_ = coord.Heartbeat(ctx, node)
	testAssignment(t, coord, node, "index-0000")
	owner, e := coord.Acquire(ctx, "index-0000", node)
	if e != nil {
		t.Fatal(e)
	}
	service, e := NewService(ctx, root, partitionObjects())
	if e != nil {
		t.Fatal(e)
	}
	defer service.Index.graph.Close()
	owner, e = coord.Update(ctx, owner, true, false)
	if e != nil {
		t.Fatal(e)
	}
	c := &ClusterService{root: root, coordinator: coord, replicas: 2, locals: map[IndexPartitionID]*localPartition{owner.Partition: {owner: owner, service: service}}}
	putLogRecords(t, root, 3)
	if service.Cursor() != 0 {
		t.Fatal("test replica was not stale")
	}
	req := partitionRPC{Namespace: root.Prefix, Partition: owner.Partition, Epoch: owner.Epoch, Operation: "search", ReadThrough: 3, Query: Search{Vector: []float64{0, 0}, K: 10, Exact: true}}
	reply, e := c.execute(ctx, req)
	if e != nil || reply.AppliedOffset < 3 || len(reply.Hits) != 3 {
		t.Fatal(reply, e)
	}
	_, e = root.Mutate(ctx, "0", Mutation{RequestID: "delete-0"}, true)
	if e != nil {
		t.Fatal(e)
	}
	req.ReadThrough = 4
	reply, e = c.execute(ctx, req)
	if e != nil || len(reply.Hits) != 2 {
		t.Fatal(reply, e)
	}
	req.ReadThrough = 5
	if _, e = c.execute(ctx, req); !errors.Is(e, ErrNotVisible) {
		t.Fatal("unreachable watermark accepted", e)
	}
}
