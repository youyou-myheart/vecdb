package vector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLightSearchRoutesUseScansAndReturnDetachedHints(t *testing.T) {
	f := newGraphFixture(t)
	c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, PartitionedService: &PartitionedService{layout: f.layout}}
	base := f.r.beginTxn
	begins := 0
	reads := map[string]int{}
	f.r.beginTxn = func() (transactionAPI, error) { begins++; tx, err := base(); return &countedScheduleTx{tx, reads}, err }
	state, err := c.SearchRouting(f.ctx)
	if err != nil || len(state.Nodes) != 3 || len(state.ReplicaOwners["index-0000"].Owners) != 2 {
		t.Fatal(state, err)
	}
	if begins != 1 || len(reads) != 0 || len(state.DataProgress) != 0 || len(state.GraphHeads) != 0 {
		t.Fatal("heavy status IO", begins, reads)
	}
	state.Nodes[0].Session = "changed"
	state.ReplicaOwners["index-0000"].Owners[0].URL = "http://changed"
	f.r.beginTxn = func() (transactionAPI, error) { panic("cache hit performed storage IO") }
	again, err := c.SearchRouting(f.ctx)
	if err != nil || again.Nodes[0].Session == "changed" || again.ReplicaOwners["index-0000"].Owners[0].URL == "http://changed" {
		t.Fatal(again, err)
	}
}

func TestCachedSearchTriesAlternateWithoutMetadataReads(t *testing.T) {
	f := newGraphFixture(t)
	c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, graphReplication: true, PartitionedService: &PartitionedService{layout: f.layout}}
	c.coordinator.leaseReads = true
	if _, err := c.cachedSearchRoutes(f.ctx, false); err != nil {
		t.Fatal(err)
	}
	f.r.beginTxn = func() (transactionAPI, error) { panic("cached query performed metadata IO") }
	visited := 0
	c.client = &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		visited++
		code, body := 200, `{"hits":[{"id":"a"}]}`
		if visited == 1 {
			code, body = 503, `{"code":"fenced"}`
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	p := &remotePartition{cluster: c, id: "index-0000"}
	if reply, err := p.replicaCall(f.ctx, "search", Search{}); err != nil || len(reply.Hits) != 1 || visited != 2 {
		t.Fatal(reply, err, visited)
	}
}

func TestRouteRefreshWaitHonorsCancellation(t *testing.T) {
	c := &ClusterService{}
	c.searchRoutes.refresh = make(chan struct{}, 1)
	c.searchRoutes.refresh <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if _, err := c.cachedSearchRoutes(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("refresh wait ignored cancellation")
	}
}

func TestCachedSearchMetadataFailureIsUnavailable(t *testing.T) {
	r, _ := newFakeRepo()
	r.beginTxn = func() (transactionAPI, error) { return nil, injected }
	c := &ClusterService{root: r, coordinator: &Coordinator{leaseReads: true}, replicas: 2, graphReplication: true}
	p := &remotePartition{cluster: c, id: "index-0000"}
	if _, err := p.replicaCall(context.Background(), "search", Search{}); !errors.Is(err, ErrPartitionUnavailable) || !errors.Is(err, injected) {
		t.Fatal(err)
	}
}
