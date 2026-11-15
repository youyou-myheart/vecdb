package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInternalRPCAuthenticationAndFencing(t *testing.T) {
	root, db := newFakeRepo()
	db.nowMillis = 1000
	ctx := context.Background()
	coord := &Coordinator{root: root, ttl: 6000}
	node := ClusterNode{Roles: []string{RoleWorker}, ID: "a", Session: "a1", URL: "http://localhost:1"}
	if e := coord.Heartbeat(ctx, node); e != nil {
		t.Fatal(e)
	}
	testAssignment(t, coord, node, "index-0000")
	owner, e := coord.Acquire(ctx, "index-0000", node)
	if e != nil {
		t.Fatal(e)
	}
	service, e := NewService(ctx, root, partitionObjects())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = coord.Update(ctx, owner, true, false); e != nil {
		t.Fatal(e)
	}
	c := &ClusterService{coordinator: coord, node: node, root: root, token: "cluster-unit-secret", locals: map[IndexPartitionID]*localPartition{owner.Partition: {owner: owner, service: service}}}
	req := partitionRPC{Namespace: root.Prefix, Partition: owner.Partition, Epoch: owner.Epoch, Operation: "search", Query: Search{Vector: []float64{0, 0}, K: 1}}
	if _, e = c.execute(ctx, req); e != nil {
		t.Fatal(e)
	}
	for _, op := range []string{"sync", "checkpoint", "prune", "rebuild"} {
		r := req
		r.Operation = op
		if _, e = c.execute(ctx, r); e != nil {
			t.Fatal(op, e)
		}
	}
	for _, bad := range []partitionRPC{{Namespace: "other", Partition: owner.Partition, Epoch: owner.Epoch}, {Namespace: root.Prefix, Partition: owner.Partition, Epoch: 2}} {
		if _, e = c.execute(ctx, bad); !errors.Is(e, ErrFenced) {
			t.Fatal(e)
		}
	}
	body, _ := json.Marshal(req)
	for _, tc := range []struct {
		method, auth string
		status       int
	}{{"POST", "", 401}, {"GET", "Bearer cluster-unit-secret", 405}, {"POST", "Bearer cluster-unit-secret", 200}} {
		r := httptest.NewRequest(tc.method, "/internal/partition", bytes.NewReader(body))
		r.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		c.InternalHandler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	db.nowMillis = 8000
	if _, e = c.execute(ctx, req); !errors.Is(e, ErrFenced) {
		t.Fatal("expired owner served", e)
	}
}
func TestRemotePartitionErrors(t *testing.T) {
	root, _ := newFakeRepo()
	coord := &Coordinator{root: root, ttl: 6000}
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"code":"not_visible","error":"not visible"}`))
	}))
	defer server.Close()
	node := ClusterNode{Roles: []string{RoleWorker}, ID: "remote", Session: "s", URL: server.URL}
	if e := coord.Heartbeat(ctx, node); e != nil {
		t.Fatal(e)
	}
	testAssignment(t, coord, node, "index-0000")
	o, e := coord.Acquire(ctx, "index-0000", node)
	if e != nil {
		t.Fatal(e)
	}
	_, e = coord.Update(ctx, o, true, false)
	if e != nil {
		t.Fatal(e)
	}
	c := &ClusterService{root: root, coordinator: coord, node: ClusterNode{Roles: []string{RoleWorker}, ID: "local"}, token: "secret", client: server.Client()}
	p := &remotePartition{cluster: c, id: o.Partition}
	if _, e = p.Search(Search{Vector: []float64{0, 0}, K: 1}); !errors.Is(e, ErrNotVisible) {
		t.Fatal(e)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if e = p.Sync(cancelCtx); e == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestRoleHTTPGate(t *testing.T) {
	public := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	c := &ClusterService{node: ClusterNode{Roles: []string{RoleMaster}}}
	for _, tc := range []struct {
		path   string
		status int
	}{{"/v1/records/a", 403}, {"/v1/search", 403}, {"/v1/cluster", 204}, {"/healthz", 204}, {"/internal/partition", 503}} {
		w := httptest.NewRecorder()
		c.WrapHandler(public).ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status {
			t.Fatal(tc.path, w.Code)
		}
	}
	c.gateway = &Gateway{}
	w := httptest.NewRecorder()
	c.WrapHandler(public).ServeHTTP(w, httptest.NewRequest("GET", "/v1/records/a", nil))
	if w.Code != 204 {
		t.Fatal(w.Code)
	}
}
