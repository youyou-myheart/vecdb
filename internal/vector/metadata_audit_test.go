package vector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAllHintedOperationsAvoidSenderMetadataReads(t *testing.T) {
	for _, op := range []string{"search", "follower_search", "deliver", "sync", "checkpoint", "prune", "rebuild"} {
		t.Run(op, func(t *testing.T) {
			f := newGraphFixture(t)
			c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, cachedRouting: true, PartitionedService: &PartitionedService{layout: f.layout}}
			state, err := c.cachedSearchRoutes(f.ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			publisher := state.ReplicaPlans["index-0000"].Publisher
			f.r.beginTxn = func() (transactionAPI, error) { panic("cache hit performed sender metadata IO") }
			calls := 0
			c.client = &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				var req partitionRPC
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				if req.Operation != op {
					t.Fatal(req.Operation, op)
				}
				if op == "follower_search" && req.Partition == publisher {
					t.Fatal("sent follower query to publisher")
				}
				if op != "search" && op != "follower_search" && req.Partition != publisher {
					t.Fatal("stateful request not sent to publisher")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
			})}
			p := &remotePartition{cluster: c, id: "index-0000"}
			switch op {
			case "follower_search":
				_, err = p.followerCall(f.ctx, Search{})
			case "deliver":
				err = c.deliver(f.ctx, "index-0000", Delivery{})
			default:
				_, err = p.call(f.ctx, op, Search{})
			}
			if err != nil || calls != 1 {
				t.Fatal(err, calls)
			}
		})
	}
}

func TestMaintenanceAdmissionUsesLocalOwnerLease(t *testing.T) {
	e, q := testReadEntry()
	r, _ := newFakeRepo()
	q.Namespace = r.Prefix
	r.beginTxn = func() (transactionAPI, error) { t.Fatal("lease admission read TiKV"); return nil, injected }
	c := &ClusterService{root: r, localOwnerLeases: true, coordinator: &Coordinator{leaseReads: true, root: r}, graphReplication: true, locals: map[IndexPartitionID]*localPartition{q.Partition: e}}
	for _, op := range []string{"sync", "checkpoint", "prune", "rebuild"} {
		q.Operation = op
		// Valid authority reaches the service check; a stale session does not.
		if _, err := c.execute(context.Background(), q); !errors.Is(err, ErrPartitionUnavailable) {
			t.Fatal(op, err)
		}
		stale := q
		stale.Session = "old"
		if _, err := c.execute(context.Background(), stale); !errors.Is(err, ErrFenced) {
			t.Fatal(op, err)
		}
	}
}

func TestMaintenanceAndDeliveryDoNotReplayUnknownResults(t *testing.T) {
	for _, op := range []string{"deliver", "checkpoint", "prune", "rebuild"} {
		f := newGraphFixture(t)
		c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, cachedRouting: true, PartitionedService: &PartitionedService{layout: f.layout}}
		if _, err := c.cachedSearchRoutes(f.ctx, false); err != nil {
			t.Fatal(err)
		}
		f.r.beginTxn = func() (transactionAPI, error) { panic("unknown operation was retried after refresh") }
		calls := 0
		c.client = &http.Client{Transport: routeTransport(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("response lost") })}
		p := &remotePartition{cluster: c, id: "index-0000"}
		if _, err := p.cachedRoutedCall(f.ctx, op, Search{}, &Delivery{}); err == nil || calls != 1 {
			t.Fatal(op, err, calls)
		}
	}
}

func TestDeliveryEpochRejectionIsNotAnAddressFailure(t *testing.T) {
	f := newGraphFixture(t)
	c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, cachedRouting: true, PartitionedService: &PartitionedService{layout: f.layout}}
	before, err := c.cachedSearchRoutes(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	f.r.beginTxn = func() (transactionAPI, error) { panic("source epoch rejection incorrectly refreshed routes") }
	calls := 0
	c.client = &http.Client{Transport: routeTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{"code":"fenced","source_epoch":{"source":0,"epoch":4,"commit_lsn":1}}`)), Header: make(http.Header)}, nil
	})}
	err = c.deliver(f.ctx, "index-0000", Delivery{})
	var rejected *SourceEpochError
	if !errors.As(err, &rejected) || rejected.Epoch != 4 || calls != 1 {
		t.Fatal(err, calls)
	}
	if after, err := c.cachedSearchRoutes(f.ctx, false); err != nil || after != before {
		t.Fatal("valid routes invalidated", err)
	}
}

func TestIdempotentSyncKeepsBoundedHandoffRetry(t *testing.T) {
	f := newGraphFixture(t)
	c := &ClusterService{root: f.r, coordinator: f.c, replicas: 2, cachedRouting: true, graphReplication: true, PartitionedService: &PartitionedService{layout: f.layout}}
	calls := 0
	c.client = &http.Client{Transport: routeTransport(func(*http.Request) (*http.Response, error) {
		calls++
		status, body := 200, `{}`
		if calls == 1 {
			status, body = 503, `{"code":"fenced"}`
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	p := &remotePartition{cluster: c, id: "index-0000"}
	if _, err := p.call(f.ctx, "sync", Search{}); err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
}

type visibilityPartition struct {
	PartitionIndex
	syncs     int
	lastToken *Token
}

func (p *visibilityPartition) Sync(context.Context) error { p.syncs++; return nil }
func (p *visibilityPartition) Search(q Search) ([]Hit, error) {
	p.lastToken = q.MinVisible
	return []Hit{{ID: "a", Revision: 1}}, nil
}

func TestDeleteVisibilityTokenReachesEveryIndex(t *testing.T) {
	l, _ := newLayout(3)
	p := &PartitionedService{layout: l, router: IdentityPartitionRouter{l}, dimension: 2, indexes: map[IndexPartitionID]PartitionIndex{}}
	var indexes []*visibilityPartition
	for _, b := range l.Bindings {
		index := &visibilityPartition{}
		indexes = append(indexes, index)
		p.indexes[b.Index] = index
	}
	q := Search{Vector: []float64{0, 0}, K: 10, MinVisible: &Token{ID: "a", Revision: 2}, visibilityAll: true}
	if _, err := p.SearchContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	for _, i := range indexes {
		if i.lastToken == nil || i.lastToken.Revision != 2 {
			t.Fatal("tombstone barrier dropped", i.lastToken)
		}
	}
}

func TestRoutedMinVisibleReadsRecordAndSynchronizesOnce(t *testing.T) {
	r, _ := newFakeRepo()
	ctx := context.Background()
	if _, err := r.Mutate(ctx, "a", Mutation{RequestID: "create", Vector: []float64{1, 0}}, false); err != nil {
		t.Fatal(err)
	}
	tx, _ := r.begin()
	if err := putJSON(tx, completedKey(r), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	l, _ := newLayout(1)
	l.Routing = &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}
	index := &visibilityPartition{}
	c := &ClusterService{root: r, coordinator: &Coordinator{root: r}, PartitionedService: &PartitionedService{layout: l, router: clusterRouter{IdentityPartitionRouter{l}, l.Routing}, data: map[DataPartitionID]*Repository{0: r}, indexes: map[IndexPartitionID]PartitionIndex{"index-0000": index}, dimension: 2}}
	base := r.beginTxn
	reads := map[string]int{}
	r.beginTxn = func() (transactionAPI, error) { tx, err := base(); return &countedScheduleTx{tx, reads}, err }
	if _, err := c.SearchContext(ctx, Search{Vector: []float64{1, 0}, K: 1, MinVisible: &Token{ID: "a", Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	if index.syncs != 1 || reads[string(r.key("record", "a"))] != 1 {
		t.Fatal("duplicate visibility work", index.syncs, reads)
	}
}
