package vector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func attachProgressWriter(t *testing.T, f *graphFixture, c *ClusterService, owner Ownership) *mutationGroup {
	t.Helper()
	c.runCtx = f.ctx
	c.node.ID, c.node.Session = owner.Node, owner.Session
	w, err := c.recoverWriter(f.ctx, 0, owner)
	if err != nil {
		t.Fatal(err)
	}
	confirmAppendGrant(t, w.lease, owner.Epoch, owner.Expires, time.Minute)
	c.writers = map[DataPartitionID]*mutationGroup{0: w}
	return w
}

type noDataProgressRead struct{ transactionAPI }

func (tx noDataProgressRead) Get(ctx context.Context, key []byte) ([]byte, error) {
	if strings.Contains(string(key), "log-floor") || strings.Contains(string(key), "dispatch-completed") {
		return nil, errors.New("status read data metadata")
	}
	return tx.transactionAPI.Get(ctx, key)
}
func (tx noDataProgressRead) LastKey(context.Context, []byte, []byte) ([]byte, error) {
	return nil, errors.New("status scanned the data log")
}

func TestDataProgressUsesLiveCountersAndMarksCachedSamples(t *testing.T) {
	f, c, owner := routedFixture(t)
	w := attachProgressWriter(t, f, c, owner)
	w.lease.head.Store(100)
	w.lease.indexed.Store(80)
	w.lease.floor.Store(20)
	begin := f.r.beginTxn
	f.r.beginTxn = func() (transactionAPI, error) {
		tx, err := begin()
		return noDataProgressRead{tx}, err
	}
	read := func() DataProgress {
		t.Helper()
		p, err := c.dataProgress(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		return p["data-0000"]
	}
	p := read()
	if !p.Known || !p.Fresh || !p.OwnerAvailable || p.Head != 100 || p.Completed != 80 || p.Floor != 20 || p.ObservedEpoch != owner.Epoch || p.ObservedAtMS == 0 {
		t.Fatal(p)
	}
	w.lease.StepDown()
	stale := read()
	if !stale.Known || stale.Fresh || stale.OwnerAvailable || stale.Head != p.Head || stale.Completed != p.Completed || stale.ObservedAtMS != p.ObservedAtMS {
		t.Fatal("stale counters presented as current", stale)
	}
	text := clusterMetrics(ClusterStatus{DataProgress: map[string]DataProgress{"data-0000": stale}}).String()
	if strings.Contains(text, "vecdb_data_pending_events") {
		t.Fatal("stale progress reported a current lag", text)
	}
	c.progressCache = nil
	unknown := read()
	if unknown.Known || unknown.Fresh || unknown.OwnerAvailable || unknown.ObservedAtMS != 0 {
		t.Fatal("unobserved partition presented as empty", unknown)
	}
}

func TestDataProgressRPCRejectsWrongOwnerWithoutStorage(t *testing.T) {
	f, worker, owner := routedFixture(t)
	w := attachProgressWriter(t, f, worker, owner)
	w.lease.head.Store(10)
	w.lease.indexed.Store(7)
	worker.token = "progress-unit-secret"
	server := httptest.NewServer(worker.InternalHandler())
	defer server.Close()
	owner.URL = server.URL
	caller := &ClusterService{root: f.r, node: ClusterNode{ID: "gateway"}, token: worker.token, client: server.Client()}
	source := DataPartitionID(0)
	req := partitionRPC{Namespace: f.r.Prefix, Partition: owner.Partition, Source: &source, Epoch: owner.Epoch, Session: owner.Session, Operation: "data_progress"}
	f.r.beginTxn = func() (transactionAPI, error) { return nil, errors.New("progress RPC touched TiKV") }
	if reply, err := caller.sendPartition(f.ctx, owner, req); err != nil || reply.DataProgress == nil || reply.DataProgress.Head != 10 || reply.DataProgress.Completed != 7 {
		t.Fatal(reply, err)
	}
	for _, change := range []func(*partitionRPC){
		func(q *partitionRPC) { q.Epoch++ },
		func(q *partitionRPC) { q.Session = "old-process" },
		func(q *partitionRPC) { q.Namespace = "another-collection" },
		func(q *partitionRPC) { q.Partition = sourceID(1) },
		func(q *partitionRPC) { q.Source = nil },
	} {
		bad := req
		change(&bad)
		if _, err := caller.sendPartition(f.ctx, owner, bad); !errors.Is(err, ErrFenced) {
			t.Fatal("bad progress request accepted", bad, err)
		}
	}
	w.lease.StepDown()
	if _, err := caller.sendPartition(f.ctx, owner, req); !errors.Is(err, ErrFenced) {
		t.Fatal("expired data owner reported fresh progress", err)
	}
}

func TestDataProgressRejectsMismatchedSampleAndBoundsOfflineWait(t *testing.T) {
	f, c, owner := routedFixture(t)
	old := DataProgress{Known: true, Fresh: true, OwnerAvailable: true, Owner: owner,
		Head: 10, Completed: 7, ObservedEpoch: owner.Epoch, ObservedAtMS: time.Now().UnixMilli()}
	c.progressCache = map[DataPartitionID]DataProgress{0: old}
	for _, change := range []func(*DataProgress){
		func(p *DataProgress) { p.Owner.Session = "old-process" },
		func(p *DataProgress) { p.ObservedEpoch++ },
		func(p *DataProgress) { p.Completed = p.Head + 1 },
		func(p *DataProgress) { p.Floor = p.Completed + 1 },
	} {
		p := old
		change(&p)
		body, err := json.Marshal(partitionReply{DataProgress: &p})
		if err != nil {
			t.Fatal(err)
		}
		c.client = &http.Client{Transport: routeTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
		})}
		progress, err := c.dataProgress(f.ctx)
		if d := progress["data-0000"]; err != nil || !d.Known || d.Fresh || d.Head != old.Head || d.Completed != old.Completed {
			t.Fatal("invalid sample replaced the last observation", d, err)
		}
	}
	c.client = &http.Client{Transport: routeTransport(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	started := time.Now()
	progress, err := c.dataProgress(f.ctx)
	if d := progress["data-0000"]; err != nil || !d.Known || d.Fresh || time.Since(started) > 3*time.Second {
		t.Fatal("offline owner blocked status or lost cached progress", d, err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := c.dataProgress(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation ignored", err)
	}
}
