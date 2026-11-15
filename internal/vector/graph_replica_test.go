package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

func equalGraph(t *testing.T, a, b *HNSWIndex) {
	t.Helper()
	x, e := a.ExportState()
	if e != nil {
		t.Fatal(e)
	}
	y, e := b.ExportState()
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(x, y) {
		t.Fatal("graph, records, mapping or RNG state differ")
	}
}
func TestMaterializedGraphDelta(t *testing.T) {
	a, b := mustGraph(t, 2), mustGraph(t, 2)
	for batch := 0; batch < 8; batch++ {
		var records []Record
		for k := 0; k < 100; k++ {
			id := k + batch*40
			records = append(records, Record{ID: fmt.Sprint(id), Revision: uint64(batch + 1), Vector: []float64{float64(id % 7), float64(id) / 100}, Payload: map[string]string{"group": fmt.Sprint(id % 7)}})
		}
		if batch > 0 {
			records = append(records, Record{ID: "0", Revision: uint64(batch + 1), Deleted: true})
		}
		prior, _ := a.ExportState()
		d, e := a.GenerateDelta(records)
		if e != nil {
			t.Fatal(e)
		}
		after, _ := a.ExportState()
		if !reflect.DeepEqual(prior, after) {
			t.Fatal("construction leaked uncommitted mutation")
		}
		if e = b.PrepareDelta(d); e != nil {
			t.Fatal(e)
		}
		equalGraph(t, a, b)
		if e = a.ApplyDelta(d, false); e != nil {
			t.Fatal(e)
		}
		if e = b.ApplyDelta(d, false); e != nil {
			t.Fatal(e)
		}
		equalGraph(t, a, b)
		if e = b.ApplyDelta(d, false); e == nil {
			t.Fatal("duplicate accepted")
		}
		if batch == 3 {
			state, _ := b.ExportState()
			restored, e := RestoreHNSW(state)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(restored.graph.Close)
			b = restored
			a, b = b, a
		}
	}
}

type graphFixture struct {
	ctx      context.Context
	r        *Repository
	db       *fakeDB
	c        *Coordinator
	master   Ownership
	layout   PartitionLayout
	nodes    []ClusterNode
	services []*Service
	objects  *Objects
}

func newGraphFixture(t *testing.T) *graphFixture {
	t.Helper()
	ctx := context.Background()
	r, db := newFakeRepo()
	c := &Coordinator{root: r, ttl: 60000}
	layout, _ := newLayout(1)
	master := ClusterNode{ID: "m", Session: "m", URL: "http://m", Roles: []string{RoleMaster}}
	if e := c.Heartbeat(ctx, master); e != nil {
		t.Fatal(e)
	}
	lease, e := c.Acquire(ctx, masterLeaseID, master)
	if e != nil {
		t.Fatal(e)
	}
	nodes := []ClusterNode{{ID: "a", Session: "a", URL: "http://a", Roles: []string{RoleWorker}}, {ID: "b", Session: "b", URL: "http://b", Roles: []string{RoleWorker}}}
	for _, n := range nodes {
		if e = c.Heartbeat(ctx, n); e != nil {
			t.Fatal(e)
		}
	}
	if e = c.ScheduleReplicas(ctx, lease, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	objects := partitionObjects()
	services := []*Service{}
	for _, n := range nodes {
		owner, err := c.Acquire(ctx, replicaID("index-0000", n), n)
		if err != nil {
			t.Fatal(err)
		}
		repo := *r
		repo.Fence = c.ReplicaFence("index-0000", owner)
		repo.Retain = c.RetainReplicas("index-0000")
		s, err := newGraphService(ctx, &repo, objects, graphJournal{c, "index-0000", owner})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Index.graph.Close)
		if _, err = c.Update(ctx, owner, true, false); err != nil {
			t.Fatal(err)
		}
		services = append(services, s)
	}
	if e = c.ScheduleReplicas(ctx, lease, layout, nodes, 2); e != nil {
		t.Fatal(e)
	}
	p, _ := c.Plan(ctx, "index-0000")
	if services[0].graphReplica.journal.owner.Partition != p.Publisher {
		services[0], services[1] = services[1], services[0]
	}
	return &graphFixture{ctx, r, db, c, lease, layout, nodes, services, objects}
}
func (f *graphFixture) write(t *testing.T, id string, deleted bool) Record {
	t.Helper()
	r, e := f.r.Mutate(f.ctx, id, Mutation{RequestID: fmt.Sprintf("write-%d", f.db.commits), Vector: []float64{1, 2}}, deleted)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestGraphAsyncReplicationAndFailover(t *testing.T) {
	f := newGraphFixture(t)
	a, b := f.services[0], f.services[1]
	ctx := f.ctx
	r := f.write(t, "v", false)
	if e := a.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	head, e := a.graphReplica.journal.Head(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if head.Committed != head.Visible || head.Visible.Sequence != 1 || a.Cursor() != 1 || b.Cursor() != 0 {
		t.Fatal("leader waited for follower", head, a.Cursor(), b.Cursor())
	}
	hits, pos, e := b.searchGraphLocal(ctx, Search{Vector: []float64{1, 2}, K: 1})
	if e != nil || len(hits) != 0 || pos.Sequence != 0 {
		t.Fatal("stale query must remain available", hits, pos, e)
	}
	if e = b.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	// Commit a graph delta and crash before publication. Raw data already durable.
	f.write(t, "w", false)
	h, e := a.graphReplica.journal.Claim(ctx)
	if e != nil {
		t.Fatal(e)
	}
	events, e := f.r.ReadEvents(ctx, 1, 0, 256)
	if e != nil {
		t.Fatal(e)
	}
	d, e := a.Index.GenerateDelta([]Record{events.Events[0].Record})
	if e != nil {
		t.Fatal(e)
	}
	data, e := jsonMarshalDelta(d)
	if e != nil {
		t.Fatal(e)
	}
	m, e := f.objects.saveBytes(ctx, f.r.Namespace(), 2, deltaFormat, data)
	if e != nil {
		t.Fatal(e)
	}
	pending, e := a.graphReplica.journal.Commit(ctx, h, 2, "delta", m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.Update(ctx, a.graphReplica.journal.owner, false, true); e != nil {
		t.Fatal(e)
	}
	if e = f.c.ScheduleReplicas(ctx, f.master, f.layout, f.nodes, 2); e != nil {
		t.Fatal(e)
	}
	if e = b.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	after, e := b.graphReplica.journal.Head(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if after.Visible != pending.Position || after.Term <= h.Term || b.Cursor() != 2 {
		t.Fatal("promotion lost pending entry", after)
	}
	if _, e = a.graphReplica.journal.Commit(ctx, h, 3, "delta", m); !errors.Is(e, ErrFenced) {
		t.Fatal("old leader not fenced", e)
	}
	if _, e = a.graphReplica.journal.Publish(ctx); !errors.Is(e, ErrFenced) {
		t.Fatal("old publisher not fenced", e)
	}
	f.write(t, "z", false)
	if e = b.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	if b.Cursor() != 3 {
		t.Fatal("did not resume data log")
	}
	if _, e = b.Checkpoint(ctx); e != nil {
		t.Fatal(e)
	}
	restored, e := newGraphService(ctx, b.graphReplica.repo, f.objects, b.graphReplica.journal)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Index.graph.Close()
	equalGraph(t, b.Index, restored.Index)
	if e = b.Rebuild(ctx); e != nil {
		t.Fatal(e)
	}
	if e = restored.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, b.Index, restored.Index)
	if _, e = b.Prune(ctx); e != nil {
		t.Fatal(e)
	}
	// Existing revision visibility remains explicit while ordinary reads can lag.
	_, _, e = b.searchGraphLocal(ctx, Search{Vector: []float64{1, 2}, K: 1, MinVisible: &Token{ID: r.ID, Revision: r.Revision}})
	if e != nil {
		t.Fatal(e)
	}
}
func jsonMarshalDelta(d GraphDelta) ([]byte, error) { return json.Marshal(d) }
func TestGraphDeltaRejectionDoesNotMutateRecords(t *testing.T) {
	a := mustGraph(t, 2)
	d, e := a.GenerateDelta([]Record{{ID: "a", Revision: 1, Vector: []float64{1, 2}}})
	if e != nil {
		t.Fatal(e)
	}
	before, _ := a.ExportState()
	d.Native = d.Native[:len(d.Native)-1]
	if e = a.ApplyDelta(d, false); e == nil {
		t.Fatal("truncation accepted")
	}
	after, _ := a.ExportState()
	if !bytes.Equal(before.Graph, after.Graph) || !reflect.DeepEqual(before.Records, after.Records) {
		t.Fatal("corruption mutated visible records")
	}
	if _, e = a.Search(Search{Vector: []float64{1, 2}, K: 1}); !errors.Is(e, ErrGraphCorrupt) {
		t.Fatal("poisoned replica served reads", e)
	}
}

func TestGraphJournalCommitOutcomes(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			f := newGraphFixture(t)
			s := f.services[0]
			g := s.graphReplica
			f.write(t, "one", false)
			head, e := g.journal.Claim(f.ctx)
			if e != nil {
				t.Fatal(e)
			}
			batch, e := f.r.ReadEvents(f.ctx, 0, 0, 256)
			if e != nil {
				t.Fatal(e)
			}
			d, e := s.Index.GenerateDelta([]Record{batch.Events[0].Record})
			if e != nil {
				t.Fatal(e)
			}
			data, _ := json.Marshal(d)
			m, e := f.objects.saveBytes(f.ctx, f.r.Namespace(), 2, deltaFormat, data)
			if e != nil {
				t.Fatal(e)
			}
			f.db.commitErr = injected
			f.db.committedDespiteError = uncertain
			if _, e = g.journal.Commit(f.ctx, head, 1, "delta", m); !errors.Is(e, injected) {
				t.Fatal(e)
			}
			f.db.commitErr = nil
			f.db.committedDespiteError = false
			h, e := g.journal.Head(f.ctx)
			if e != nil {
				t.Fatal(e)
			}
			if (h.Committed.Sequence == 1) != uncertain || h.Visible.Sequence != 0 || s.Cursor() != 0 {
				t.Fatal("ambiguous commit leaked visible state", h)
			}
			if e = s.Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			h, e = g.journal.Head(f.ctx)
			if e != nil {
				t.Fatal(e)
			}
			if h.Visible.Sequence != 1 || s.Cursor() != 1 {
				t.Fatal("retry duplicated or lost committed graph batch", h)
			}
			if e = f.services[1].Sync(f.ctx); e != nil {
				t.Fatal(e)
			}
			equalGraph(t, s.Index, f.services[1].Index)
		})
	}
}
func TestGraphModeProtocolIsolation(t *testing.T) {
	r, _ := newFakeRepo()
	ctx := context.Background()
	if e := r.ensureGraphMode(ctx, "hash", "host"); e != nil {
		t.Fatal(e)
	}
	if e := r.ensureGraphMode(ctx, "hash", "host"); e != nil {
		t.Fatal(e)
	}
	if e := r.ensureTopologyMode(ctx, "hash", "host"); e == nil {
		t.Fatal("independent writer joined graph namespace")
	}
	if e := r.ensureGraphMode(ctx, "hash", "node"); e == nil {
		t.Fatal("failure domain mismatch accepted")
	}
}

type delayedGraphBlobs struct {
	blobAPI
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *delayedGraphBlobs) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
	}
	return b.blobAPI.Get(ctx, bucket, key)
}
func TestGraphFollowerQueriesDuringSlowReplay(t *testing.T) {
	f := newGraphFixture(t)
	a, b := f.services[0], f.services[1]
	f.write(t, "initial", false)
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	f.write(t, "next", false)
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	blobs := &delayedGraphBlobs{blobAPI: f.objects.Client, entered: make(chan struct{}), release: make(chan struct{})}
	f.objects.Client = blobs
	done := make(chan error, 1)
	go func() { done <- b.Sync(f.ctx) }()
	select {
	case <-blobs.entered:
	case <-time.After(3 * time.Second):
		close(blobs.release)
		t.Fatal("replay did not start")
	}
	query := make(chan error, 1)
	go func() {
		hits, pos, e := b.searchGraphLocal(f.ctx, Search{Vector: []float64{1, 2}, K: 10})
		if e == nil && (len(hits) != 1 || pos.Sequence != 1) {
			e = errors.New("query saw an unpublished/incomplete view")
		}
		query <- e
	}()
	select {
	case e := <-query:
		if e != nil {
			t.Error(e)
		}
	case <-time.After(time.Second):
		t.Error("query blocked on object download")
	}
	close(blobs.release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
}
