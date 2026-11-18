package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func enableReferences(f *graphFixture) {
	f.c.referenceLogs = true
	f.c.data = map[DataPartitionID]*Repository{0: f.r}
	for _, s := range f.services {
		s.graphReplica.retention = true
	}
}

type unavailableGraphObjects struct{ blobAPI }

func (unavailableGraphObjects) Put(context.Context, string, string, []byte) error {
	return errors.New("S3 unavailable")
}
func (unavailableGraphObjects) Get(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("S3 unavailable")
}

func TestReferenceGraphReplaysHistoricalDataWithoutObjects(t *testing.T) {
	f := newGraphFixture(t)
	enableReferences(f)
	a, b := f.services[0], f.services[1]
	f.objects.Client = unavailableGraphObjects{f.objects.Client}
	for round := 0; round < 5; round++ {
		for n := 0; n < 40; n++ {
			_, err := f.r.Mutate(f.ctx, fmt.Sprint(n), Mutation{RequestID: fmt.Sprintf("%d/%d", round, n), Vector: []float64{float64(n % 5), float64(round)}, Payload: map[string]string{"secret": "attributes-stay-in-data-log"}}, round == 4 && n%3 == 0)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := a.Sync(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Follower reads old versions even though authoritative current rows changed.
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
	for seq := uint64(1); seq <= 5; seq++ {
		e, err := a.graphReplica.journal.Entry(f.ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		if e.Payload == nil || e.Object.Key != "" {
			t.Fatal("normal delta still uses S3")
		}
		payload, err := a.graphReplica.journal.loadPayload(f.ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(payload, []byte("attributes-stay-in-data-log")) {
			t.Fatal("attributes duplicated in graph journal")
		}
		var w ReferenceDelta
		if err = json.Unmarshal(payload, &w); err != nil {
			t.Fatal(err)
		}
		if w.Delta.Records != nil {
			t.Fatal("full records persisted in graph journal")
		}
	}
}

func TestReferenceReplayRejectsMissingOrCorruptDependencies(t *testing.T) {
	for _, damage := range []string{"missing_event", "wrong_event", "missing_chunk", "wrong_chunk"} {
		t.Run(damage, func(t *testing.T) {
			f := newGraphFixture(t)
			enableReferences(f)
			a, b := f.services[0], f.services[1]
			f.write(t, "a", false)
			if err := a.Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			key := string(f.r.logKey(1))
			if damage == "missing_chunk" || damage == "wrong_chunk" {
				key = string(a.graphReplica.journal.payloadKey(1, 0))
			}
			if damage == "missing_event" || damage == "missing_chunk" {
				delete(f.db.data, key)
			} else {
				f.db.data[key] = []byte(`{"sequence":1,"record":{"id":"wrong","revision":1,"vector":[1,2]}}`)
			}
			if err := b.Sync(f.ctx); err == nil {
				t.Fatal("corruption was accepted")
			}
			if b.graphReplica.position.Sequence != 0 {
				t.Fatal("partial graph published")
			}
		})
	}
}

func TestReferenceCheckpointUploadAllowsLaterAppends(t *testing.T) {
	f := newGraphFixture(t)
	enableReferences(f)
	a, b := f.services[0], f.services[1]
	f.write(t, "first", false)
	if err := a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	blobs := &delayedGraphBlobs{blobAPI: f.objects.Client, entered: make(chan struct{}), release: make(chan struct{})}
	f.objects.Client = blobs
	done := make(chan error, 1)
	go func() { _, err := a.Checkpoint(f.ctx); done <- err }()
	select {
	case <-blobs.entered:
	case <-time.After(3 * time.Second):
		close(blobs.release)
		t.Fatal("upload never started")
	}
	f.write(t, "later", false)
	appendDone := make(chan error, 1)
	go func() { appendDone <- a.Sync(f.ctx) }()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("upload blocked ingestion")
	}
	close(blobs.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h, err := a.graphReplica.journal.Head(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.CheckpointPosition.Sequence != 1 || h.Committed.Sequence != 2 || h.CheckpointBytes >= h.LogBytes {
		t.Fatal("checkpoint swallowed concurrent append", h)
	}
	state, err := f.objects.LoadState(f.ctx, *h.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Records) != 1 || state.LogBytes != h.CheckpointBytes {
		t.Fatal("snapshot coverage wrong")
	}
	if _, err = a.graphReplica.journal.Prune(f.ctx, a.graphReplica.repo, 256); err != nil {
		t.Fatal(err)
	}
	delete(f.db.data, string(f.r.logKey(1))) // Covered record can now be absent.
	if err = b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
}

func TestReferenceCheckpointTriggersAndIdle(t *testing.T) {
	f := newGraphFixture(t)
	enableReferences(f)
	a := f.services[0]
	j := a.graphReplica.journal
	if due, err := j.checkpointDue(f.ctx, 1, time.Second); err != nil || due {
		t.Fatal(due, err)
	}
	f.write(t, "a", false)
	if err := a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if due, err := j.checkpointDue(f.ctx, 1, DefaultGraphCheckpointAge); err != nil || !due {
		t.Fatal("size trigger", due, err)
	}
	if due, err := j.checkpointDue(f.ctx, ^uint64(0), DefaultGraphCheckpointAge); err != nil || due {
		t.Fatal("premature age trigger", due, err)
	}
	entry, _ := j.Entry(f.ctx, 1)
	f.db.nowMillis = entry.CreatedMS + DefaultGraphCheckpointAge.Milliseconds()
	if due, err := j.checkpointDue(f.ctx, ^uint64(0), DefaultGraphCheckpointAge); err != nil || !due {
		t.Fatal("age trigger", due, err)
	}
	f.db.nowMillis = entry.CreatedMS + 1
	if _, err := a.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.db.nowMillis = entry.CreatedMS + 2*DefaultGraphCheckpointAge.Milliseconds()
	if due, err := j.checkpointDue(f.ctx, 1, time.Second); err != nil || due {
		t.Fatal("idle graph checkpoint repeated", due, err)
	}
}

func TestReferenceGCProtectsPinnedGraphData(t *testing.T) {
	f, c, owner := routedFixture(t)
	enableReferences(f)
	c.consumerRetention = true
	c.graphReplication = true
	a, b := f.services[0], f.services[1]
	for _, s := range f.services {
		s.graphReplica.repo.IndexPrefix = "index/index-0000/"
	}
	for n := 0; n < 2; n++ {
		f.write(t, fmt.Sprint(n), false)
		q := deliveryFor(t, f, owner, uint64(n))
		if err := a.graphReplica.enqueue(f.ctx, q); err != nil {
			t.Fatal(err)
		}
		if err := a.Sync(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	seed(f.db, completedKey(f.r), uint64(2))
	safe := func() uint64 {
		tx, _ := f.r.begin()
		defer tx.Rollback()
		v, err := c.retainGraphReferences(f.ctx, tx, 0, 2)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if safe() != 0 {
		t.Fatal("completed LSN alone allowed data GC")
	}
	if _, err := a.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}
	// Pin the second data delta (sequence 3, following an epoch barrier).
	req := ConsumerRequest{Partition: "index-0000", Stream: "graph", Consumer: "sink", Session: "reference-test-session", Action: "acquire", Ack: 2}
	lease, err := c.ManageConsumer(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.graphReplica.journal.Prune(f.ctx, a.graphReplica.repo, 256); err != nil {
		t.Fatal(err)
	}
	if safe() != 1 {
		t.Fatal("remaining graph references not protected", safe())
	}
	e, err := a.graphReplica.journal.Entry(f.ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.graphReplica.loadReferenceEntry(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	req.Action = "ack"
	req.Epoch = lease.Leases["sink"].Epoch
	req.Ack = 3
	if _, err = c.ManageConsumer(f.ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err = a.graphReplica.journal.Prune(f.ctx, a.graphReplica.repo, 256); err != nil {
		t.Fatal(err)
	}
	if safe() != 2 {
		t.Fatal("safe prefix failed to advance")
	}
	if _, exists := f.db.data[string(a.graphReplica.journal.payloadKey(3, 0))]; exists {
		t.Fatal("payload chunks leaked")
	}
	f.r.DataSnapshots = true
	if err = c.maintainData(f.ctx, 0, owner); err != nil {
		t.Fatal(err)
	}
	if batch, err := f.r.ReadEvents(f.ctx, 0, 0, 10); !errors.Is(err, ErrLogCompacted) || batch.Floor != 2 {
		t.Fatal("automatic data snapshot/GC failed", batch, err)
	}
	if err = b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
}

func TestReferenceAppendFailureAndUnknownOutcome(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			f, _, owner := routedFixture(t)
			enableReferences(f)
			a, b := f.services[0], f.services[1]
			g := a.graphReplica
			f.write(t, "x", false)
			q := deliveryFor(t, f, owner, 0)
			h, err := g.journal.Claim(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = g.enqueue(f.ctx, q); err != nil {
				t.Fatal(err)
			}
			h, err = g.journal.Head(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			f.db.commitErr = injected
			f.db.committedDespiteError = unknown
			if err = g.commitRouted(f.ctx, a, h); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			f.db.commitErr = nil
			f.db.committedDespiteError = false
			h, err = g.journal.Head(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(0)
			if unknown {
				want = 1
			}
			if h.Committed.Sequence != 1+want || h.Sources["0"] != want {
				t.Fatal("partial source ACK", h)
			}
			_, exists := f.db.data[string(g.journal.payloadKey(2, 0))]
			if exists != unknown {
				t.Fatal("orphan/missing payload")
			}
			if err = g.enqueue(f.ctx, q); err != nil {
				t.Fatal(err)
			}
			if err = a.Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err = b.Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			equalGraph(t, a.Index, b.Index)
			h, err = g.journal.Head(f.ctx)
			if err != nil || h.Committed.Sequence != 2 {
				t.Fatal("duplicate committed", h, err)
			}
		})
	}
}

func TestReferenceFailedCheckpointDoesNotAuthorizeGC(t *testing.T) {
	f := newGraphFixture(t)
	enableReferences(f)
	a := f.services[0]
	f.write(t, "x", false)
	if err := a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	blobs := f.objects.Client
	f.objects.Client = unavailableGraphObjects{blobs}
	if _, err := a.Checkpoint(f.ctx); err == nil {
		t.Fatal("unavailable object store published checkpoint")
	}
	h, err := a.graphReplica.journal.Head(f.ctx)
	if err != nil || h.Checkpoint != nil {
		t.Fatal(h, err)
	}
	if floor, err := a.graphReplica.journal.Prune(f.ctx, a.graphReplica.repo, 256); err != nil || floor.Sequence != 0 {
		t.Fatal(floor, err)
	}
	f.objects.Client = blobs
	if _, err = a.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReferenceGCUsesActualTargetsAndStopsAtHole(t *testing.T) {
	f, c, _ := routedFixture(t)
	enableReferences(f)
	c.layout.Routing = &ClusterRouting{Version: 1, Centers: [][]float64{{-10, 0}, {10, 0}}}
	c.layout.Bindings = []PartitionBinding{{Index: "index-0000"}, {Index: "index-0001"}}
	c.router = clusterRouter{IdentityPartitionRouter{c.layout}, c.layout.Routing}
	for n, v := range [][]float64{{-10, 0}, {10, 0}, {-10, 0}} {
		if _, err := f.r.Mutate(f.ctx, fmt.Sprint(n), Mutation{RequestID: fmt.Sprint(n), Vector: v}, n == 2); err != nil {
			t.Fatal(err)
		}
	}
	for _, last := range []uint64{0, 2, 3} {
		for id, n := range map[IndexPartitionID]uint64{"index-0000": 3, "index-0001": last} {
			seed(f.db, (graphJournal{c: f.c, id: id}).key("head"), GraphHead{FloorSources: map[string]uint64{"0": n}})
		}
		tx, _ := f.r.begin()
		safe, err := c.retainGraphReferences(f.ctx, tx, 0, 3)
		_ = tx.Rollback()
		want := last
		if last == 0 {
			want = 1
		}
		if err != nil || safe != want {
			t.Fatal("skipped hole or included unrelated index", last, safe, err)
		}
	}
}

type countCheckpointPuts struct {
	blobAPI
	puts int
}

func (b *countCheckpointPuts) Put(ctx context.Context, bucket, key string, data []byte) error {
	b.puts++
	return b.blobAPI.Put(ctx, bucket, key, data)
}

func TestReferenceCheckpointRetriesFencesWithoutReupload(t *testing.T) {
	f := newGraphFixture(t)
	enableReferences(f)
	s := f.services[0]
	g := s.graphReplica
	f.write(t, "x", false)
	if err := s.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	blobs := &countCheckpointPuts{blobAPI: f.objects.Client}
	f.objects.Client = blobs
	original := f.r.beginTxn
	calls := 0
	hooked := func() (transactionAPI, error) {
		tx, err := original()
		if err != nil {
			return nil, err
		}
		return indexCommitHookTx{tx, func() error {
			calls++
			if calls == 1 || calls == 3 {
				return indexWriteConflict()
			}
			return nil
		}}, nil
	}
	f.r.beginTxn = hooked
	g.repo.beginTxn = hooked
	_, err := s.Checkpoint(f.ctx)
	f.r.beginTxn = original
	g.repo.beginTxn = original
	if err != nil || calls != 4 || blobs.puts != 1 {
		t.Fatal("checkpoint did not retry aborted metadata transactions exactly", err, calls, blobs.puts)
	}
	h, err := g.journal.Head(f.ctx)
	if err != nil || h.CheckpointPosition != h.Committed {
		t.Fatal(h, err)
	}
	if m, err := s.Checkpoint(f.ctx); err != nil || m.Generation != h.Checkpoint.Generation || blobs.puts != 1 {
		t.Fatal("idle checkpoint reuploaded", m, err, blobs.puts)
	}
	if _, err := f.services[1].Checkpoint(f.ctx); !errors.Is(err, ErrFenced) {
		t.Fatal("follower bypassed checkpoint fencing", err)
	}
}
