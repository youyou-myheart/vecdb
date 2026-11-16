package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func retentionCluster(f *graphFixture) *ClusterService {
	for _, s := range f.services {
		s.graphReplica.retention = true
	}
	return &ClusterService{consumerRetention: true, graphReplication: true, coordinator: f.c, PartitionedService: &PartitionedService{layout: f.layout, data: map[DataPartitionID]*Repository{0: f.r}}}
}
func TestConsumerRetentionAndEpochs(t *testing.T) {
	f := newGraphFixture(t)
	c := retentionCluster(f)
	a, b := f.services[0], f.services[1]
	for n := 0; n < 3; n++ {
		f.write(t, fmt.Sprint(n), false)
		if e := a.Sync(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
	if e := b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := a.Checkpoint(f.ctx); e != nil {
		t.Fatal(e)
	}
	for _, s := range f.services {
		if e := f.c.Progress(f.ctx, s.graphReplica.journal.owner, s.Cursor()); e != nil {
			t.Fatal(e)
		}
	}
	request := ConsumerRequest{Partition: "index-0000", Stream: "data", Consumer: "sink", Session: "session-one-123456", Action: "acquire", TTLSeconds: 10}
	lease, e := c.ManageConsumer(f.ctx, request)
	if e != nil {
		t.Fatal(e)
	}
	request.Epoch = lease.Leases["sink"].Epoch
	graph := request
	graph.Stream = "graph"
	graph.Ack = 1
	if _, e = c.ManageConsumer(f.ctx, graph); e != nil {
		t.Fatal(e)
	}
	if floor, e := a.Prune(f.ctx); e != nil || floor != 0 {
		t.Fatal("data ACK did not pin", floor, e)
	}
	head, _ := a.graphReplica.journal.Head(f.ctx)
	if head.Floor.Sequence != 1 {
		t.Fatal("graph ACK did not pin", head)
	}
	request.Action = "ack"
	request.Ack = 2
	if _, e = c.ManageConsumer(f.ctx, request); e != nil {
		t.Fatal(e)
	}
	if floor, e := a.Prune(f.ctx); e != nil || floor != 2 {
		t.Fatal(floor, e)
	}
	request.Ack = 1
	if _, e = c.ManageConsumer(f.ctx, request); !errors.Is(e, ErrInvalidSubscription) {
		t.Fatal("ACK regressed", e)
	}
	request.Ack = 2
	request.Session = "other-session-123456"
	if _, e = c.ManageConsumer(f.ctx, request); !errors.Is(e, ErrFenced) {
		t.Fatal("old session accepted", e)
	}
	request.Action = "acquire"
	if _, e = c.ManageConsumer(f.ctx, request); !errors.Is(e, ErrConflict) {
		t.Fatal("live consumer stolen", e)
	}
	// Advance authoritative fake TSO beyond both leases, but not Worker leases.
	f.db.nowMillis = lease.Leases["sink"].Expires + 1
	request.Ack = 2
	current, e := c.ManageConsumer(f.ctx, request)
	if e != nil {
		t.Fatal(e)
	}
	if current.Leases["sink"].Epoch <= lease.Leases["sink"].Epoch {
		t.Fatal("epoch reused")
	}
	request.Action = "ack"
	request.Session = "session-one-123456"
	if _, e = c.ManageConsumer(f.ctx, request); !errors.Is(e, ErrFenced) {
		t.Fatal("expired session renewed", e)
	}
	request.Session = "other-session-123456"
	request.Epoch = current.Leases["sink"].Epoch
	request.Action = "release"
	if _, e = c.ManageConsumer(f.ctx, request); e != nil {
		t.Fatal(e)
	}
	if floor, e := a.Prune(f.ctx); e != nil || floor != 3 {
		t.Fatal(floor, e)
	}
	old := ConsumerRequest{Partition: "index-0000", Stream: "data", Consumer: "new", Session: "new-session-123456", Action: "acquire"}
	if _, e = c.ManageConsumer(f.ctx, old); !errors.Is(e, ErrLogCompacted) {
		t.Fatal("acquired compacted position", e)
	}
}
func TestGraphGCAndLaggingRecovery(t *testing.T) {
	f := newGraphFixture(t)
	c := retentionCluster(f)
	a, b := f.services[0], f.services[1]
	f.write(t, "first", false)
	if e := a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	entry, e := a.graphReplica.journal.Entry(f.ctx, 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.Checkpoint(f.ctx); e != nil {
		t.Fatal(e)
	}
	// B has never consumed the graph and must recover from the surviving snapshot.
	if _, e = a.Prune(f.ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = f.objects.loadBytes(f.ctx, entry.Object); e == nil {
		t.Fatal("retired payload retained")
	}
	page, e := c.ReadChanges(f.ctx, ChangeRequest{Partition: "index-0000", Stream: "graph"})
	if e != nil || !page.ResetRequired || page.Next != 0 || page.CheckpointPosition.Sequence != 1 {
		t.Fatal(page, e)
	}
	if e = b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	f.write(t, "tail", false)
	if e = a.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e = b.Sync(f.ctx); e != nil {
		t.Fatal(e)
	}
	equalGraph(t, a.Index, b.Index)
	// Resume after the retained floor, never misinterpret a data offset as a graph sequence.
	page, e = c.ReadChanges(f.ctx, ChangeRequest{Partition: "index-0000", Stream: "graph", After: 1})
	if e != nil || len(page.Graph) != 1 || page.Next != 2 {
		t.Fatal(page, e)
	}
}
func TestOrphanObjectTombstone(t *testing.T) {
	f := newGraphFixture(t)
	retentionCluster(f)
	g := f.services[0].graphReplica
	m, e := g.saveBlob(f.ctx, deltaFormat, []byte("orphan upload"))
	if e != nil {
		t.Fatal(e)
	}
	tx, _ := f.r.begin()
	var tracked trackedObject
	if e = readJSON(f.ctx, tx, g.journal.objectKey(m), &tracked); e != nil {
		t.Fatal(e)
	}
	_ = tx.Rollback()
	// Expire only the object intent so the cluster ownership remains valid.
	tx, _ = f.r.begin()
	tracked.Expires = txnMillis(tx) - 1
	_ = putJSON(tx, g.journal.objectKey(m), tracked)
	if e = tx.Commit(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e = g.journal.CollectObjects(f.ctx, g.repo, g.objects, 128); e != nil {
		t.Fatal(e)
	}
	if _, e = f.objects.loadBytes(f.ctx, m); e == nil {
		t.Fatal("orphan was not deleted")
	}
	tx, _ = f.r.begin()
	if e = g.journal.adoptObject(f.ctx, tx, m); !errors.Is(e, ErrFenced) {
		t.Fatal("garbage object resurrected", e)
	}
	_ = tx.Rollback()
	// Simulate a delayed PUT arriving after DELETE. The permanent tombstone causes
	// a later sweep to delete it again, without bucket-wide object discovery.
	_ = f.objects.Client.Put(f.ctx, f.objects.Bucket, m.Key, []byte("orphan upload"))
	tx, _ = f.r.begin()
	_ = readJSON(f.ctx, tx, g.journal.objectKey(m), &tracked)
	tracked.NextAttempt = 0
	_ = putJSON(tx, g.journal.objectKey(m), tracked)
	if e = tx.Commit(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e = g.journal.CollectObjects(context.Background(), g.repo, g.objects, 128); e != nil {
		t.Fatal(e)
	}
	if _, e = f.objects.loadBytes(f.ctx, m); e == nil {
		t.Fatal("late upload survived retry")
	}
}
func TestRetentionProtocolIsolation(t *testing.T) {
	r, _ := newFakeRepo()
	ctx := context.Background()
	if e := r.ensureGraphProtocol(ctx, "hash", "node", true); e != nil {
		t.Fatal(e)
	}
	if e := r.ensureGraphMode(ctx, "hash", "node"); e == nil {
		t.Fatal("older protocol admitted")
	}
}

type failingDeleteBlobs struct {
	*partitionBlobs
	fail bool
}

func (b *failingDeleteBlobs) Delete(ctx context.Context, bucket, key string) error {
	if b.fail {
		return injected
	}
	return b.partitionBlobs.Delete(ctx, bucket, key)
}
func TestObjectDeletionFailureIsRetryable(t *testing.T) {
	f := newGraphFixture(t)
	retentionCluster(f)
	g := f.services[0].graphReplica
	m, e := g.saveBlob(f.ctx, deltaFormat, []byte("orphan"))
	if e != nil {
		t.Fatal(e)
	}
	tx, _ := f.r.begin()
	var o trackedObject
	_ = readJSON(f.ctx, tx, g.journal.objectKey(m), &o)
	o.Expires = 0
	_ = putJSON(tx, g.journal.objectKey(m), o)
	_ = tx.Commit(f.ctx)
	failing := &failingDeleteBlobs{partitionBlobs: f.objects.Client.(*partitionBlobs), fail: true}
	f.objects.Client = failing
	if e = g.journal.CollectObjects(f.ctx, g.repo, g.objects, 128); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	tx, _ = f.r.begin()
	if e = readJSON(f.ctx, tx, g.journal.objectKey(m), &o); e != nil {
		t.Fatal(e)
	}
	if o.State != "garbage" {
		t.Fatal(o)
	}
	o.NextAttempt = 0
	_ = putJSON(tx, g.journal.objectKey(m), o)
	_ = tx.Commit(f.ctx)
	failing.fail = false
	if e = g.journal.CollectObjects(f.ctx, g.repo, g.objects, 128); e != nil {
		t.Fatal(e)
	}
	if _, e = f.objects.loadBytes(f.ctx, m); e == nil {
		t.Fatal("retry did not delete")
	}
}
func TestUploadIntentPersistsWhenPutFails(t *testing.T) {
	f := newGraphFixture(t)
	retentionCluster(f)
	g := f.services[0].graphReplica
	f.objects.Client = &fakeBlobs{exists: true, putErr: injected}
	if _, e := g.saveBlob(f.ctx, deltaFormat, []byte("failed")); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	tx, _ := f.r.begin()
	defer tx.Rollback()
	it, e := tx.Iter(g.journal.key("objects/"), g.journal.key("objects0"))
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatal("upload failed without a durable intent")
	}
	var o trackedObject
	if e = json.Unmarshal(it.Value(), &o); e != nil || o.State != "staged" {
		t.Fatal(o, e)
	}
}
