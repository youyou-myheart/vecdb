package vector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func nextSourceOwner(t *testing.T, f *graphFixture, old Ownership) Ownership {
	t.Helper()
	if _, err := f.c.Update(f.ctx, old, false, true); err != nil {
		t.Fatal(err)
	}
	for _, node := range f.nodes {
		if node.ID == old.Node && node.Session == old.Session {
			owner, err := f.c.Acquire(f.ctx, old.Partition, node)
			if err != nil {
				t.Fatal(err)
			}
			owner, err = f.c.Update(f.ctx, owner, true, false)
			if err != nil {
				t.Fatal(err)
			}
			if owner.Epoch <= old.Epoch {
				t.Fatal("epoch did not advance")
			}
			return owner
		}
	}
	t.Fatal("source node missing")
	return Ownership{}
}
func epochHead(t *testing.T, f *graphFixture, g *graphReplica) GraphHead {
	t.Helper()
	h, err := g.journal.Head(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func requireEpochRejection(t *testing.T, err error, epoch, commit uint64) {
	t.Helper()
	var rejected *SourceEpochError
	if !errors.Is(err, ErrFenced) || !errors.As(err, &rejected) || rejected.Source != 0 || rejected.Epoch != epoch || rejected.Committed != commit {
		t.Fatal("wrong durable epoch rejection", err)
	}
}

func TestSourceEpochBarrierReplacesQueuedWorkAndFencesInFlight(t *testing.T) {
	f, c, owner := routedFixture(t)
	enableReferences(f)
	c.graphReplication = true
	a, b := f.services[0], f.services[1]
	g := a.graphReplica
	f.write(t, "first", false)
	q := deliveryFor(t, f, owner, 0)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	base := epochHead(t, f, g)
	if base.SourceEpochs["0"] != owner.Epoch || base.Sources["0"] != 0 || base.Committed.Sequence != 1 || base.Committed.DataOffset != 0 {
		t.Fatal(base)
	}
	before, _ := a.Index.ExportState()
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
	// Hold already validated/built work while a new source owner is admitted.
	delta, err := a.Index.GenerateDelta([]Record{q.Events[0].Record})
	if err != nil {
		t.Fatal(err)
	}
	newer := nextSourceOwner(t, f, owner)
	fresh := q
	fresh.Owner = newer
	if err := g.enqueue(f.ctx, fresh); err != nil {
		t.Fatal(err)
	}
	h := epochHead(t, f, g)
	if h.Committed.Sequence != base.Committed.Sequence+1 || h.Sources["0"] != 0 || h.SourceEpochs["0"] != newer.Epoch || g.input.queue[0].Owner.Epoch != newer.Epoch {
		t.Fatal(h)
	}
	for n, epoch := range []uint64{owner.Epoch, newer.Epoch} {
		page, e := c.ReadChanges(f.ctx, ChangeRequest{Partition: g.journal.id, Stream: "graph", After: uint64(n), Limit: 1})
		if e != nil || page.Next != uint64(n+1) || len(page.Graph) != 1 {
			t.Fatal(page, e)
		}
		entry := page.Graph[0]
		if entry.Kind != sourceEpochEntry || entry.Payload != nil || len(entry.PayloadData) != 0 || entry.Object.Key != "" || entry.SourceEpochs["0"] != epoch || entry.Sources["0"] != 0 || entry.Position.DataOffset != 0 {
			t.Fatal("epoch subscription changed data/HNSW", entry)
		}
	}
	requireEpochRejection(t, g.appendDelta(f.ctx, a, base, 1, delta, []Delivery{q}), newer.Epoch, 0)
	requireEpochRejection(t, g.enqueue(f.ctx, q), newer.Epoch, 0)
	unchanged, _ := a.Index.ExportState()
	if !reflect.DeepEqual(before, unchanged) {
		t.Fatal("rejected delta mutated HNSW")
	}
	if err := a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
	h = epochHead(t, f, g)
	if h.Sources["0"] != 1 || h.Committed.Sequence != 3 {
		t.Fatal(h)
	}
	requireEpochRejection(t, g.enqueue(f.ctx, q), newer.Epoch, 1)
	if g.input.bytes != 0 || g.input.hasQueued() {
		t.Fatal("queue accounting leaked")
	}
	// Wire response carries the same durable epoch/LSN (including duplicate rejection).
	c.locals = map[IndexPartitionID]*localPartition{g.journal.owner.Partition: {owner: g.journal.owner, service: a}}
	reqBody, _ := json.Marshal(partitionRPC{Namespace: c.root.Prefix, Partition: g.journal.owner.Partition, Epoch: g.journal.owner.Epoch, Operation: "deliver", Delivery: &q})
	request := httptest.NewRequest("POST", "/internal/partition", bytes.NewReader(reqBody))
	request.Header.Set("Authorization", "Bearer "+c.token)
	response := httptest.NewRecorder()
	c.InternalHandler().ServeHTTP(response, request)
	var reply partitionReply
	if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if response.Code != 503 || reply.Code != "fenced" || reply.SourceEpoch == nil {
		t.Fatal(response.Code, reply)
	}
	requireEpochRejection(t, reply.SourceEpoch, newer.Epoch, 1)
	metrics, err := c.LocalMetrics()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics, `vecdb_index_source_epoch_rejections_total{index_partition="index-0000",phase="commit"} 1`) || !strings.Contains(metrics, `vecdb_index_source_epoch_rejections_total{index_partition="index-0000",phase="admission"} 3`) || !strings.Contains(metrics, `vecdb_index_source_epoch_advances_total{index_partition="index-0000",source="0"} 2`) {
		t.Fatal("epoch advances not observable")
	}
}

func TestSourceEpochAuthorizationAndAtomicOutcome(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			f, _, owner := routedFixture(t)
			enableReferences(f)
			g := f.services[0].graphReplica
			f.write(t, "v", false)
			q := deliveryFor(t, f, owner, 0)
			for _, epoch := range []uint64{0, owner.Epoch + 100} {
				bad := q
				bad.Owner.Epoch = epoch
				if err := g.enqueue(f.ctx, bad); !errors.Is(err, ErrFenced) {
					t.Fatal("unissued epoch accepted", err)
				}
			}
			if epochHead(t, f, g).Committed.Sequence != 0 {
				t.Fatal("invalid admission changed durable state")
			}
			f.db.commitErr, f.db.committedDespiteError = injected, unknown
			if err := g.enqueue(f.ctx, q); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			f.db.commitErr, f.db.committedDespiteError = nil, false
			h := epochHead(t, f, g)
			want := uint64(0)
			if unknown {
				want = 1
			}
			if h.Committed.Sequence != want || h.Sources["0"] != 0 || h.SourceEpochs["0"] != want || g.input.hasQueued() {
				t.Fatal("partial epoch state", h)
			}
			if err := g.enqueue(f.ctx, q); err != nil {
				t.Fatal(err)
			}
			if h = epochHead(t, f, g); h.Committed.Sequence != 1 {
				t.Fatal("uncertain barrier appended twice", h)
			}
			if err := f.services[0].Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			if h = epochHead(t, f, g); h.Committed.Sequence != 2 || h.Sources["0"] != 1 {
				t.Fatal(h)
			}
		})
	}
}

func TestSourceEpochDuplicateSurvivesGCCheckpointAndLeaderFailover(t *testing.T) {
	f, c, owner := routedFixture(t)
	enableReferences(f)
	c.consumerRetention, c.graphReplication = true, true
	a, b := f.services[0], f.services[1]
	g := a.graphReplica
	f.write(t, "v", false)
	q := deliveryFor(t, f, owner, 0)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := a.Index.ExportState()
	seed(f.db, completedKey(f.r), uint64(1))
	f.r.DataSnapshots = true
	if _, err := a.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.journal.Prune(f.ctx, g.repo, 256); err != nil {
		t.Fatal(err)
	}
	if err := c.maintainData(f.ctx, 0, owner); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.db.data[string(f.r.logKey(1))]; ok {
		t.Fatal("data GC not exercised")
	}
	newer := nextSourceOwner(t, f, owner)
	fresh := q
	fresh.Owner = newer
	if err := g.enqueue(f.ctx, fresh); err != nil {
		t.Fatal("new epoch duplicate should not need compacted data", err)
	}
	h := epochHead(t, f, g)
	if h.SourceEpochs["0"] != newer.Epoch || h.CheckpointSourceEpochs["0"] != owner.Epoch || h.Sources["0"] != 1 || h.Committed.Sequence != 3 || h.Committed.DataOffset != 1 || g.input.hasQueued() {
		t.Fatal(h)
	}
	// Replay epoch-only tail from an older checkpoint; no vector work or data reads.
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
	if _, err := a.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.journal.Prune(f.ctx, g.repo, 256); err != nil {
		t.Fatal(err)
	}
	h = epochHead(t, f, g)
	if h.Floor != h.Committed || !maps.Equal(h.CheckpointSourceEpochs, h.SourceEpochs) {
		t.Fatal(h)
	}
	if _, err := f.c.Update(f.ctx, g.journal.owner, false, true); err != nil {
		t.Fatal(err)
	}
	if err := f.c.ScheduleReplicas(f.ctx, f.master, f.layout, f.nodes, 2); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := newGraphService(f.ctx, b.graphReplica.repo, f.objects, b.graphReplica.journal)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Index.graph.Close()
	restored.graphReplica.input = newRoutedInput(c)
	requireEpochRejection(t, restored.graphReplica.enqueue(f.ctx, q), newer.Epoch, 1)
	if err = restored.graphReplica.enqueue(f.ctx, fresh); err != nil {
		t.Fatal(err)
	}
	after, _ := restored.Index.ExportState()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("epoch-only replay rebuilt graph")
	}
	// Metadata/object disagreement must fail closed on restart.
	h = epochHead(t, f, restored.graphReplica)
	h.CheckpointSourceEpochs = map[string]uint64{"0": owner.Epoch}
	seed(f.db, g.journal.key("head"), h)
	if corrupt, e := newGraphService(f.ctx, b.graphReplica.repo, f.objects, b.graphReplica.journal); e == nil {
		corrupt.Index.graph.Close()
		t.Fatal("checkpoint epoch corruption accepted")
	}
}

func TestSourceEpochBarrierCheckpointWithoutDataThenAppend(t *testing.T) {
	for _, references := range []bool{false, true} {
		t.Run(fmt.Sprint(references), func(t *testing.T) {
			f, _, owner := routedFixture(t)
			if references {
				enableReferences(f)
			}
			a, b := f.services[0], f.services[1]
			g := a.graphReplica
			// A legal empty duplicate accepts a source before its first data event.
			if err := g.enqueue(f.ctx, Delivery{Source: 0, Owner: owner, RouteVersion: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Checkpoint(f.ctx); err != nil {
				t.Fatal(err)
			}
			checkpoint := epochHead(t, f, g)
			f.write(t, "after-control", false)
			if err := g.enqueue(f.ctx, deliveryFor(t, f, owner, 0)); err != nil {
				t.Fatal(err)
			}
			if err := a.Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := b.Sync(f.ctx); err != nil {
				t.Fatal(err)
			}
			equalGraph(t, a.Index, b.Index)
			h := epochHead(t, f, g)
			if h.LogBytes < checkpoint.LogBytes || h.SourceEpochs["0"] != owner.Epoch || h.Sources["0"] != 1 {
				t.Fatal(h)
			}
		})
	}
}
