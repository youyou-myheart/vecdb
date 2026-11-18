package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type graphWriteAudit struct {
	transactionAPI
	writes *[]string
}

func (t graphWriteAudit) Set(key, value []byte) error {
	*t.writes = append(*t.writes, string(key))
	return t.transactionAPI.Set(key, value)
}

func TestGraphDurableTailReplaysBeforeLeaderApply(t *testing.T) {
	f, c, owner := routedFixture(t)
	enableReferences(f)
	a, b := f.services[0], f.services[1]
	g := a.graphReplica
	f.write(t, "only-entry", false)
	q := deliveryFor(t, f, owner, 0)
	h, err := g.journal.Claim(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	anchor := bytes.Clone(f.db.data[string(g.journal.key("head"))])
	begin := f.r.beginTxn
	var writes []string
	f.r.beginTxn = func() (transactionAPI, error) {
		tx, err := begin()
		return graphWriteAudit{tx, &writes}, err
	}
	if err = g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	h, err = g.journal.Head(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.commitRouted(f.ctx, a, h); err != nil {
		t.Fatal(err)
	}
	for _, key := range writes {
		if key == string(g.journal.c.fenceKey(owner.Partition)) || key == string(g.journal.key("head")) || strings.Contains(key, "/acks/") {
			t.Fatal("append wrote a second publication key", key)
		}
	}
	if !bytes.Equal(anchor, f.db.data[string(g.journal.key("head"))]) {
		t.Fatal("append rewrote anchor")
	}
	h, err = g.journal.Head(f.ctx)
	if err != nil || h.Visible.Sequence != 2 || h.Visible != h.Committed || a.Cursor() != 0 {
		t.Fatal(h, err)
	}
	commits, err := c.sourceCommits(f.ctx, 0, []IndexPartitionID{g.journal.id})
	if err != nil || commits[g.journal.id] != 1 {
		t.Fatal("source ACK missing before leader apply", commits, err)
	}
	// No next entry, leader apply, ACK transaction or publication event exists.
	if err = b.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	hits, pos, err := b.searchGraphLocal(f.ctx, Search{Vector: []float64{1, 2}, K: 1})
	if err != nil || len(hits) != 1 || pos != h.Committed || a.Cursor() != 0 {
		t.Fatal(hits, pos, err)
	}
	if err = a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, a.Index, b.Index)
	// A duplicate delivery after both replicas replayed must not append again.
	if err = g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err = a.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	after, err := g.journal.Head(f.ctx)
	if err != nil || after.Committed != h.Committed {
		t.Fatal(after, err)
	}
}

func TestGraphRejectsInvalidDeltaBeforeDurability(t *testing.T) {
	f, c, owner := routedFixture(t)
	enableReferences(f)
	s := f.services[0]
	g := s.graphReplica
	f.write(t, "invalid", false)
	q := deliveryFor(t, f, owner, 0)
	h, err := g.journal.Claim(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.Index.GenerateDelta([]Record{q.Events[0].Record})
	if err != nil {
		t.Fatal(err)
	}
	d.Native = d.Native[:len(d.Native)-1]
	blobs := &replayTestBlobs{blobAPI: f.objects.Client}
	f.objects.Client = blobs
	if err = g.appendDelta(f.ctx, s, h, 1, d, []Delivery{q}); err == nil {
		t.Fatal("invalid patch accepted")
	}
	after, err := g.journal.Head(f.ctx)
	if err != nil || after.Committed.Sequence != 0 || blobs.gets.Load() != 0 {
		t.Fatal("invalid patch persisted", after, err)
	}
	commits, err := c.sourceCommits(f.ctx, 0, []IndexPartitionID{g.journal.id})
	if err != nil || commits[g.journal.id] != 0 {
		t.Fatal("invalid patch acknowledged", commits, err)
	}
}

func TestGraphSourceWatermarksSurviveCompleteCompaction(t *testing.T) {
	f, c, owner := routedFixture(t)
	enableReferences(f)
	s := f.services[0]
	g := s.graphReplica
	g.retention = true
	for n := uint64(0); n < 3; n++ {
		f.write(t, fmt.Sprint(n), false)
		if err := g.enqueue(f.ctx, deliveryFor(t, f, owner, n)); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(f.ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Checkpoint(f.ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := g.journal.Prune(f.ctx, g.repo, 256); err != nil {
			t.Fatal(err)
		}
		h, err := g.journal.Head(f.ctx)
		if err != nil || h.Floor != h.Committed || h.Sources["0"] != n+1 {
			t.Fatal(h, err)
		}
		commits, err := c.sourceCommits(f.ctx, 0, []IndexPartitionID{g.journal.id})
		if err != nil || commits[g.journal.id] != n+1 {
			t.Fatal(commits, err)
		}
	}
	if err := f.services[1].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	equalGraph(t, s.Index, f.services[1].Index)
}

func TestGraphDurableTailIntegrity(t *testing.T) {
	for _, damage := range []string{"hash", "sources", "key", "missing", "legacy_anchor"} {
		t.Run(damage, func(t *testing.T) {
			f, entries := replayFixture(t, 1)
			g := f.services[0].graphReplica
			// Capture an anchor so a lost tail cannot silently look empty.
			if _, err := f.services[0].Checkpoint(f.ctx); err != nil {
				t.Fatal(err)
			}
			key := string(g.journal.key("entries/00000000000000000001"))
			entry := entries[0]
			switch damage {
			case "hash":
				entry.Position.Hash = "wrong"
				seed(f.db, []byte(key), entry)
			case "sources":
				entry.Sources = map[string]uint64{"0": 999}
				seed(f.db, []byte(key), entry)
			case "key":
				f.db.data[key+"9"] = f.db.data[key]
				delete(f.db.data, key)
			case "missing":
				delete(f.db.data, key)
			case "legacy_anchor":
				var h GraphHead
				if err := json.Unmarshal(f.db.data[string(g.journal.key("head"))], &h); err != nil {
					t.Fatal(err)
				}
				h.Visible = GraphPosition{}
				seed(f.db, g.journal.key("head"), h)
			}
			if _, err := g.journal.Head(f.ctx); err == nil {
				t.Fatal("damaged tail accepted")
			}
		})
	}
}

func TestGraphDurableProtocolRejectsOldNamespaces(t *testing.T) {
	for _, old := range []string{"v8-graph-async-hash-node-2", "v9-graph-retention-hash-node-2", "v10-cluster-routing-hash-node-retention=true", "v11-graph-durable-hash-node-2", "v11-cluster-routing-durable-hash-node-retention=true"} {
		r, db := newFakeRepo()
		db.data[r.Prefix+"cluster-mode"] = []byte(old)
		ctx := context.Background()
		if err := r.ensureGraphProtocol(ctx, "hash", "node", true); err == nil {
			t.Fatal("mixed graph protocols", old)
		}
		if _, err := r.ensureRoutedLayout(ctx, 1, &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}, "hash", "node", true); err == nil {
			t.Fatal("mixed routed protocols", old)
		}
		if string(db.data[r.Prefix+"cluster-mode"]) != old {
			t.Fatal("silently migrated legacy namespace")
		}
	}
}

func TestSourceEpochProtocolRejectsSharedFenceNamespace(t *testing.T) {
	r, db := newFakeRepo()
	old := "v12-cluster-routing-reference-hash-node-retention=true"
	db.data[r.Prefix+"cluster-mode"] = []byte(old)
	if _, err := r.ensureRoutedLayout(context.Background(), 1, &ClusterRouting{Version: 1, Centers: [][]float64{{0, 0}}}, "hash", "node", true); err == nil {
		t.Fatal("v12 writers mixed with per-Index epoch protocol")
	}
	if string(db.data[r.Prefix+"cluster-mode"]) != old {
		t.Fatal("silently changed existing namespace")
	}
}
