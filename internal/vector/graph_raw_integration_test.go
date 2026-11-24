package vector

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

type graphRawWriteProbe struct {
	graphRawClient
	keys      [][]byte
	afterLast func() error
}

func (p *graphRawWriteProbe) BatchPut(ctx context.Context, keys, values [][]byte) error {
	for _, key := range keys {
		p.keys = append(p.keys, bytes.Clone(key))
	}
	return p.graphRawClient.BatchPut(ctx, keys, values)
}

func (p *graphRawWriteProbe) last(ctx context.Context, start, end []byte) ([]byte, []byte, error) {
	key, value, err := p.graphRawClient.last(ctx, start, end)
	if err == nil && p.afterLast != nil {
		err = p.afterLast()
	}
	return key, value, err
}

func TestGraphRawTiKVAppendAndSeal(t *testing.T) {
	f := newEpochTiKVFixture(t, 1)
	c, svc := f.c, f.services[0]
	g := svc.graphReplica
	c.coordinator.holdReleasedLeases = true
	store, err := openRawDataStore(f.ctx, f.r.pd, f.r.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	probe := &graphRawWriteProbe{graphRawClient: store.client.(graphRawClient)}
	c.coordinator.rawGraph = probe
	c.token = "integration-raw-graph-token"
	c.replicas = 2
	enableNativeLease(t, f)
	if _, err = c.cachedSearchRoutes(f.ctx, true); err != nil {
		t.Fatal(err)
	}

	first := f.delivery(t, false)
	first.verified = true
	if err = g.enqueue(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	// A healthy publisher has already recovered its lease and head. Opening
	// a transactional snapshot during ordinary append would fail this step.
	f.r.beginTxn = func() (transactionAPI, error) {
		return nil, errors.New("ordinary RawKV graph append opened a transaction")
	}
	err = svc.Sync(f.ctx)
	f.r.beginTxn = nil
	if err != nil {
		t.Fatal(err)
	}
	old, err := g.journal.Head(f.ctx)
	if err != nil || old.Committed.Sequence != 1 || old.SourceEpochs["0"] != f.source.Epoch ||
		old.RawActive == nil || len(probe.keys) != 1 ||
		!bytes.Equal(probe.keys[0], g.journal.rawStream(old.RawActive.ID).Key(1)) {
		t.Fatal(old, probe.keys, err)
	}
	_, token, err := g.writer.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	probe.afterLast = func() error { return g.journal.closeRawRun(f.ctx, token) }
	_, err = g.journal.Head(f.ctx)
	probe.afterLast = nil
	if !errors.Is(err, ErrConflict) {
		t.Fatal("a reader accepted a tail sampled during closure", err)
	}
	// A replica can restore the known prefix after a crash during closure.
	boot, err := newGraphService(f.ctx, g.repo, f.c.objects, g.journal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(boot.Index.graph.Close)
	g.writer.invalidate()
	if err = svc.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	h, err := g.journal.Head(f.ctx)
	if err != nil || h.RawActive == nil || h.RawActive.ID == old.RawActive.ID ||
		h.Committed != old.Committed || len(h.RawSealed) != 1 {
		t.Fatal("recovery did not seal the old run", h, err)
	}

	// A delayed write beyond the seal cannot become the new tail, even
	// though the old RawKV key can still physically reach storage.
	key := g.journal.rawStream(old.RawActive.ID).Key(2)
	if err = store.client.BatchPut(f.ctx, [][]byte{key}, [][]byte{[]byte("late old write")}); err != nil {
		t.Fatal(err)
	}
	if err = svc.Rebuild(f.ctx); err != nil {
		t.Fatal(err)
	}
	reset, err := g.journal.Entry(f.ctx, 2)
	if err != nil || reset.Kind != "reset" || reset.Previous != old.Committed {
		t.Fatal(reset, err)
	}

	record, err := f.r.Mutate(f.ctx, "v", Mutation{RequestID: "second", Vector: []float64{1, 0}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = svc.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Previous, second.Through = 1, 2
	second.Events = []LogEvent{{Sequence: 2, Record: record}}
	if err = g.enqueue(f.ctx, second); err != nil {
		t.Fatal(err)
	}
	if err = svc.Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	h, err = g.journal.Head(f.ctx)
	if err != nil || h.Committed.Sequence != 3 || h.Sources["0"] != 2 || len(probe.keys) != 2 {
		t.Fatal(h, probe.keys, err)
	}
	if err = boot.graphReplica.catchUp(f.ctx, boot, h.Committed); err != nil {
		t.Fatal("follower could not replay across RawKV and reset", err)
	}
	if _, err = svc.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Prune(f.ctx); err != nil {
		t.Fatal(err)
	}
	if h, err = g.journal.Head(f.ctx); err != nil || h.Floor != h.Committed {
		t.Fatal("checkpoint did not advance the retention floor", h, err)
	}
}
