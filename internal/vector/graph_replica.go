package vector

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math/rand"
	"sync"
)

type graphBlobs interface {
	GraphObjectAPI
	saveTracked(context.Context, string, int, string, []byte, func(Manifest) error) (Manifest, error)
	removeObject(context.Context, string) error
	saveBytes(context.Context, string, int, string, []byte) (Manifest, error)
	loadBytes(context.Context, Manifest) ([]byte, error)
}
type graphReplica struct {
	writer       *indexWriter
	fastAppend   bool // Under Service.syncMu and writer.op.
	input        *routedInput
	retention    bool
	checkpointMu sync.Mutex
	viewMu       sync.RWMutex
	journal      graphJournal
	repo         *Repository
	objects      graphBlobs
	position     GraphPosition
	prepared     GraphPosition
	delta        *GraphDelta
	reset        *IndexSnapshot
	leader       bool // Last authoritative plan read; guarded by Service.syncMu.
}

func newGraphService(ctx context.Context, r *Repository, o ObjectAPI, j graphJournal) (*Service, error) {
	blobs, ok := o.(graphBlobs)
	if !ok {
		return nil, errors.New("graph journal object adapter unavailable")
	}
	if e := o.Ensure(ctx); e != nil {
		return nil, e
	}
	idx, e := NewHNSWIndex(r.Dimension, DefaultIndexConfig())
	if e != nil {
		return nil, e
	}
	g := &graphReplica{journal: j, repo: r, objects: blobs}
	s := &Service{Repo: r, Objects: o, Index: idx, graphReplica: g}
	success := false
	defer func() {
		if !success {
			s.Index.graph.Close()
		}
	}()
	head, e := j.Head(ctx)
	if e != nil {
		return nil, e
	}
	if head.Checkpoint != nil {
		state, err := blobs.LoadState(ctx, *head.Checkpoint)
		if err != nil {
			return nil, err
		}
		if (g.journal.c.referenceLogs && (state.LogBytes != head.CheckpointBytes || !maps.Equal(state.Sources, head.CheckpointSources))) || !maps.Equal(state.SourceEpochs, head.CheckpointSourceEpochs) || state.GraphSequence != head.CheckpointPosition.Sequence || state.GraphHash != head.CheckpointPosition.Hash || state.Offset != head.CheckpointPosition.DataOffset || len(state.GraphState) == 0 {
			return nil, errors.New("graph checkpoint position mismatch")
		}
		fresh, err := RestoreHNSW(state)
		if err != nil {
			return nil, err
		}
		s.Index.graph.Close()
		s.Index = fresh
		g.position = head.CheckpointPosition
		s.cursor = g.position.DataOffset
	}
	if e = g.catchUp(ctx, s, head.Visible); e != nil {
		return nil, e
	}
	success = true
	return s, nil
}
func (g *graphReplica) loadEntry(ctx context.Context, e GraphEntry) (_ *GraphDelta, _ *IndexSnapshot, err error) {
	defer g.track("graph_load")(&err)
	if g.input != nil {
		p := &g.input.cluster.observations.pipeline
		p.init()
		gauge := p.graphLoads.WithLabelValues(string(g.journal.id))
		gauge.Inc()
		defer gauge.Dec()
	}
	if e.Kind == sourceEpochEntry {
		if e.Payload != nil || e.Object.Key != "" || e.Position.DataOffset != e.Previous.DataOffset || len(e.SourceEpochs) == 0 {
			return nil, nil, errors.New("invalid source epoch barrier")
		}
		return nil, nil, nil
	}
	if e.Kind == "reset" {
		state, err := g.objects.LoadState(ctx, e.Object)
		if err != nil {
			return nil, nil, err
		}
		if state.Offset != e.Position.DataOffset || len(state.GraphState) == 0 {
			return nil, nil, errors.New("graph reset mismatch")
		}
		return nil, &state, nil
	}
	if e.Kind == "delta" && e.Payload != nil {
		d, err := g.loadReferenceEntry(ctx, e)
		return d, nil, err
	}
	if e.Kind != "delta" || e.Object.Format != deltaFormat {
		return nil, nil, errors.New("unknown graph entry kind")
	}
	data, err := g.objects.loadBytes(ctx, e.Object)
	if err != nil {
		return nil, nil, err
	}
	var delta GraphDelta
	err = json.Unmarshal(data, &delta)
	return &delta, nil, err
}

// Decode and allocate a replacement graph while queries keep using the old
// view. Only installation and position publication need the query-view lock.
func (g *graphReplica) apply(s *Service, d *GraphDelta, state *IndexSnapshot, pos GraphPosition) (err error) {
	defer g.track("graph_apply")(&err)
	var fresh *HNSWIndex
	var e error
	if state != nil {
		fresh, e = RestoreHNSW(*state)
		if e != nil {
			return e
		}
	}
	g.viewMu.Lock()
	defer g.viewMu.Unlock()
	if fresh != nil {
		s.Index.mu.Lock()
		s.Index.replaceGraphLocked(fresh)
		s.Index.mu.Unlock()
	} else if d != nil {
		if e = s.Index.ApplyDelta(*d, false); e != nil {
			return e
		}
	}
	g.position = pos
	s.cursor = pos.DataOffset
	return nil
}
func (g *graphReplica) catchUp(ctx context.Context, s *Service, target GraphPosition) (err error) {
	defer g.track("graph_catchup")(&err)
	if g.position.Sequence > target.Sequence {
		return ErrNotVisible
	}
	for g.position.Sequence < target.Sequence {
		if e := ctx.Err(); e != nil {
			return e
		}
		entries, e := g.journal.replayEntries(ctx, g.position, target)
		if e == nil {
			e = g.replayBatch(ctx, s, entries)
		}
		if e != nil {
			// Compaction can race both metadata reads and object downloads.
			// Only a durable floor beyond the applied prefix permits a reset.
			head, err := g.readHead(ctx)
			if err != nil {
				return err
			}
			if errors.Is(e, ErrLogCompacted) || g.position.Sequence < head.Floor.Sequence {
				target, e = g.restoreCheckpoint(ctx, s)
				if e != nil {
					return e
				}
				continue
			}
			return e
		}
	}
	if g.position != target {
		return errors.New("graph durable hash mismatch")
	}
	return nil
}

func (g *graphReplica) readHead(ctx context.Context) (_ GraphHead, err error) {
	defer g.track("graph_head_read")(&err)
	return g.journal.Head(ctx)
}

// Validation is speculative: apply and roll back before durable append. Once
// append succeeds, every replica may replay this exact patch independently.
func (g *graphReplica) validateDelta(s *Service, d GraphDelta) (err error) {
	defer g.track("graph_prepare_delta")(&err)
	return s.Index.PrepareDelta(d)
}

// Called under Service.syncMu. A follower consumes graph entries only. A newly
// elected leader completes a committed entry before touching the data journal.
func (g *graphReplica) sync(ctx context.Context, s *Service, rebuild bool) error {
	if g.writer != nil {
		return g.syncLeased(ctx, s, rebuild)
	}
	return g.syncSlow(ctx, s, rebuild)
}

func (g *graphReplica) syncSlow(ctx context.Context, s *Service, rebuild bool) (err error) {
	defer g.track("graph_sync")(&err)
	// Explicit sync/rebuild paths can fail after a durable commit. Wake the
	// consumer even if no new data arrives so durable replay can recover.
	defer func() {
		if err != nil && g.input != nil {
			g.input.notify()
		}
	}()
	head, e := g.readHead(ctx)
	if e != nil {
		return e
	}
	if e = g.catchUp(ctx, s, head.Visible); e != nil {
		return e
	}
	plan, e := g.journal.c.Plan(ctx, g.journal.id)
	if e != nil {
		return e
	}
	leader := plan.Publisher == g.journal.owner.Partition
	g.leader = leader
	if rebuild && !leader {
		return ErrFenced
	}
	if leader {
		head, e = g.journal.Claim(ctx)
		if e != nil {
			return e
		}
		if e = g.catchUp(ctx, s, head.Visible); e != nil {
			return e
		}
	}
	if leader {
		if rebuild {
			fresh, err := NewHNSWIndex(s.Index.dimension, s.Index.config)
			if err != nil {
				return err
			}
			defer fresh.graph.Close()
			records := s.Index.Snapshot()
			rand.New(rand.NewSource(42)).Shuffle(len(records), func(a, b int) { records[a], records[b] = records[b], records[a] })
			for _, r := range records {
				if err = ctx.Err(); err != nil {
					return err
				}
				if err = fresh.Apply([]Record{r}); err != nil {
					return err
				}
			}
			state, err := fresh.ExportState()
			if err != nil {
				return err
			}
			state.Offset = s.cursor
			state.LogVersion = LogVersion
			// Verify that a persisted reset can be restored before accepting it.
			verified, err := RestoreHNSW(state)
			if err != nil {
				return err
			}
			verified.graph.Close()
			m, err := g.saveState(ctx, state)
			if err != nil {
				return err
			}
			entry, err := g.journal.Commit(ctx, head, s.cursor, "reset", m)
			if err != nil {
				return err
			}
			g.prepared, g.delta, g.reset = entry.Position, nil, &state
		} else if g.input != nil {
			if e = g.commitRouted(ctx, s, head); e != nil {
				return e
			}
		} else {
			batch, err := g.repo.ReadEvents(ctx, head.Committed.DataOffset, 0, LogBatchSize)
			if err != nil {
				return err
			}
			if len(batch.Events) == 0 {
				return nil
			}
			records := make([]Record, len(batch.Events))
			for n, event := range batch.Events {
				if event.Sequence != head.Committed.DataOffset+uint64(n)+1 {
					return errors.New("graph input log gap")
				}
				records[n] = event.Record
			}
			d, err := s.Index.GenerateDelta(records)
			if err != nil {
				return err
			}
			if err = g.appendDelta(ctx, s, head, batch.Events[len(batch.Events)-1].Sequence, d, nil, batch.Events...); err != nil {
				return err
			}
		}
		head, e = g.readHead(ctx)
		if e != nil {
			return e
		}
	}
	return g.catchUp(ctx, s, head.Visible)
}

func (g *graphReplica) checkpoint(ctx context.Context, s *Service) (Manifest, error) {
	g.checkpointMu.Lock()
	defer g.checkpointMu.Unlock()
	// Capture at S; uploads must not block ingestion of later entries.
	s.syncMu.Lock()
	head, e := g.readHead(ctx)
	if e == nil {
		e = g.catchUp(ctx, s, head.Visible)
	}
	if e == nil && g.journal.c.referenceLogs && head.Checkpoint != nil && head.CheckpointPosition == g.position {
		// An automatic due check may have raced a completed manual checkpoint.
		// Reuse its image instead of uploading another generation while idle.
		tx, err := g.repo.begin()
		if err == nil {
			err = g.journal.c.ReplicaFence(g.journal.id, g.journal.owner)(ctx, tx)
			_ = tx.Rollback()
		}
		s.syncMu.Unlock()
		return *head.Checkpoint, err
	}
	var state IndexSnapshot
	if e == nil {
		state, e = s.Index.ExportState()
	}
	if e == nil {
		state.LogVersion = LogVersion
		state.Offset = g.position.DataOffset
		state.GraphSequence = g.position.Sequence
		state.GraphHash = g.position.Hash
		state.Sources = maps.Clone(head.Sources)
		state.SourceEpochs = maps.Clone(head.SourceEpochs)
		state.LogBytes = head.LogBytes
	}
	s.syncMu.Unlock()
	if e != nil {
		return Manifest{}, e
	}
	m, e := g.saveState(ctx, state)
	if e != nil {
		return m, e
	}
	if g.journal.c.referenceLogs {
		// Upload is outside syncMu, so ongoing ingestion can conflict on the
		// ownership fence. Retry only definite transaction aborts, reusing S and
		// the validated object; never repeat the upload or guess an unknown commit.
		e = retryIndexCommit(ctx, func() error {
			return g.journal.saveCheckpoint(ctx, head.Committed, m, g.repo, &head)
		}, func() {})
		return m, e
	}
	return m, g.journal.SaveCheckpoint(ctx, head.Committed, m, g.repo)
}

func (s *Service) searchGraphLocal(ctx context.Context, q Search) ([]Hit, GraphPosition, error) {
	g := s.graphReplica
	if g.input != nil {
		return s.searchRouted(ctx, q)
	}
	if q.MinVisible != nil {
		s.syncMu.Lock()
		head, e := g.readHead(ctx)
		if e == nil {
			e = g.catchUp(ctx, s, head.Visible)
		}
		s.syncMu.Unlock()
		if e != nil {
			return nil, GraphPosition{}, e
		}
	}
	g.viewMu.RLock()
	defer g.viewMu.RUnlock()
	hits, e := s.Index.Search(q)
	return hits, g.position, e
}

// syncGraphThrough preserves the explicit Sync contract: consume a finite data
// head captured at entry. Each Sync call is bounded to one graph batch; the
// routed consumer repeats calls without waiting while its queue has input.
func (s *Service) syncGraphThrough(ctx context.Context) error {
	if s.graphReplica.input != nil {
		return s.Sync(ctx)
	}
	tx, e := s.graphReplica.repo.begin()
	if e != nil {
		return e
	}
	target, e := s.graphReplica.repo.readCounter(ctx, tx, "log-head")
	_ = tx.Rollback()
	if e != nil {
		return e
	}
	for s.Cursor() < target {
		before := s.Cursor()
		if e = s.Sync(ctx); e != nil {
			return e
		}
		if s.Cursor() == before {
			return ErrNotVisible
		}
	}
	return nil
}

func (g *graphReplica) saveBlob(ctx context.Context, format string, data []byte) (_ Manifest, err error) {
	defer g.track("graph_object_save")(&err)
	if !g.retention {
		return g.objects.saveBytes(ctx, g.repo.Namespace(), g.repo.Dimension, format, data)
	}
	return g.objects.saveTracked(ctx, g.repo.Namespace(), g.repo.Dimension, format, data, func(m Manifest) (err error) {
		defer g.track("graph_object_intent")(&err)
		return retryIndexCommit(ctx, func() error { return g.journal.stageObject(ctx, g.repo, m) }, func() {})
	})
}
func (g *graphReplica) saveState(ctx context.Context, state IndexSnapshot) (Manifest, error) {
	data, e := json.Marshal(state)
	if e != nil {
		return Manifest{}, e
	}
	m, e := g.saveBlob(ctx, state.Format, data)
	m.LogVersion = state.LogVersion
	m.Offset = state.Offset
	return m, e
}

func (g *graphReplica) restoreCheckpoint(ctx context.Context, s *Service) (GraphPosition, error) {
	h, e := g.readHead(ctx)
	if e != nil {
		return GraphPosition{}, e
	}
	if h.Checkpoint == nil || h.CheckpointPosition.Sequence <= g.position.Sequence || h.CheckpointPosition.Sequence < h.Floor.Sequence {
		return GraphPosition{}, errors.New("graph floor without recovery checkpoint")
	}
	state, e := g.objects.LoadState(ctx, *h.Checkpoint)
	if e != nil {
		return GraphPosition{}, e
	}
	if (g.journal.c.referenceLogs && (state.LogBytes != h.CheckpointBytes || !maps.Equal(state.Sources, h.CheckpointSources))) || !maps.Equal(state.SourceEpochs, h.CheckpointSourceEpochs) || state.GraphSequence != h.CheckpointPosition.Sequence || state.GraphHash != h.CheckpointPosition.Hash || state.Offset != h.CheckpointPosition.DataOffset || len(state.GraphState) == 0 {
		return GraphPosition{}, errors.New("recovery checkpoint position mismatch")
	}
	if e = g.apply(s, nil, &state, h.CheckpointPosition); e != nil {
		return GraphPosition{}, e
	}
	g.delta = nil
	g.reset = nil
	g.prepared = GraphPosition{}
	return h.Visible, nil
}
