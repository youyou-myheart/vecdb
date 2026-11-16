package vector

import (
	"context"
	"encoding/json"
	"errors"
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
	input     *routedInput
	retention bool
	viewMu    sync.RWMutex
	journal   graphJournal
	repo      *Repository
	objects   graphBlobs
	position  GraphPosition
	prepared  GraphPosition
	delta     *GraphDelta
	reset     *IndexSnapshot
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
		if state.GraphSequence != head.CheckpointPosition.Sequence || state.GraphHash != head.CheckpointPosition.Hash || state.Offset != head.CheckpointPosition.DataOffset || len(state.GraphState) == 0 {
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
	} else if e = s.Index.ApplyDelta(*d, false); e != nil {
		return e
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
		entry, e := g.journal.Entry(ctx, g.position.Sequence+1)
		if errors.Is(e, ErrLogCompacted) {
			target, e = g.restoreCheckpoint(ctx, s)
			if e != nil {
				return e
			}
			continue
		}
		if e != nil {
			return e
		}
		if entry.Previous != g.position {
			return errors.New("graph journal discontinuity")
		}
		d, state := g.delta, g.reset
		if g.prepared != entry.Position {
			d, state, e = g.loadEntry(ctx, entry)
			if e != nil {
				head, err := g.journal.Head(ctx)
				if err != nil {
					return err
				}
				if g.position.Sequence < head.Floor.Sequence {
					target, e = g.restoreCheckpoint(ctx, s)
					if e != nil {
						return e
					}
					continue
				}
				return e
			}
		}
		if e = g.apply(s, d, state, entry.Position); e != nil {
			return e
		}
		g.delta = nil
		g.reset = nil
		g.prepared = GraphPosition{}
	}
	if g.position != target {
		return errors.New("graph published hash mismatch")
	}
	return nil
}
func (g *graphReplica) prepare(ctx context.Context, s *Service, head GraphHead) (err error) {
	defer g.track("graph_prepare")(&err)
	if head.Committed == head.Visible {
		return nil
	}
	if g.prepared != head.Committed {
		entry, e := g.journal.Entry(ctx, head.Committed.Sequence)
		if e != nil {
			return e
		}
		if entry.Previous != g.position || entry.Position != head.Committed {
			return errors.New("graph pending base mismatch")
		}
		d, state, e := g.loadEntry(ctx, entry)
		if e != nil {
			return e
		}
		if state != nil {
			fresh, err := RestoreHNSW(*state)
			if err != nil {
				return err
			}
			fresh.graph.Close()
		} else {
			if e = s.Index.PrepareDelta(*d); e != nil {
				return e
			}
		}
		g.prepared = entry.Position
		g.delta = d
		g.reset = state
	}
	return g.journal.Ack(ctx, head.Committed)
}

// Called under Service.syncMu. A follower consumes graph entries only. A newly
// elected leader completes a committed entry before touching the data journal.
func (g *graphReplica) sync(ctx context.Context, s *Service, rebuild bool) (err error) {
	defer g.track("graph_sync")(&err)
	head, e := g.journal.Head(ctx)
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
	if rebuild && !leader {
		return ErrFenced
	}
	builtReset := false
	if leader {
		head, e = g.journal.Claim(ctx)
		if e != nil {
			return e
		}
		if e = g.catchUp(ctx, s, head.Visible); e != nil {
			return e
		}
	}
	if head.Committed == head.Visible && leader {
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
			m, err := g.saveState(ctx, state)
			if err != nil {
				return err
			}
			if _, err = g.journal.Commit(ctx, head, s.cursor, "reset", m); err != nil {
				return err
			}
			builtReset = true
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
			data, err := json.Marshal(d)
			if err != nil {
				return err
			}
			m, err := g.saveBlob(ctx, deltaFormat, data)
			if err != nil {
				return err
			}
			if _, err = g.journal.Commit(ctx, head, batch.Events[len(batch.Events)-1].Sequence, "delta", m); err != nil {
				return err
			}
		}
		head, e = g.journal.Head(ctx)
		if e != nil {
			return e
		}
	}
	if head.Committed != head.Visible && leader {
		if e = g.prepare(ctx, s, head); e != nil {
			return e
		}
		if leader {
			publish := g.track("graph_publish")
			_, e = g.journal.Publish(ctx)
			publish(&e)
			if e != nil {
				return e
			}
		}
		head, e = g.journal.Head(ctx)
		if e != nil {
			return e
		}
		if e = g.catchUp(ctx, s, head.Visible); e != nil {
			return e
		}
		if rebuild && !builtReset {
			return g.sync(ctx, s, true)
		}
		return nil
	}
	return nil
}
func (g *graphReplica) checkpoint(ctx context.Context, s *Service) (Manifest, error) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	head, e := g.journal.Head(ctx)
	if e != nil {
		return Manifest{}, e
	}
	if e = g.catchUp(ctx, s, head.Visible); e != nil {
		return Manifest{}, e
	}
	state, e := s.Index.ExportState()
	if e != nil {
		return Manifest{}, e
	}
	state.LogVersion = LogVersion
	state.Offset = s.cursor
	state.GraphSequence = g.position.Sequence
	state.GraphHash = g.position.Hash
	m, e := g.saveState(ctx, state)
	if e != nil {
		return m, e
	}
	return m, g.journal.SaveCheckpoint(ctx, g.position, m, g.repo)
}

func (s *Service) searchGraphLocal(ctx context.Context, q Search) ([]Hit, GraphPosition, error) {
	g := s.graphReplica
	if g.input != nil {
		return s.searchRouted(ctx, q)
	}
	if q.MinVisible != nil {
		s.syncMu.Lock()
		head, e := g.journal.Head(ctx)
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
// head captured at entry. Background Sync remains bounded to one graph batch.
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
		return g.journal.stageObject(ctx, g.repo, m)
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
	h, e := g.journal.Head(ctx)
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
	if state.GraphSequence != h.CheckpointPosition.Sequence || state.GraphHash != h.CheckpointPosition.Hash || state.Offset != h.CheckpointPosition.DataOffset || len(state.GraphState) == 0 {
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
