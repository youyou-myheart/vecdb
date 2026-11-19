package vector

import (
	"context"
	"errors"
	"sync"
)

type RepositoryAPI interface {
	Namespace() string
	VectorDimension() int
	Scan(context.Context, string) ([]Record, error)
	Manifest(context.Context) (*Manifest, error)
	Publish(context.Context, string, Manifest) error
}
type ObjectAPI interface {
	Ensure(context.Context) error
	Save(context.Context, string, int, []Record) (Manifest, error)
	Load(context.Context, Manifest) ([]Record, error)
}
type Service struct {
	graphReplica *graphReplica
	Repo         RepositoryAPI
	Objects      ObjectAPI
	Index        *HNSWIndex
	syncMu       sync.Mutex
	cursor       uint64
}

func NewService(ctx context.Context, r RepositoryAPI, o ObjectAPI) (*Service, error) {
	return newServiceWithProgress(ctx, r, o, nil)
}

func newServiceWithProgress(ctx context.Context, r RepositoryAPI, o ObjectAPI, onBaseline func(uint64)) (*Service, error) {
	if e := o.Ensure(ctx); e != nil {
		return nil, e
	}
	idx, err := NewHNSWIndex(r.VectorDimension(), DefaultIndexConfig())
	if err != nil {
		return nil, err
	}
	s := &Service{Repo: r, Objects: o, Index: idx}
	restored := false
	defer func() {
		if !restored {
			s.Index.graph.Close()
		}
	}()
	m, e := r.Manifest(ctx)
	if e != nil {
		return nil, e
	}
	if m != nil {
		if m.LogVersion != 0 && m.LogVersion != LogVersion {
			return nil, errors.New("unsupported log version")
		}
		if m.Dimension != r.VectorDimension() {
			return nil, errors.New("snapshot dimension mismatch")
		}
		if m.Format != "" {
			graphObjects, ok := o.(GraphObjectAPI)
			if !ok {
				return nil, errors.New("graph snapshot adapter unavailable")
			}
			state, err := graphObjects.LoadState(ctx, *m)
			if err != nil {
				return nil, err
			}
			idx, err := RestoreHNSW(state)
			if err != nil {
				return nil, err
			}
			s.Index.graph.Close()
			s.Index = idx
			if state.LogVersion != m.LogVersion || state.Offset != m.Offset {
				return nil, errors.New("snapshot cursor mismatch")
			}
			s.cursor = state.Offset
		} else {
			records, e := o.Load(ctx, *m)
			if e != nil {
				return nil, e
			}
			for _, rec := range records {
				if rec.ID == "" || rec.Revision == 0 {
					return nil, errors.New("invalid snapshot record")
				}
				if !rec.Deleted {
					if e = ValidateVector(rec.Vector, r.VectorDimension()); e != nil {
						return nil, e
					}
				}
			}
			if e = s.Index.Apply(records); e != nil {
				return nil, e
			}
		}
	}
	if incremental, ok := r.(IncrementalRepository); ok && (m == nil || m.LogVersion != LogVersion) {
		records, head, err := incremental.Bootstrap(ctx)
		if err != nil {
			return nil, err
		}
		if err = s.Index.Apply(records); err != nil {
			return nil, err
		}
		s.cursor = head
	}
	if onBaseline != nil {
		onBaseline(s.cursor)
	}
	if e = s.Sync(ctx); e != nil {
		return nil, e
	}
	restored = true
	return s, nil
}

// Each call consumes a finite head captured at its first batch; writers cannot starve it.
func (s *Service) Sync(ctx context.Context) error {
	wait := func(*error) {}
	if s.graphReplica != nil {
		wait = s.graphReplica.track("graph_sync_lock_wait")
	}
	s.syncMu.Lock()
	wait(nil)
	defer s.syncMu.Unlock()
	if s.graphReplica != nil {
		return s.graphReplica.sync(ctx, s, false)
	}
	if repo, ok := s.Repo.(IncrementalRepository); ok {
		return s.syncIncremental(ctx, repo)
	}
	events, e := s.Repo.Scan(ctx, "outbox")
	if e != nil {
		return e
	}
	for _, rec := range events {
		if rec.ID == "" || rec.Revision == 0 {
			return errors.New("invalid outbox record")
		}
		if !rec.Deleted {
			if err := ValidateVector(rec.Vector, s.Repo.VectorDimension()); err != nil {
				return err
			}
		}
	}
	return s.Index.Apply(events)
}
func (s *Service) Checkpoint(ctx context.Context) (Manifest, error) {
	if s.graphReplica != nil {
		if e := s.syncGraphThrough(ctx); e != nil {
			return Manifest{}, e
		}
		return s.graphReplica.checkpoint(ctx, s)
	}
	old, e := s.Repo.Manifest(ctx)
	if e != nil {
		return Manifest{}, e
	}
	expected := ""
	if old != nil {
		expected = old.Generation
	}
	if e = s.Sync(ctx); e != nil {
		return Manifest{}, e
	}
	var m Manifest
	s.syncMu.Lock()
	if o, ok := s.Objects.(GraphObjectAPI); ok {
		var state IndexSnapshot
		state, e = s.Index.ExportState()
		state.LogVersion = 0
		if _, ok := s.Repo.(IncrementalRepository); ok {
			state.LogVersion = LogVersion
			state.Offset = s.cursor
		}
		s.syncMu.Unlock()
		if e == nil {
			m, e = o.SaveState(ctx, s.Repo.Namespace(), state)
		}
	} else {
		records := s.Index.Snapshot()
		s.syncMu.Unlock()
		m, e = s.Objects.Save(ctx, s.Repo.Namespace(), s.Repo.VectorDimension(), records)
	}
	if e != nil {
		return Manifest{}, e
	}
	e = s.Repo.Publish(ctx, expected, m)
	return m, e
}

func (s *Service) Search(q Search) ([]Hit, error) { return s.Index.Search(q) }

func (s *Service) Rebuild(ctx context.Context) error {
	if s.graphReplica != nil {
		if e := s.syncGraphThrough(ctx); e != nil {
			return e
		}
		s.syncMu.Lock()
		defer s.syncMu.Unlock()
		return s.graphReplica.sync(ctx, s, true)
	}
	if err := s.Sync(ctx); err != nil {
		return err
	}
	return s.Index.Rebuild(ctx)
}
func (s *Service) MaybeRebuild(ctx context.Context) error {
	if g := s.graphReplica; g != nil && g.input != nil {
		s.syncMu.Lock()
		e := g.scrub(ctx, s)
		s.syncMu.Unlock()
		if e != nil {
			return e
		}
	}
	if s.graphReplica != nil && !s.Index.NeedsRebuild() {
		return nil
	}
	if s.graphReplica != nil {
		p, e := s.graphReplica.journal.c.Plan(ctx, s.graphReplica.journal.id)
		if e != nil {
			return e
		}
		if p.Publisher != s.graphReplica.journal.owner.Partition {
			return nil
		}
		if s.Index.NeedsRebuild() {
			return s.Rebuild(ctx)
		}
		return nil
	}
	if s.Index.NeedsRebuild() {
		return s.Index.Rebuild(ctx)
	}
	return nil
}

func (s *Service) syncIncremental(ctx context.Context, repo IncrementalRepository) error {
	var target uint64
	for {
		batch, err := repo.ReadEvents(ctx, s.cursor, target, LogBatchSize)
		if errors.Is(err, ErrLogCompacted) {
			records, head, e := repo.Bootstrap(ctx)
			if e != nil {
				return e
			}
			if e = s.Index.Apply(records); e != nil {
				return e
			}
			s.cursor = head
			return nil
		}
		if err != nil {
			return err
		}
		if target == 0 {
			target = batch.Head
		}
		if len(batch.Events) == 0 {
			if s.cursor < target {
				return errors.New("incomplete log batch")
			}
			return nil
		}
		records := make([]Record, len(batch.Events))
		for j, event := range batch.Events {
			if event.Sequence != s.cursor+uint64(j)+1 {
				return errors.New("non-contiguous log batch")
			}
			records[j] = event.Record
		}
		if err = s.Index.Apply(records); err != nil {
			return err
		}
		s.cursor = batch.Events[len(batch.Events)-1].Sequence
		if s.cursor >= target {
			return nil
		}
	}
}
func (s *Service) Prune(ctx context.Context) (uint64, error) {
	if s.graphReplica != nil {
		s.syncMu.Lock()
		defer s.syncMu.Unlock()
	}
	repo, ok := s.Repo.(IncrementalRepository)
	if !ok {
		return 0, errors.New("incremental log unavailable")
	}
	var floor uint64
	var e error
	g := s.graphReplica
	if g != nil && (g.retention || g.journal.c.referenceLogs) {
		e = retryIndexCommit(ctx, func() error { _, err := g.journal.Prune(ctx, g.repo, LogBatchSize); return err }, func() {})
		if e != nil {
			return floor, e
		}
	}
	if g == nil || g.input == nil {
		if g != nil && g.journal.c.referenceLogs {
			r := *g.repo
			old := r.Retain
			r.Retain = func(ctx context.Context, tx transactionAPI, end uint64) (uint64, error) {
				h, err := g.journal.headIn(ctx, tx)
				if err != nil {
					return 0, err
				}
				end = min(end, h.Floor.DataOffset)
				if old != nil {
					return old(ctx, tx, end)
				}
				return end, nil
			}
			e = retryIndexCommit(ctx, func() error { var err error; floor, err = r.PruneEvents(ctx, LogBatchSize); return err }, func() {})
		} else {
			floor, e = repo.PruneEvents(ctx, LogBatchSize)
		}
	}
	if e == nil && g != nil && (g.retention || g.journal.c.referenceLogs) {
		e = retryIndexCommit(ctx, func() error { return g.journal.CollectObjects(ctx, g.repo, g.objects, 128) }, func() {})
	}
	return floor, e
}

func (s *Service) Cursor() uint64 { s.syncMu.Lock(); defer s.syncMu.Unlock(); return s.cursor }
