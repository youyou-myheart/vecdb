package vector

import "context"

type recallRebuildResult struct {
	Epoch      uint64 // Index epoch at installation.
	DataOffset uint64 // Input covered by the reset. Standalone indexes use Epoch.
}

func (s *Service) rebuildForRecall(ctx context.Context, expectedEpoch, expectedTerm uint64) (recallRebuildResult, error) {
	if s.graphReplica == nil {
		canInstall := func() bool {
			term, ok := s.recallTerm()
			return ok && term == expectedTerm
		}
		if !canInstall() {
			return recallRebuildResult{}, ErrFenced
		}
		if err := s.Index.rebuild(ctx, &expectedEpoch, canInstall); err != nil {
			return recallRebuildResult{}, err
		}
		return recallRebuildResult{Epoch: expectedEpoch, DataOffset: expectedEpoch}, nil
	}

	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	g := s.graphReplica
	// Unmanaged graph services determine publisher status under syncMu;
	// the worker-managed paths read the independently renewed local grant.
	term, ok := uint64(1), g.leader
	if s.publisherTerm != nil || g.writer != nil {
		term, ok = s.recallTerm()
	}
	if !ok || term != expectedTerm {
		return recallRebuildResult{}, ErrFenced
	}
	if g.lastReset.Epoch > expectedEpoch {
		// A manual or replicated reset superseded the low-recall observation.
		return recallRebuildResult{}, ErrConflict
	}
	// Ingestion between the probe and this lock is included in the reset.
	// Use the ordinary durable publisher fence, journal replay, and view swap.
	var err error
	if g.writer != nil {
		// Bind the quality sample to the same publisher grant through reset commit.
		err = g.syncLeased(ctx, s, true, expectedTerm)
	} else {
		err = g.sync(ctx, s, true)
	}
	if err != nil {
		return recallRebuildResult{}, err
	}
	return g.lastReset, nil
}
