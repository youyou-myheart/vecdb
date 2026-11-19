package vector

import "errors"

// IndexPolicy configures the serving process. Replicas can use different query
// strategies over identical persisted graphs, checkpoints and journal deltas.
type IndexPolicy struct {
	ExactThreshold *int
	Recall         *RecallPolicy
}

func (p IndexPolicy) Validate() error {
	if p.ExactThreshold != nil && *p.ExactThreshold < 0 {
		return errors.New("HNSW_EXACT_THRESHOLD must be nonnegative")
	}
	if p.Recall != nil {
		return p.Recall.Validate()
	}
	return nil
}

func (p IndexPolicy) clone() IndexPolicy {
	if p.ExactThreshold != nil {
		threshold := *p.ExactThreshold
		p.ExactThreshold = &threshold
	}
	if p.Recall != nil {
		recall := *p.Recall
		p.Recall = &recall
	}
	return p
}

func (p IndexPolicy) exactThreshold(dimension int) int {
	if p.ExactThreshold != nil {
		return *p.ExactThreshold
	}
	if dimension == 64 {
		return 4096
	}
	return DefaultIndexConfig().ExactThreshold
}

// Install after any startup snapshot has been restored, before queries begin.
// Rebuilds and replayed resets replace only graph contents on this instance.
func (s *Service) setIndexPolicy(p IndexPolicy) {
	s.policy = p
	s.Index.exactThreshold = p.exactThreshold(s.Index.dimension)
}
