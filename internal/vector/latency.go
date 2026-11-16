package vector

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// A private registry per service avoids global registration and test collisions.
// Labels are code-defined stages and configured partitions, never IDs, epochs,
// request IDs or error text. All durations use the local monotonic clock.
type pipelineMetrics struct {
	once     sync.Once
	registry *prometheus.Registry
	duration *prometheus.HistogramVec
	batch    *prometheus.HistogramVec
	mu       sync.Mutex
	lastSync time.Time
}

func (p *pipelineMetrics) init() {
	p.once.Do(func() {
		p.registry = prometheus.NewRegistry()
		p.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vecdb_pipeline_stage_seconds",
			Help:    "Local stage duration, including failed attempts. Nested stages must not be added together.",
			Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2, 4, 8, 16, 32, 60},
		}, []string{"stage", "partition", "outcome"})
		p.batch = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vecdb_index_batch_records",
			Help:    "Input records per routed graph commit attempt, before revision coalescing.",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		}, []string{"partition", "outcome"})
		p.registry.MustRegister(p.duration, p.batch)
	})
}

func metricOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func (p *pipelineMetrics) observe(stage, partition string, duration time.Duration, err error) {
	p.init()
	p.duration.WithLabelValues(stage, partition, metricOutcome(err)).Observe(duration.Seconds())
}

// Call as defer p.track(... )(&err), with a named result. A nil pointer is
// appropriate only for measurements that cannot fail (e.g. mutex wait).
func (p *pipelineMetrics) track(stage, partition string) func(*error) {
	started := time.Now()
	return func(err *error) {
		var outcome error
		if err != nil {
			outcome = *err
		}
		p.observe(stage, partition, time.Since(started), outcome)
	}
}

func (p *pipelineMetrics) syncStarted(now time.Time) {
	p.mu.Lock()
	previous := p.lastSync
	p.lastSync = now
	p.mu.Unlock()
	if !previous.IsZero() {
		p.observe("worker_sync_interval", "", now.Sub(previous), nil)
	}
}

func (p *pipelineMetrics) appendTo(m *metricText) error {
	p.init()
	families, err := p.registry.Gather()
	if err != nil {
		return err
	}
	for _, family := range families {
		if _, err := expfmt.MetricFamilyToText(&m.Builder, family); err != nil {
			return fmt.Errorf("encode pipeline metrics: %w", err)
		}
	}
	return nil
}

func (g *graphReplica) track(stage string) func(*error) {
	if g.input == nil {
		return func(*error) {}
	}
	return g.input.cluster.observations.pipeline.track(stage, string(g.journal.id))
}
