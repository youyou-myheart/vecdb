package vector

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
)

func TestPipelineHistogramConcurrentOutcomes(t *testing.T) {
	var p pipelineMetrics
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				p.observe("test", "data-0000", 2*time.Second, nil)
				p.observe("test", "data-0000", 4*time.Second, injected)
			}
		}()
	}
	wg.Wait()
	m := &metricText{}
	if err := p.appendTo(m); err != nil {
		t.Fatal(err)
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(m.String()))
	if err != nil {
		t.Fatal(err)
	}
	metrics := families["vecdb_pipeline_stage_seconds"].GetMetric()
	if len(metrics) != 2 {
		t.Fatal(metrics)
	}
	for _, metric := range metrics {
		seconds := 2.0
		for _, label := range metric.Label {
			if label.GetName() == "outcome" && label.GetValue() == "error" {
				seconds = 4
			}
		}
		h := metric.GetHistogram()
		if h.GetSampleCount() != 800 || h.GetSampleSum() != seconds*800 {
			t.Fatal(h)
		}
		for _, b := range h.GetBucket() {
			want := uint64(0)
			if b.GetUpperBound() >= seconds {
				want = 800
			}
			if b.GetCumulativeCount() != want {
				t.Fatal(b)
			}
		}
	}
}

func TestQueueAgeRetryAndErrorStages(t *testing.T) {
	f, c, owner := routedFixture(t)
	f.write(t, "v", false)
	g := f.services[0].graphReplica
	q := deliveryFor(t, f, owner, 0)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	g.input.queue[q.Source].admitted = now.Add(-3 * time.Second)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if got := g.input.oldestAge(now); got != 3*time.Second {
		t.Fatal(got)
	}
	bad := q
	bad.RouteVersion++
	if err := g.enqueue(f.ctx, bad); err == nil {
		t.Fatal("invalid route admitted")
	}
	head, err := g.journal.Claim(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.db.commitErr = injected
	if err := g.commitRouted(f.ctx, f.services[0], head); err == nil {
		t.Fatal("failed graph commit succeeded")
	}
	f.db.commitErr = nil
	if got := g.input.oldestAge(now); got != 0 {
		t.Fatal(got)
	}
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	text, err := c.LocalMetrics()
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"index_admission", "graph_commit", "routed_commit"} {
		if !strings.Contains(text, `vecdb_pipeline_stage_seconds_count{outcome="error",partition="index-0000",stage="`+stage+`"} 1`) {
			t.Fatal(text)
		}
	}
	if !strings.Contains(text, `vecdb_pipeline_stage_seconds_count{outcome="success",partition="index-0000",stage="index_queue_wait"} 2`) {
		t.Fatal(text)
	}
	// A disconnected store must not prevent reading local timing diagnostics.
	f.db.beginErr = injected
	if _, err := c.Metrics(f.ctx); err == nil {
		t.Fatal("global store failure not exercised")
	}
	if _, err := c.LocalMetrics(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerSyncIntervalSkipsFirstSample(t *testing.T) {
	var p pipelineMetrics
	now := time.Now()
	p.syncStarted(now)
	p.syncStarted(now.Add(500 * time.Millisecond))
	m := &metricText{}
	if err := p.appendTo(m); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.String(), `vecdb_pipeline_stage_seconds_count{outcome="success",partition="",stage="worker_sync_interval"} 1`) ||
		!strings.Contains(m.String(), `vecdb_pipeline_stage_seconds_sum{outcome="success",partition="",stage="worker_sync_interval"} 0.5`) {
		t.Fatal(m.String())
	}
}

func TestLocalQueueAndAppliedGaugesDuringStorageFailure(t *testing.T) {
	f, c, owner := routedFixture(t)
	g := f.services[0].graphReplica
	c.locals = map[IndexPartitionID]*localPartition{
		g.journal.owner.Partition: {owner: g.journal.owner, service: f.services[0]},
	}
	f.write(t, "v", false)
	q := deliveryFor(t, f, owner, 0)
	if err := g.enqueue(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	g.input.queue[q.Source].admitted = time.Now().Add(-2 * time.Second)
	check := func(queued, applied float64) {
		t.Helper()
		text, err := c.LocalMetrics()
		if err != nil {
			t.Fatal(err)
		}
		var parser expfmt.TextParser
		families, err := parser.TextToMetricFamilies(strings.NewReader(text))
		if err != nil {
			t.Fatal(err)
		}
		value := func(name string) float64 {
			metrics := families[name].GetMetric()
			if len(metrics) != 1 {
				t.Fatalf("missing or duplicate %s", name)
			}
			return metrics[0].GetGauge().GetValue()
		}
		if value("vecdb_index_queue_batches") != queued || value("vecdb_graph_local_applied_sequence") != applied {
			t.Fatal(text)
		}
		age, bytes := value("vecdb_index_queue_oldest_seconds"), value("vecdb_index_queue_bytes")
		if (queued == 1 && (age < 2 || bytes <= 0)) || (queued == 0 && (age != 0 || bytes != 0)) {
			t.Fatal(age, bytes)
		}
	}
	f.db.beginErr = injected
	check(1, 0)
	f.db.beginErr = nil
	if err := f.services[0].Sync(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.db.beginErr = injected
	check(0, 1)
}
