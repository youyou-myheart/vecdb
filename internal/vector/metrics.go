package vector

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Counters are process-local and reset on restart; durable cursors/epochs below
// survive ownership changes. Never label metrics with record IDs or error text.
type clusterObservations struct {
	pipeline                                                    pipelineMetrics
	scrubScanned, scrubRemoved, staleCandidates, exactFallbacks atomic.Uint64
	acquisitions, dispatches, dispatchErrors, dispatchNanos     atomic.Uint64
	deliveries, deliveryErrors                                  atomic.Uint64
	admissions, duplicates, rejections                          atomic.Uint64
}

type metricText struct {
	strings.Builder
	seen map[string]bool
}

func (m *metricText) add(name, kind, help, labels string, value any) {
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if !m.seen[name] {
		fmt.Fprintf(&m.Builder, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
		m.seen[name] = true
	}
	fmt.Fprintf(&m.Builder, "%s%s %v\n", name, labels, value)
}

func lag(head, offset uint64) uint64 {
	if head > offset {
		return head - offset
	}
	return 0
}

func clusterMetrics(s ClusterStatus) *metricText {
	m := &metricText{}
	flag := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	m.add("vecdb_cluster_ready", "gauge", "At least one serving replica per index.", "", flag(s.Ready))
	m.add("vecdb_cluster_fully_replicated", "gauge", "Configured serving replica count and failure domains available.", "", flag(s.FullyReplicated))
	m.add("vecdb_master_available", "gauge", "Master lease resolves successfully.", "", flag(s.MasterAvailable))
	m.add("vecdb_master_epoch", "gauge", "Current resolved Master fencing epoch.", "", s.Leader.Epoch)
	if s.Routing != nil {
		m.add("vecdb_routing_version", "gauge", "Immutable cluster routing version.", "", s.Routing.Version)
	}
	for id, d := range s.DataProgress {
		labels := fmt.Sprintf("{data_partition=%q}", id)
		for _, v := range []struct {
			name, help string
			value      uint64
		}{
			{"head_lsn", "Durable data log head.", d.Head},
			{"completed_lsn", "Contiguous durable index completion prefix.", d.Completed},
			{"floor_lsn", "Data log retention floor.", d.Floor},
			{"pending_events", "Data events beyond contiguous completion.", lag(d.Head, d.Completed)},
			{"retained_events", "Retained data log span.", lag(d.Head, d.Floor)},
			{"owner_epoch", "Current resolved dispatcher fencing epoch.", d.Owner.Epoch},
		} {
			m.add("vecdb_data_"+v.name, "gauge", v.help, labels, v.value)
		}
		m.add("vecdb_data_owner_available", "gauge", "Dispatcher lease resolves successfully.", labels, flag(d.OwnerAvailable))
		m.add("vecdb_data_dispatch_error", "gauge", "Current dispatcher reports an error; details in cluster status.", labels, flag(d.Owner.LastError != ""))
	}
	for id, h := range s.GraphHeads {
		labels := fmt.Sprintf("{index_partition=%q}", id)
		for _, v := range []struct {
			name, help string
			value      uint64
		}{
			{"term", "Graph publisher fencing term.", h.Term},
			{"committed_sequence", "Durable graph journal head.", h.Committed.Sequence},
			{"visible_sequence", "Published query version.", h.Visible.Sequence},
			{"floor_sequence", "Graph log retention floor.", h.Floor.Sequence},
			{"unpublished_entries", "Committed entries not yet published.", lag(h.Committed.Sequence, h.Visible.Sequence)},
		} {
			m.add("vecdb_graph_"+v.name, "gauge", v.help, labels, v.value)
		}
		for source, commit := range h.Sources {
			m.add("vecdb_index_source_commit_lsn", "gauge", "Durable sparse source coverage; not replica applied progress.", fmt.Sprintf("{index_partition=%q,source=%q}", id, source), commit)
		}
		for _, o := range s.ReplicaOwners[id].Owners {
			labels := fmt.Sprintf("{index_partition=%q,replica=%q}", id, o.Partition)
			pos, known := s.GraphPositions[o.Partition]
			m.add("vecdb_graph_replica_progress_known", "gauge", "Replica graph progress was read successfully.", labels, flag(known))
			if known {
				m.add("vecdb_graph_replica_applied_sequence", "gauge", "Applied graph journal sequence.", labels, pos.Sequence)
				m.add("vecdb_graph_replica_lag_entries", "gauge", "Committed graph entries beyond replica applied progress.", labels, lag(h.Committed.Sequence, pos.Sequence))
			}
		}
	}
	return m
}

func (c *ClusterService) Metrics(ctx context.Context) (string, error) {
	s, err := c.Status(ctx)
	if err != nil {
		return "", err
	}
	m := clusterMetrics(s)
	if err := c.localMetrics(m); err != nil {
		return "", err
	}
	return m.String(), nil
}

// LocalMetrics remains available when TiKV is slow or unavailable. Scrape every
// Worker for local pipeline costs; a Gateway does not construct graphs.
func (c *ClusterService) LocalMetrics() (string, error) {
	m := &metricText{}
	if err := c.localMetrics(m); err != nil {
		return "", err
	}
	return m.String(), nil
}

func (c *ClusterService) localMetrics(m *metricText) error {
	o := &c.observations
	for _, v := range []struct {
		name, help string
		value      uint64
	}{
		{"dispatch_acquisitions_total", "Local dispatcher lease acquisitions, including initial ownership.", o.acquisitions.Load()},
		{"dispatch_ticks_total", "Local source dispatch attempts, including idle ticks.", o.dispatches.Load()},
		{"dispatch_errors_total", "Local failed dispatch attempts.", o.dispatchErrors.Load()},
		{"delivery_attempts_total", "Local outbound delivery attempts, including retries.", o.deliveries.Load()},
		{"delivery_errors_total", "Local failed outbound delivery attempts.", o.deliveryErrors.Load()},
		{"index_admissions_total", "Validated batches admitted locally.", o.admissions.Load()},
		{"index_duplicates_total", "Queued or durably committed delivery retries.", o.duplicates.Load()},
		{"index_rejections_total", "Rejected local delivery attempts, including fencing and backpressure.", o.rejections.Load()},
		{"scrub_scanned_records_total", "Authoritative records inspected by local misplaced-vector cleanup.", o.scrubScanned.Load()},
		{"scrub_removed_records_total", "Misplaced local records removed by committed cleanup deltas.", o.scrubRemoved.Load()},
		{"search_stale_candidates_total", "Candidates rejected by authoritative revision or destination checks.", o.staleCandidates.Load()},
		{"search_exact_fallbacks_total", "Searches exceeding the ANN candidate budget and using exact validation.", o.exactFallbacks.Load()},
	} {
		m.add("vecdb_"+v.name, "counter", v.help, "", v.value)
	}
	m.add("vecdb_dispatch_duration_seconds_total", "counter", "Cumulative local dispatch time including IO.", "", float64(o.dispatchNanos.Load())/1e9)
	for _, entry := range c.entries() {
		entry.mu.RLock()
		if service := entry.service; service != nil && service.graphReplica != nil && service.graphReplica.input != nil {
			g := service.graphReplica
			g.viewMu.RLock()
			sequence := g.position.Sequence
			g.viewMu.RUnlock()
			m.add("vecdb_graph_local_applied_sequence", "gauge", "Actual local applied sequence without waiting for the heartbeat progress report.", fmt.Sprintf("{index_partition=%q,replica=%q}", g.journal.id, entry.owner.Partition), sequence)
			in := g.input
			in.mu.Lock()
			batches, bytes := len(in.queue), in.bytes
			oldest := in.oldestAge(time.Now())
			in.mu.Unlock()
			labels := fmt.Sprintf("{replica=%q}", entry.owner.Partition)
			m.add("vecdb_index_queue_batches", "gauge", "Queued batches awaiting construction; excludes active batch.", labels, batches)
			m.add("vecdb_index_queue_bytes", "gauge", "Encoded bytes in local queued batches.", labels, bytes)
			m.add("vecdb_index_queue_oldest_seconds", "gauge", "Oldest locally admitted batch age; excludes active batch, resets on process restart.", labels, oldest.Seconds())
		}
		entry.mu.RUnlock()
	}
	if err := o.pipeline.appendTo(m); err != nil {
		return err
	}
	return nil
}
