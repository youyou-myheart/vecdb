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
	m.add("vecdb_cluster_ready", "gauge", "At least one serving replica per index and every routed data writer ready.", "", flag(s.Ready))
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
	for id, plan := range s.ReplicaPlans {
		m.add("vecdb_index_publisher_lease_draining", "gauge", "Master is waiting for the old publisher lease before switching.", fmt.Sprintf("{index_partition=%q}", id), flag(plan.LeaseDraining))
	}
	for id, h := range s.GraphHeads {
		labels := fmt.Sprintf("{index_partition=%q}", id)
		for _, v := range []struct {
			name, help string
			value      uint64
		}{
			{"term", "Graph publisher fencing term.", h.Term},
			{"log_bytes", "Cumulative serialized graph payload bytes.", h.LogBytes},
			{"checkpoint_bytes", "Graph bytes covered by the published checkpoint.", h.CheckpointBytes},
			{"checkpoint_pending_bytes", "Durable graph bytes beyond the checkpoint.", lag(h.LogBytes, h.CheckpointBytes)},
			{"checkpoint_sequence", "Self-contained checkpoint graph position.", h.CheckpointPosition.Sequence},
			{"committed_sequence", "Durable graph journal head.", h.Committed.Sequence},
			{"visible_sequence", "Durable replayable graph position; not local apply progress.", h.Visible.Sequence},
			{"floor_sequence", "Graph log retention floor.", h.Floor.Sequence},
			{"unpublished_entries", "Compatibility gauge: committed minus replayable; zero in durable protocol.", lag(h.Committed.Sequence, h.Visible.Sequence)},
		} {
			m.add("vecdb_graph_"+v.name, "gauge", v.help, labels, v.value)
		}
		for source, commit := range h.Sources {
			m.add("vecdb_index_source_commit_lsn", "gauge", "Durable sparse source coverage; not replica applied progress.", fmt.Sprintf("{index_partition=%q,source=%q}", id, source), commit)
		}
		for source, epoch := range h.SourceEpochs {
			m.add("vecdb_index_source_epoch", "gauge", "Durable source Dispatcher epoch accepted by this Index.", fmt.Sprintf("{index_partition=%q,source=%q}", id, source), epoch)
		}
		for source, n := range h.CheckpointSources {
			m.add("vecdb_index_source_checkpoint_lsn", "gauge", "Source coverage captured in the checkpoint.", fmt.Sprintf("{index_partition=%q,source=%q}", id, source), n)
		}
		for source, n := range h.FloorSources {
			m.add("vecdb_index_source_retired_lsn", "gauge", "Source coverage whose graph entries have been retired.", fmt.Sprintf("{index_partition=%q,source=%q}", id, source), n)
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
			followerQueue := g.followerReads
			g.viewMu.RUnlock()
			if followerQueue != nil {
				followerQueue.metrics(m, fmt.Sprintf("{index_partition=%q,replica=%q}", g.journal.id, entry.owner.Partition))
			}
			m.add("vecdb_graph_local_applied_sequence", "gauge", "Actual local applied sequence without waiting for the heartbeat progress report.", fmt.Sprintf("{index_partition=%q,replica=%q}", g.journal.id, entry.owner.Partition), sequence)
			m.add("vecdb_graph_local_owner_epoch", "gauge", "Owner epoch of the local graph, paired with its applied sequence.", fmt.Sprintf("{index_partition=%q,replica=%q}", g.journal.id, entry.owner.Partition), entry.owner.Epoch)
			if w := g.writer; w != nil {
				w.mu.Lock()
				remaining := max(float64(0), time.Until(w.deadline).Seconds())
				valid, ready := 0, 0
				if !w.retired && remaining > 0 {
					valid = 1
					if w.known {
						ready = 1
					}
				}
				w.mu.Unlock()
				labels := fmt.Sprintf("{index_partition=%q,replica=%q}", g.journal.id, entry.owner.Partition)
				m.add("vecdb_index_local_lease_valid", "gauge", "Local publisher lease permits work according to the monotonic deadline.", labels, valid)
				m.add("vecdb_index_local_lease_remaining_seconds", "gauge", "Conservative local publisher lease remaining duration.", labels, remaining)
				m.add("vecdb_index_local_head_ready", "gauge", "Confirmed cached graph head ready for leased appends.", labels, ready)
			}
			in := g.input
			waiting := 0
			if in.waiting.Load() {
				waiting = 1
			}
			m.add("vecdb_index_consumer_waiting", "gauge", "Local consumer is blocked waiting for a notification or follower poll.", fmt.Sprintf("{index_partition=%q,replica=%q}", g.journal.id, entry.owner.Partition), waiting)
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
