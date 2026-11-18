package vector

import "time"

func (q *followerReadQueue) metrics(m *metricText, labels string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var waiting, capturing int
	var oldest time.Duration
	var target uint64
	for r := range q.pending {
		oldest = max(oldest, time.Since(r.queued))
		if r.target != nil {
			waiting++
			target = max(target, r.target.Sequence)
		} else if r.batch != nil {
			capturing++
		}
	}
	m.add("vecdb_follower_read_pending", "gauge", "Bounded follower reads awaiting capture or apply.", labels, len(q.pending))
	m.add("vecdb_follower_read_capture_pending", "gauge", "Follower reads included in the in-flight leader capture.", labels, capturing)
	m.add("vecdb_follower_read_apply_pending", "gauge", "Follower reads waiting for captured graph positions.", labels, waiting)
	m.add("vecdb_follower_read_oldest_seconds", "gauge", "Oldest live follower read wait.", labels, oldest.Seconds())
	m.add("vecdb_follower_read_target_lag_entries", "gauge", "Maximum pending target beyond local applied position.", labels, lag(target, q.position.Sequence))
	for _, v := range []struct {
		name  string
		value uint64
	}{
		{"ready", q.completed}, {"timeout", q.timedOut}, {"canceled", q.canceled}, {"rejected", q.rejected}, {"failed", q.failed},
	} {
		m.add("vecdb_follower_read_"+v.name+"_total", "counter", "Follower read queue outcomes; ready means permission to query, not query success.", labels, v.value)
	}
}
