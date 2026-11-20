package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"example.com/vector-storage/internal/vector"
	"github.com/google/uuid"
	"github.com/prometheus/common/expfmt"
)

// All seven service instances live in this test executable. TiKV/PD/S3 are
// external dependencies; no vector-server or supervisor subprocess is created.
func TestOneProcessPipeline(t *testing.T)          { oneProcessPipeline(t, false, false) }
func TestGraphOneProcessPipeline(t *testing.T)     { oneProcessPipeline(t, true, false) }
func TestRetentionOneProcessPipeline(t *testing.T) { oneProcessPipeline(t, true, true) }
func TestClusteredOneProcessPipeline(t *testing.T) { oneProcessPipeline(t, true, true, true) }
func oneProcessPipeline(t *testing.T, graph, retention bool, clustered ...bool) {
	var routing *vector.ClusterRouting
	dataCount, indexCount := 32, 32
	if len(clustered) > 0 && clustered[0] {
		routing = &vector.ClusterRouting{Version: 1, Centers: [][]float64{{0, 0, 0}, {48, 0, 0}, {96, 0, 0}}}
		dataCount, indexCount = 4, 3
	}
	if os.Getenv("ONEBOX_ACCEPTANCE") != "1" {
		t.Skip("set ONEBOX_ACCEPTANCE=1; requires TiKV/PD/SeaweedFS")
	}
	const token = "onebox-pipeline-public-token"
	namespace := "one-process/" + uuid.NewString()
	replication := "independent"
	reportName := "onebox-pipeline.json"
	if graph {
		replication = "graph"
		reportName = "graph-onebox-pipeline.json"
	}
	if retention {
		reportName = "retention-onebox-pipeline.json"
	}
	if routing != nil {
		reportName = "clustered-onebox-pipeline.json"
	}
	started := time.Now()
	lastStep := started
	type node struct {
		id, role, url string
		service       *vector.ClusterService
		server        *http.Server
		cancel        context.CancelFunc
		done          chan struct{}
		closeOnce     sync.Once
		repo          *vector.Repository
	}
	nodes := map[string]*node{}
	report := map[string]any{"index_replication": replication, "status": "running", "pid": os.Getpid(), "namespace": namespace, "topology": map[string]int{"gateway": 2, "master": 2, "worker": 3}, "checks": []string{}}
	check := func(name string) {
		report["checks"] = append(report["checks"].([]string), name)
		steps, _ := report["steps"].([]map[string]any)
		report["steps"] = append(steps, map[string]any{"name": name, "status": "passed", "seconds": time.Since(lastStep).Seconds()})
		lastStep = time.Now()
		t.Log(name)
	}
	stop := func(n *node) {
		n.closeOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = n.server.Shutdown(ctx)
			n.cancel()
			<-n.done
			n.service.Close()
			_ = n.repo.Client.Close()
		})
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			stop(n)
		}
		if t.Failed() {
			report["status"] = "failed"
		} else {
			report["status"] = "passed"
		}
		report["all_instances_stopped"] = true
		report["elapsed_seconds"] = time.Since(started).Seconds()
		path := filepath.Join("..", "..", "reports", reportName)
		_ = os.MkdirAll(filepath.Dir(path), 0755)
		raw, _ := json.MarshalIndent(report, "", "  ")
		if e := os.WriteFile(path, raw, 0644); e != nil {
			t.Error(e)
		}
	})
	start := func(id, role string) *node {
		t.Helper()
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		repo, e := vector.NewRepository([]string{env("PD_ENDPOINTS", "127.0.0.1:12379")}, namespace, 3)
		if e != nil {
			listener.Close()
			cancel()
			t.Fatal(e)
		}
		objects, e := vector.NewObjects(env("S3_ENDPOINT", "127.0.0.1:18333"), env("S3_ACCESS_KEY", "poc-access"), env("S3_SECRET_KEY", "poc-secret-local-only"), env("S3_BUCKET", "vector-index-poc"))
		if e != nil {
			listener.Close()
			cancel()
			repo.Client.Close()
			t.Fatal(e)
		}
		url := "http://" + listener.Addr().String()
		service, e := vector.OpenClusterService(ctx, repo, objects, dataCount, vector.ClusterConfig{Routing: routing, ConsumerRetention: retention, IndexReplication: replication, NodeID: id, Roles: role, AdvertiseURL: url, Token: "onebox-pipeline-peer-token", LeaseTTL: 6 * time.Second, Replicas: 2, Scheduler: "load", BalanceCooldown: time.Second})
		if e != nil {
			listener.Close()
			cancel()
			repo.Client.Close()
			t.Fatal(e)
		}
		server := &http.Server{Handler: service.WrapHandler(newHandler(service, service, 3, token, namespace)), ReadHeaderTimeout: 5 * time.Second}
		n := &node{id: id, role: role, url: url, service: service, server: server, cancel: cancel, done: make(chan struct{}), repo: repo}
		nodes[id] = n
		go func() { _ = server.Serve(listener) }()
		go func() {
			defer close(n.done)
			if service.EventDrivenIndexing() {
				<-ctx.Done()
				return
			}
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					syncCtx, done := context.WithTimeout(ctx, 5*time.Second)
					_ = service.SyncOwned(syncCtx)
					done()
				}
			}
		}()
		return n
	}
	client := &http.Client{Timeout: 35 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	call := func(n *node, method, path string, body any, want int) map[string]json.RawMessage {
		t.Helper()
		report["last_request"] = map[string]string{"node": n.id, "method": method, "path": path}
		raw, e := json.Marshal(body)
		if e != nil {
			t.Fatal(e)
		}
		request, e := http.NewRequest(method, n.url+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response, e := client.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		var out map[string]json.RawMessage
		if e = json.NewDecoder(response.Body).Decode(&out); e != nil {
			t.Fatal(e)
		}
		if response.StatusCode != want {
			t.Fatalf("%s %s: status %d want %d: %s", n.id, path, response.StatusCode, want, out)
		}
		return out
	}
	status := func(n *node) vector.ClusterStatus {
		t.Helper()
		raw := call(n, "GET", "/v1/cluster", nil, 200)
		b, _ := json.Marshal(raw)
		var s vector.ClusterStatus
		if e := json.Unmarshal(b, &s); e != nil {
			t.Fatal(e)
		}
		return s
	}
	wait := func(name string, fn func() bool) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if fn() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timeout: %s", name)
	}
	for _, role := range []string{"master", "gateway", "worker"} {
		count := 2
		if role == "worker" {
			count = 3
		}
		for i := 0; i < count; i++ {
			start(fmt.Sprintf("%s-%d", role, i), role)
		}
	}
	g0, g1 := nodes["gateway-0"], nodes["gateway-1"]
	ready := func() bool {
		s := status(g0)
		if !s.Ready || !s.FullyReplicated {
			return false
		}
		for _, p := range s.ReplicaPlans {
			if p.Publisher == "" || len(p.Desired) != len(p.Serving) {
				return false
			}
		}
		return len(s.ReplicaPlans) == indexCount
	}
	wait("seven instances ready", ready)
	tasksComplete := func() bool {
		state := status(g0)
		var page vector.TaskPage
		raw := call(g0, "GET", "/v1/cluster/tasks?limit=200", nil, 200)
		encoded, _ := json.Marshal(raw)
		if err := json.Unmarshal(encoded, &page); err != nil {
			t.Fatal(err)
		}
		byID := map[string]vector.MigrationTask{}
		for _, task := range page.Tasks {
			byID[task.ID] = task
		}
		for _, plan := range state.ReplicaPlans {
			if plan.TaskID == "" || byID[plan.TaskID].Stage != vector.MigrationCompleted {
				return false
			}
		}
		return len(state.ReplicaPlans) == indexCount
	}
	wait("initial migration tasks completed", tasksComplete)
	var preview vector.BalancePreview
	previewJSON, _ := json.Marshal(call(g0, "GET", "/v1/cluster/balance", nil, 200))
	if err := json.Unmarshal(previewJSON, &preview); err != nil {
		t.Fatal(err)
	}
	if len(preview.Partitions) != indexCount {
		t.Fatal(preview)
	}
	for _, ex := range preview.Partitions {
		if ex.Changed || ex.Task == nil || ex.Version == 0 {
			t.Fatal(ex)
		}
	}
	call(g1, "GET", "/v1/cluster/explain?partition=index-0000", nil, 200)
	call(g1, "GET", "/v1/cluster/task?id="+preview.Partitions[0].TaskID, nil, 200)
	check("durable_tasks_explain_and_read_only_balance_preview")

	s := status(g0)
	if len(s.Nodes) != 7 {
		t.Fatal(len(s.Nodes))
	}
	for _, n := range nodes {
		if n.role != "worker" && len(status(n).LocalPartitions) != 0 {
			t.Fatal("non-worker loaded an index")
		}
	}
	check(fmt.Sprintf("seven_instances_one_PID_%d_data_%d_index_shards_ready", dataCount, indexCount))
	expectedOffsets := map[vector.IndexPartitionID]uint64{}
	shard := func(id string) vector.IndexPartitionID {
		h := sha256.Sum256([]byte(id))
		return vector.IndexPartitionID(fmt.Sprintf("index-%04d", binary.BigEndian.Uint64(h[:8])%32))
	}
	var latest vector.Token
	for i := 0; i < 96; i++ {
		n := g0
		if i%2 != 0 {
			n = g1
		}
		id := fmt.Sprintf("item-%03d", i)
		expectedOffsets[shard(id)]++
		body := map[string]any{"request_id": "create-" + id, "vector": []float64{float64(i), 0, 0}, "payload": map[string]string{"group": fmt.Sprint(i % 3)}}
		out := call(n, "PUT", "/v1/records/"+id, body, 200)
		_ = json.Unmarshal(out["token"], &latest)
		if i == 0 {
			again := call(g1, "PUT", "/v1/records/"+id, body, 200)
			if string(again["token"]) != string(out["token"]) {
				t.Fatal("idempotency changed revision")
			}
			body["request_id"] = "duplicate"
			body["expected_revision"] = 0
			call(g1, "PUT", "/v1/records/"+id, body, 409)
		}
	}
	got := call(g1, "GET", "/v1/records/item-000", nil, 200)
	if string(got["revision"]) != "1" {
		t.Fatal(got)
	}
	call(g0, "GET", "/v1/records/absent", nil, 404)
	check("cross_gateway_insert_get_idempotency_and_conflicts")
	out := call(g1, "PUT", "/v1/records/item-000", map[string]any{"request_id": "update", "vector": []float64{0, 0, 0}, "payload": map[string]string{"group": "changed"}, "expected_revision": 1}, 200)
	_ = json.Unmarshal(out["token"], &latest)
	call(g0, "PUT", "/v1/records/item-000", map[string]any{"request_id": "stale", "vector": []float64{0, 0, 0}, "expected_revision": 1}, 409)
	expectedOffsets[shard("item-000")]++
	wait("all replicas consume fixture writes", func() bool {
		state := status(g0)
		if routing != nil {
			for _, d := range state.DataProgress {
				if d.Completed != d.Head {
					return false
				}
			}
			for id, h := range state.GraphHeads {
				owners := state.ReplicaOwners[id].Owners
				if len(owners) != 2 {
					return false
				}
				for _, o := range owners {
					if state.GraphPositions[o.Partition] != h.Visible || h.Visible != h.Committed {
						return false
					}
				}
			}
			return true
		}
		for id, want := range expectedOffsets {
			owners := state.ReplicaOwners[id].Owners
			if len(owners) != 2 {
				return false
			}
			for _, owner := range owners {
				if owner.Offset < want {
					return false
				}
			}
		}
		return true
	})
	deleted := false
	zeroRevision := uint64(2)
	verify := func(n *node) {
		t.Helper()
		out := call(n, "POST", "/v1/search", map[string]any{"vector": []float64{0, 0, 0}, "k": 1000, "exact": true, "min_visible": latest}, 200)
		var hits []vector.Hit
		_ = json.Unmarshal(out["hits"], &hits)
		count := 96
		if deleted {
			count--
		}
		if len(hits) != count {
			t.Fatal("incomplete results", len(hits), count)
		}
		for i, h := range hits {
			expected := i
			if deleted && i >= 1 {
				expected++
			}
			if h.ID != fmt.Sprintf("item-%03d", expected) {
				t.Fatal("duplicate/missing/order", i, h)
			}
			if h.Distance != float64(expected*expected) {
				t.Fatal("incorrect distance", h)
			}
			if i > 0 && (h.Revision != 1 || h.Payload["group"] != fmt.Sprint(expected%3)) {
				t.Fatal("incorrect record state", h)
			}
			if i == 0 && (h.Revision != zeroRevision || h.Payload["group"] != "changed") {
				t.Fatal("stale update", h)
			}
		}
	}
	verify(g0)
	verify(g1)
	if routing != nil {
		// Business read through a follower, with no MinVisible/Sync side effects.
		// Distinguish the apply barrier from the complete query wall time.
		for _, gateway := range []*node{g0, g1} {
			out := call(gateway, "POST", "/v1/follower-search", map[string]any{"vector": []float64{0, 0, 0}, "k": 1000, "exact": true}, 200)
			var hits []vector.Hit
			var reads []vector.FollowerReadInfo
			if err := json.Unmarshal(out["hits"], &hits); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(out["follower_reads"], &reads); err != nil {
				t.Fatal(err)
			}
			if len(hits) != 96 || len(reads) != indexCount {
				t.Fatal("incomplete follower read", len(hits), len(reads))
			}
			for _, r := range reads {
				if r.Target.Sequence == 0 || r.Applied.Sequence < r.Target.Sequence || r.Node == "" || r.Session == "" || r.OwnerEpoch == 0 {
					t.Fatal("invalid follower barrier", r)
				}
			}
			report["follower_read_"+gateway.id] = reads
		}
		check("follower_read_on_both_gateways_with_applied_positions_and_stage_timings")
	}
	filtered := call(g1, "POST", "/v1/search", map[string]any{"vector": []float64{0, 0, 0}, "k": 10, "filter": map[string]string{"group": "changed"}}, 200)
	var fh []vector.Hit
	_ = json.Unmarshal(filtered["hits"], &fh)
	if len(fh) != 1 || fh[0].ID != "item-000" {
		t.Fatal(fh)
	}
	check("versioned_update_visibility_filter_and_global_topk")
	call(g0, "POST", "/v1/checkpoint", map[string]any{}, 200)
	call(g1, "POST", "/v1/log/prune", map[string]any{}, 200)
	out = call(g0, "DELETE", "/v1/records/item-001", map[string]any{"request_id": "delete", "expected_revision": 1}, 200)
	_ = json.Unmarshal(out["token"], &latest)
	deleted = true
	verify(g1)
	seen := map[string]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatal("cursor loop")
		}
		path := "/v1/records?limit=7"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		page := call(g1, "GET", path, nil, 200)
		var records []vector.Record
		_ = json.Unmarshal(page["records"], &records)
		for _, r := range records {
			if seen[r.ID] || r.Deleted {
				t.Fatal("duplicate/tombstone", r.ID)
			}
			seen[r.ID] = true
		}
		cursor = ""
		if b := page["next_cursor"]; b != nil {
			_ = json.Unmarshal(b, &cursor)
		}
		if cursor == "" {
			break
		}
	}
	if len(seen) != 95 || seen["item-001"] {
		t.Fatal(len(seen))
	}
	check("checkpoint_prune_delete_tail_and_cross_shard_pagination")
	// Continue querying the other Gateway while the elected Master shuts down.
	old := status(g1).Leader
	stop(nodes[old.Node])
	wait("standby Master elected", func() bool {
		verify(g1)
		s := status(g1)
		return s.MasterAvailable && s.Leader.Node != old.Node && s.Leader.Epoch > old.Epoch
	})
	check("master_failover_with_queries")
	// Drain one Worker through its authenticated public API, querying throughout.
	worker := nodes["worker-2"]
	call(worker, "POST", "/v1/drain", map[string]any{}, 200)
	wait("worker drained", func() bool {
		verify(g1)
		s := status(g1)
		for _, p := range s.ReplicaPlans {
			for _, a := range p.Serving {
				if a.Node == worker.id {
					return false
				}
			}
		}
		return s.Ready && s.FullyReplicated
	})
	stop(worker)
	verify(g0)
	wait("drain task retirement completed", tasksComplete)
	check("drain_worker_without_partial_results")
	call(g1, "POST", "/v1/rebuild", map[string]any{}, 200)
	verify(g0)
	check("rebuild_and_checkpoint_preserve_updated_deleted_state")
	// A durable update after the rebuild snapshot must survive subsequent operations.
	out = call(g1, "PUT", "/v1/records/item-000", map[string]any{"request_id": "post-snapshot", "expected_revision": 2, "vector": []float64{0, 0, 0}, "payload": map[string]string{"group": "changed"}}, 200)
	_ = json.Unmarshal(out["token"], &latest)
	zeroRevision = 3
	stop(g0)
	verify(g1)
	check("gateway_failover_shared_durable_state")
	// Restore the third Worker in the same process with a new session.
	wait("old registration expires", func() bool {
		for _, n := range status(g1).Nodes {
			if n.ID == worker.id {
				return false
			}
		}
		return true
	})
	start("worker-2", "worker")
	wait("replacement replica and data owners ready", func() bool {
		verify(g1)
		s := status(g1)
		// The next checkpoint/prune needs every data owner. A replacement
		// index replica can already serve searches during a data handoff.
		if !s.Ready || !s.FullyReplicated {
			return false
		}
		for _, owners := range s.ReplicaOwners {
			for _, o := range owners.Owners {
				if o.Node == "worker-2" {
					return true
				}
			}
		}
		return false
	})
	check("worker_restart_and_post_snapshot_update_visible")
	verify(g1)
	report["live_records"] = 95
	if routing != nil {
		// Lose all data owners after GC, verify the write cannot be acknowledged,
		// then drive more than one dispatcher window through recovered owners.
		expected := map[string]vector.Hit{}
		baseline := call(g1, "POST", "/v1/search", map[string]any{"vector": []float64{0, 0, 0}, "k": 1000, "exact": true, "min_visible": latest}, 200)
		var initial []vector.Hit
		if err := json.Unmarshal(baseline["hits"], &initial); err != nil {
			t.Fatal(err)
		}
		for _, hit := range initial {
			expected[hit.ID] = hit
		}
		metricSum := func(name string) float64 {
			t.Helper()
			req, _ := http.NewRequest("GET", g1.url+"/metrics", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatal("metrics status", resp.StatusCode)
			}
			var parser expfmt.TextParser
			families, err := parser.TextToMetricFamilies(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			family := families[name]
			if family == nil {
				t.Fatal("missing metric", name)
			}
			var total float64
			for _, value := range family.Metric {
				total += value.GetGauge().GetValue()
			}
			return total
		}
		for round := 0; round < 2; round++ {
			call(g1, "POST", "/v1/checkpoint", map[string]any{}, 200)
			call(g1, "POST", "/v1/log/prune", map[string]any{}, 200)
			for n := 0; n < 3; n++ {
				stop(nodes[fmt.Sprintf("worker-%d", n)])
			}
			probeID := fmt.Sprintf("no-owner-%d", round)
			call(g1, "PUT", "/v1/records/"+probeID, map[string]any{"request_id": probeID, "vector": []float64{1, 0, 0}}, 503)
			call(g1, "GET", "/v1/records/"+probeID, nil, 404)
			wait("old Worker registrations disappear", func() bool {
				for _, n := range status(g1).Nodes {
					for _, role := range n.Roles {
						if role == "worker" {
							return false
						}
					}
				}
				return true
			})
			for n := 0; n < 3; n++ {
				start(fmt.Sprintf("worker-%d", n), "worker")
			}
			wait("new data owners ready", func() bool {
				s := status(g1)
				if len(s.DataProgress) != dataCount {
					return false
				}
				for _, d := range s.DataProgress {
					if !d.OwnerAvailable {
						return false
					}
				}
				return true
			})
			for n := 0; n < 144; n++ {
				id := fmt.Sprintf("outage-%d-%03d", round, n)
				x := []float64{0, 48, 96}[n%3] + float64(n)/1000
				payload := map[string]string{"group": "outage"}
				out := call(g1, "PUT", "/v1/records/"+id, map[string]any{"request_id": "new-" + id, "vector": []float64{x, 1, 0}, "payload": payload}, 200)
				if err := json.Unmarshal(out["token"], &latest); err != nil {
					t.Fatal(err)
				}
				expected[id] = vector.Hit{ID: id, Revision: 1, Distance: x*x + 1, Payload: payload}
			}
			moving := fmt.Sprintf("outage-%d-000", round)
			for n, x := range []float64{48, 96} {
				call(g1, "PUT", "/v1/records/"+moving, map[string]any{"request_id": fmt.Sprintf("move-%s-%d", moving, n), "vector": []float64{x, 1, 0}}, 200)
			}
			call(g1, "DELETE", "/v1/records/"+moving, map[string]any{"request_id": "remove-" + moving}, 200)
			out := call(g1, "PUT", "/v1/records/"+moving, map[string]any{"request_id": "restore-" + moving, "vector": []float64{0, 1, 0}, "payload": map[string]string{"group": "outage"}}, 200)
			if err := json.Unmarshal(out["token"], &latest); err != nil {
				t.Fatal(err)
			}
			if latest.Revision != 5 {
				t.Fatal(latest)
			}
			hit := expected[moving]
			hit.Revision = 5
			expected[moving] = hit
			if raw := call(g1, "GET", "/v1/records/"+moving, nil, 200); string(raw["revision"]) != "5" {
				t.Fatal(raw)
			}
			wait("all sources and graph replicas recover after repeated outage", func() bool {
				s := status(g1)
				if !s.Ready || !s.FullyReplicated || len(s.DataProgress) != dataCount || len(s.GraphHeads) != indexCount {
					return false
				}
				for _, d := range s.DataProgress {
					if d.Floor > d.Completed || d.Completed != d.Head || d.Owner.Node == "" {
						return false
					}
				}
				for id, h := range s.GraphHeads {
					owners := s.ReplicaOwners[id].Owners
					if len(owners) != 2 || h.Committed != h.Visible {
						return false
					}
					for _, owner := range owners {
						if s.GraphPositions[owner.Partition] != h.Visible {
							return false
						}
					}
				}
				return true
			})
			// Receipt survives checkpoints, GC and changes to this record's revision.
			retry := call(g1, "PUT", "/v1/records/"+moving, map[string]any{"request_id": "new-" + moving, "vector": []float64{0, 1, 0}, "payload": map[string]string{"group": "outage"}}, 200)
			var original vector.Token
			if err := json.Unmarshal(retry["token"], &original); err != nil || original.Revision != 1 {
				t.Fatal(original, err)
			}
			result := call(g1, "POST", "/v1/search", map[string]any{"vector": []float64{0, 0, 0}, "k": 1000, "exact": true, "min_visible": latest}, 200)
			var hits []vector.Hit
			if err := json.Unmarshal(result["hits"], &hits); err != nil {
				t.Fatal(err)
			}
			if len(hits) != len(expected) {
				t.Fatal("lost recovered records", len(hits), len(expected))
			}
			seen := map[string]bool{}
			for _, hit := range hits {
				want, ok := expected[hit.ID]
				if !ok || seen[hit.ID] || hit.Revision != want.Revision || math.Abs(hit.Distance-want.Distance) > 1e-6 || hit.Payload["group"] != want.Payload["group"] {
					t.Fatal("recovery oracle mismatch", hit, want)
				}
				seen[hit.ID] = true
			}
			follower := call(g1, "POST", "/v1/follower-search", map[string]any{"vector": []float64{0, 0, 0}, "k": 1000, "exact": true}, 200)
			var followerHits []vector.Hit
			var reads []vector.FollowerReadInfo
			if err := json.Unmarshal(follower["hits"], &followerHits); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(follower["follower_reads"], &reads); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(followerHits, hits) || len(reads) != indexCount {
				t.Fatal("follower recovery oracle mismatch")
			}
			for _, read := range reads {
				if read.Applied.Sequence < read.Target.Sequence {
					t.Fatal("follower returned before apply", read)
				}
			}
			report[fmt.Sprintf("follower_after_outage_%d", round+1)] = reads
			if metricSum("vecdb_data_pending_events") != 0 || metricSum("vecdb_graph_replica_lag_entries") != 0 {
				t.Fatal("recovery lag failed to converge")
			}
			check(fmt.Sprintf("repeated_outage_%d_snapshot_GC_multibatch_move_delete_restore_receipt_metrics", round+1))
		}
		report["live_records"] = len(expected)
	}
}
