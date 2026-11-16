package vector

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// Hold a real secondary 2PC prewrite whose primary was never written, as can
// happen when another region rejects a conflicting prewrite. This reproduces
// the route-plan wait in the native soak. Probes are used only in this test.
func TestRoutePlanPrewriteLockTiKV(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires TiKV")
	}
	ctx := context.Background()
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "route-plan-lock/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Client.Close()
	coord := &Coordinator{root: r}
	id := IndexPartitionID("index-0000")
	write := func(version uint64) {
		t.Helper()
		tx, e := r.begin()
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		if e = putJSON(tx, coord.planKey(id), testRoutePlan(version)); e == nil {
			e = tx.Commit(ctx)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	write(1)
	p := &remotePartition{id: id, cluster: &ClusterService{coordinator: coord, graphReplication: true}}
	if _, err = p.routingPlan(ctx, "search"); err != nil {
		t.Fatal(err)
	}
	tx, err := r.Client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	raw, _ := json.Marshal(testRoutePlan(2))
	if err = tx.Set(coord.planKey(id), raw); err != nil {
		t.Fatal(err)
	}
	committer, err := (transaction.TxnProbe{KVTxn: tx}).NewCommitter(0)
	if err != nil {
		t.Fatal(err)
	}
	cleaned := false
	defer func() {
		if !cleaned {
			committer.Cleanup(ctx)
		}
	}()
	committer.SetLockTTL(5000)
	committer.SetPrimaryKey([]byte(r.Prefix + "absent-primary"))
	if err = committer.PrewriteAllMutations(ctx); err != nil {
		t.Fatal(err)
	}
	plan, err := p.routingPlan(ctx, "search")
	if err != nil || plan.Version != 1 {
		t.Fatal("query did not fall back around the prewrite lock", plan, err)
	}
	families, err := p.cluster.observations.pipeline.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var fallbacks float64
	for _, family := range families {
		if family.GetName() == "vecdb_route_plan_fallback_total" {
			for _, metric := range family.GetMetric() {
				fallbacks += metric.GetCounter().GetValue()
			}
		}
	}
	if fallbacks != 1 {
		t.Fatal("real lock did not exercise the timeout fallback", fallbacks)
	}
	committer.Cleanup(ctx)
	cleaned = true
	write(3)
	plan, err = p.routingPlan(ctx, "search")
	if err != nil || plan.Version != 3 {
		t.Fatal("route did not refresh after lock cleanup", plan, err)
	}
}
