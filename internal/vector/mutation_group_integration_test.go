package vector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestTiKVMutationGroupsCompetingGateways(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires real TiKV")
	}
	r, err := NewRepository([]string{"127.0.0.1:12379"}, "group-test/"+uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := newMutationGroup(ctx, r, &pipelineMetrics{}, "data-0000")
	b := newMutationGroup(ctx, r, &pipelineMetrics{}, "data-0000")
	defer a.close()
	defer b.close()
	groups := []*mutationGroup{a, b}
	run := func(mutation func(int) (string, Mutation)) []mutationResult {
		start := make(chan struct{})
		results := make([]mutationResult, 32)
		var wg sync.WaitGroup
		for n := range results {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				<-start
				id, m := mutation(n)
				results[n].record, results[n].err = groups[n%2].mutate(ctx, id, m, false)
			}(n)
		}
		close(start)
		wg.Wait()
		return results
	}
	zero := uint64(0)
	results := run(func(n int) (string, Mutation) {
		return "cas", Mutation{RequestID: fmt.Sprint(n), Vector: []float64{1, 2}, ExpectedRevision: &zero}
	})
	success := 0
	for _, result := range results {
		if result.err == nil {
			success++
		} else if !errors.Is(result.err, ErrConflict) {
			t.Fatal(result.err)
		}
	}
	if success != 1 {
		t.Fatal("multiple gateways passed one CAS", success)
	}
	results = run(func(int) (string, Mutation) {
		return "duplicate", Mutation{RequestID: "same-receipt", Vector: []float64{2, 3}}
	})
	for _, result := range results {
		if result.err != nil || result.record.Revision != 1 {
			t.Fatal(result)
		}
	}
	results = run(func(n int) (string, Mutation) {
		return fmt.Sprint(n), Mutation{RequestID: fmt.Sprintf("distinct-%d", n), Vector: []float64{3, 4}}
	})
	for _, result := range results {
		if result.err != nil || result.record.Revision != 1 {
			t.Fatal(result)
		}
	}
	log, err := r.ReadEvents(ctx, 0, 0, 256)
	if err != nil || log.Head != 34 || len(log.Events) != 34 {
		t.Fatal(log, err)
	}
	for n, event := range log.Events {
		if event.Sequence != uint64(n+1) {
			t.Fatal("noncontiguous LSN", event)
		}
	}
}
