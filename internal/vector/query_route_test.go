package vector

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestRoutedSearchOnlyUsesAppliedIndex(t *testing.T) {
	index := mustGraph(t, 2)
	if err := index.Apply([]Record{{ID: "old", Revision: 1, Vector: []float64{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	// No cluster/repository is installed. Any candidate storage access would panic.
	s := &Service{Index: index, graphReplica: &graphReplica{}}
	hits, _, err := s.searchRouted(context.Background(), Search{Vector: []float64{1, 0}, K: 1})
	if err != nil || len(hits) != 1 || hits[0].ID != "old" {
		t.Fatal(hits, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = s.searchRouted(ctx, Search{Vector: []float64{1, 0}, K: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRoutedSearchAllowsUnappliedUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	repo, _ := newFakeRepo()
	old, err := repo.Mutate(ctx, "a", Mutation{RequestID: "create", Vector: []float64{1, 0}}, false)
	if err != nil {
		t.Fatal(err)
	}
	index := mustGraph(t, 2)
	if err = index.Apply([]Record{old}); err != nil {
		t.Fatal(err)
	}
	s := &Service{Index: index, graphReplica: &graphReplica{}}
	for _, deleted := range []bool{false, true} {
		if _, err = repo.Mutate(ctx, "a", Mutation{RequestID: fmt.Sprint("change-", deleted), Vector: []float64{9, 0}}, deleted); err != nil {
			t.Fatal(err)
		}
		hits, _, err := s.searchRouted(ctx, Search{Vector: []float64{1, 0}, K: 1})
		if err != nil || len(hits) != 1 || hits[0].Revision != old.Revision || hits[0].Distance != 0 {
			t.Fatal(hits, err)
		}
	}
}

func TestSearchPartitionSubsetAndValidation(t *testing.T) {
	l, _ := newLayout(3)
	p := &PartitionedService{router: IdentityPartitionRouter{l}}
	ids, err := p.queryIndexes(Search{IndexPartitions: []IndexPartitionID{"index-0002"}})
	if err != nil || len(ids) != 1 || ids[0] != "index-0002" {
		t.Fatal(ids, err)
	}
	for _, ids := range [][]IndexPartitionID{{"missing"}, {"index-0000", "index-0000"}} {
		if _, err = p.queryIndexes(Search{IndexPartitions: ids}); !errors.Is(err, ErrInvalidSearchPartitions) {
			t.Fatal(err)
		}
	}
}

func TestMergeMovedIDsUsesNewestReturnedRevision(t *testing.T) {
	hits := deduplicateHits([]Hit{{ID: "a", Revision: 1, Distance: 0}, {ID: "a", Revision: 2, Distance: 5}, {ID: "b", Revision: 1, Distance: 1}})
	if len(hits) != 2 {
		t.Fatal(hits)
	}
	for _, h := range hits {
		if h.ID == "a" && h.Revision != 2 {
			t.Fatal(h)
		}
	}
}
