package vector

import (
	"context"
	"testing"
)

func TestIndexPolicyPlansOnMatchingShardSize(t *testing.T) {
	ctx := context.Background()
	zero, boundary := 0, 2
	cases := []struct {
		name             string
		dimension, count int
		threshold        *int
		thresholdWant    int
		planWant         string
	}{
		{"64d default", 64, 129, nil, 4096, "exact"},
		{"other dimension default", 8, 129, nil, 128, "hnsw"},
		{"explicit zero", 64, 3, &zero, 0, "hnsw"},
		{"at configured threshold", 64, 2, &boundary, 2, "exact"},
		{"above configured threshold", 64, 3, &boundary, 2, "hnsw"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo, _ := newFakeRepo()
			repo.Dimension = tc.dimension
			service, err := OpenPartitionedServiceWithIndexPolicy(ctx, repo, partitionObjects(), 2, IndexPolicy{ExactThreshold: tc.threshold})
			if err != nil {
				t.Fatal(err)
			}
			rows := dataset(tc.count, tc.dimension)
			for _, partition := range service.indexes {
				index := partition.(*Service).Index
				t.Cleanup(index.graph.Close)
				if index.exactThreshold != tc.thresholdWant {
					t.Fatalf("runtime threshold %d, want %d", index.exactThreshold, tc.thresholdWant)
				}
				if err := index.Apply(rows); err != nil {
					t.Fatal(err)
				}
				q := Search{Vector: rows[0].Vector, K: 1}
				_, stats, err := index.SearchWithStats(q)
				if err != nil || stats.Plan != tc.planWant {
					t.Fatalf("plan %+v, want %q, err %v", stats, tc.planWant, err)
				}
				q.Filter = map[string]string{"rare": "0"}
				hits, stats, err := index.SearchWithStats(q)
				if err != nil || stats.Plan != "exact" || len(hits) != 1 || hits[0].ID != rows[0].ID {
					t.Fatal(hits, stats, err)
				}
			}
		})
	}
}

type dimensionedPolicyRepo struct {
	fakeRepo
	dimension int
}

func (r *dimensionedPolicyRepo) VectorDimension() int { return r.dimension }

func TestIndexPolicySurvivesSnapshotRebuildAndJournalReplay(t *testing.T) {
	ctx := context.Background()
	graphConfig := DefaultIndexConfig()
	graphConfig.ExactThreshold = 7
	old, err := NewHNSWIndex(64, graphConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(old.graph.Close)
	rows := dataset(11, 64)
	if err := old.Apply(rows[:10]); err != nil {
		t.Fatal(err)
	}
	state, err := old.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	objects := partitionObjects()
	repo := &dimensionedPolicyRepo{dimension: 64}
	m, err := objects.SaveState(ctx, repo.Namespace(), state)
	if err != nil {
		t.Fatal(err)
	}
	repo.manifest = &m
	delta, err := old.GenerateDelta(rows[10:])
	if err != nil {
		t.Fatal(err)
	}

	for _, overridden := range []bool{false, true} {
		policy := IndexPolicy{}
		want := 4096
		plan := "exact"
		if overridden {
			value := 2
			policy.ExactThreshold = &value
			want = value
			plan = "hnsw"
		}
		s, err := NewServiceWithIndexPolicy(ctx, repo, objects, policy)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Index.graph.Close() })
		if policy.ExactThreshold != nil {
			*policy.ExactThreshold = 400
		}
		if s.policy.ExactThreshold != nil && *s.policy.ExactThreshold != want {
			t.Fatal("serving policy aliases caller's option")
		}
		check := func() {
			t.Helper()
			if s.Index.config != graphConfig || s.Index.exactThreshold != want {
				t.Fatalf("persisted=%+v, runtime=%d (want %d)", s.Index.config, s.Index.exactThreshold, want)
			}
			_, stats, err := s.Index.SearchWithStats(Search{Vector: rows[0].Vector, K: 1})
			if err != nil || stats.Plan != plan {
				t.Fatal(stats, err)
			}
		}
		check()
		replica := &graphReplica{}
		if err := replica.apply(s, &delta, nil, GraphPosition{}); err != nil {
			t.Fatal(err)
		}
		check()
		if err := s.Index.Rebuild(ctx); err != nil {
			t.Fatal(err)
		}
		check()
		if err := replica.apply(s, nil, &state, GraphPosition{}); err != nil {
			t.Fatal(err)
		}
		check()
		roundTrip, err := s.Index.ExportState()
		if err != nil || roundTrip.Config != graphConfig {
			t.Fatal(roundTrip.Config, err)
		}
	}
}

func TestIndexPolicyGraphWorkerRestoresCheckpointWithLocalOverride(t *testing.T) {
	f := newGraphFixture(t)
	publisher := f.services[0]
	f.write(t, "a", false)
	f.write(t, "b", false)
	if _, err := publisher.Checkpoint(f.ctx); err != nil {
		t.Fatal(err)
	}

	follower := f.services[1].graphReplica
	head, err := follower.journal.Head(f.ctx)
	if err != nil || head.Checkpoint == nil {
		t.Fatal(head.Checkpoint, err)
	}
	threshold := 0
	s, err := newGraphServiceWithPolicy(f.ctx, follower.repo, f.objects, follower.journal, IndexPolicy{ExactThreshold: &threshold})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Index.graph.Close)
	_, stats, err := s.Index.SearchWithStats(Search{Vector: []float64{1, 2}, K: 1})
	if err != nil || stats.Plan != "hnsw" {
		t.Fatal(stats, err)
	}
	if s.Index.config.ExactThreshold != DefaultIndexConfig().ExactThreshold {
		t.Fatal("overrode the persisted graph config")
	}
}
