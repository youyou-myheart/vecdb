package main

import (
	"strings"
	"testing"
	"time"

	"example.com/vector-storage/internal/vector"
)

func TestIndexPolicyEnvironment(t *testing.T) {
	for _, key := range []string{"HNSW_EXACT_THRESHOLD", "HNSW_RECALL_TARGET", "HNSW_RECALL_CONSECUTIVE", "HNSW_RECALL_INTERVAL", "HNSW_RECALL_COOLDOWN"} {
		t.Setenv(key, "")
	}
	policy, err := indexPolicyFromEnv()
	if err != nil || policy.ExactThreshold != nil || policy.Recall == nil || *policy.Recall != vector.DefaultRecallPolicy() {
		t.Fatalf("defaults: %+v, %v", policy, err)
	}
	t.Setenv("HNSW_EXACT_THRESHOLD", "0")
	t.Setenv("HNSW_RECALL_TARGET", "0.93")
	t.Setenv("HNSW_RECALL_CONSECUTIVE", "2")
	t.Setenv("HNSW_RECALL_INTERVAL", "2m")
	t.Setenv("HNSW_RECALL_COOLDOWN", "15m")
	policy, err = indexPolicyFromEnv()
	if err != nil || policy.ExactThreshold == nil || *policy.ExactThreshold != 0 || policy.Recall == nil || policy.Recall.Target != .93 || policy.Recall.Consecutive != 2 || policy.Recall.Interval != 2*time.Minute || policy.Recall.Cooldown != 15*time.Minute {
		t.Fatalf("overrides: %+v, %v", policy, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"HNSW_EXACT_THRESHOLD", "-1"}, {"HNSW_RECALL_TARGET", "NaN"}, {"HNSW_RECALL_CONSECUTIVE", "0"}, {"HNSW_RECALL_INTERVAL", "1"}, {"HNSW_RECALL_COOLDOWN", "-10s"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := indexPolicyFromEnv(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected invalid %s, got %v", tc.key, err)
			}
		})
	}
}
