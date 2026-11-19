package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"example.com/vector-storage/internal/vector"
)

func indexPolicyFromEnv() (vector.IndexPolicy, error) {
	var policy vector.IndexPolicy
	if raw := os.Getenv("HNSW_EXACT_THRESHOLD"); raw != "" {
		threshold, err := strconv.Atoi(raw)
		if err != nil || threshold < 0 {
			return policy, fmt.Errorf("HNSW_EXACT_THRESHOLD must be a non-negative integer")
		}
		policy.ExactThreshold = &threshold
	}

	recall := vector.DefaultRecallPolicy()
	if raw := os.Getenv("HNSW_RECALL_TARGET"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || !(value >= 0 && value <= 1) {
			return policy, fmt.Errorf("HNSW_RECALL_TARGET must be between 0 and 1")
		}
		recall.Target = value
	}
	if raw := os.Getenv("HNSW_RECALL_CONSECUTIVE"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 {
			return policy, fmt.Errorf("HNSW_RECALL_CONSECUTIVE must be a positive integer")
		}
		recall.Consecutive = value
	}
	if raw := os.Getenv("HNSW_RECALL_INTERVAL"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			return policy, fmt.Errorf("HNSW_RECALL_INTERVAL must be a positive duration")
		}
		recall.Interval = value
	}
	if raw := os.Getenv("HNSW_RECALL_COOLDOWN"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			return policy, fmt.Errorf("HNSW_RECALL_COOLDOWN must be a positive duration")
		}
		recall.Cooldown = value
	}
	policy.Recall = &recall
	return policy, nil
}
