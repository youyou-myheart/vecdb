package vector

import (
	"context"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
)

func failureDomain(n ClusterNode, policy string) string {
	switch policy {
	case "host":
		return n.HostID
	case "zone":
		if n.HostID == "" {
			return ""
		}
		return n.Zone
	default:
		return n.ID
	}
}
func canPlaceReplica(nodes []ClusterNode, candidate ClusterNode, policy string) bool {
	return canReplaceReplica(nodes, candidate, policy, -1)
}
func canReplaceReplica(nodes []ClusterNode, candidate ClusterNode, policy string, skip int) bool {
	domain := failureDomain(candidate, policy)
	if domain == "" {
		return false
	}
	for i, n := range nodes {
		if i == skip {
			continue
		}
		if n.ID == candidate.ID || failureDomain(n, policy) == domain || (policy == "zone" && n.HostID == candidate.HostID) {
			return false
		}
	}
	return true
}
func replicaChoices(id IndexPartitionID, workers []ClusterNode, count int, policy string) []ClusterNode {
	choices := []ClusterNode{}
	for _, n := range rankWorkers(id, workers) {
		if canPlaceReplica(choices, n, policy) {
			choices = append(choices, n)
		}
		if len(choices) == count {
			break
		}
	}
	return choices
}

// Separate mode prevents older Masters from silently ignoring failure domains
// or serving queries without the read watermark contract.
func (r *Repository) ensureTopologyMode(ctx context.Context, scheduler, domain string) error {
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	mode := fmt.Sprintf("v7-active-active-%s-%s-2", scheduler, domain)
	key := []byte(r.Prefix + "cluster-mode")
	raw, e := tx.Get(ctx, key)
	if e == nil {
		if string(raw) != mode {
			return errors.New("active-active topology protocol mismatch; use a fresh namespace")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if e = tx.Set(key, []byte(mode)); e != nil {
		return e
	}
	schema := fmt.Sprintf("v8:dimension=%d:metric=l2:replicas=2:tasks=1", r.Dimension)
	if scheduler == "load" {
		schema = fmt.Sprintf("v9:dimension=%d:metric=l2:load=1:tasks=1", r.Dimension)
	}
	if e = tx.Set([]byte(r.Prefix+"schema"), []byte(schema+dataLogSchemaSuffix)); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
