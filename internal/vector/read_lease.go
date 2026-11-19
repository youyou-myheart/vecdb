package vector

import (
	"time"

	"example.com/vector-storage/internal/lease"
)

// Only a confirmed renewal for this ready replica can establish read authority.
func (e *localPartition) grantReadLease(start time.Time, owner Ownership, ttl time.Duration) {
	if !owner.Ready || owner.Released || !sameOwner(e.owner, owner) {
		return
	}
	e.readLease.Confirm(start, lease.Grant{Epoch: owner.Epoch, Expires: owner.Expires, Remaining: ttl})
}

func (e *localPartition) retireReads() {
	e.readLease.Retire()
}

func (e *localPartition) checkReadLease(req partitionRPC) error {
	var epoch uint64
	if req.Partition != e.owner.Partition || req.Session != e.owner.Session ||
		!e.readLease.InLeaderLease(&epoch) || req.Epoch != epoch {
		return ErrFenced
	}
	return nil
}

func (c *ClusterService) localReadLeases() bool {
	return c.localOwnerLeases || c.graphReplication && c.coordinator.leaseReads
}

func (c *ClusterService) routeHintsEnabled() bool { return c.cachedRouting || c.localReadLeases() }

func cachedRouteOperation(op string) bool {
	switch op {
	case "search", "sync", "checkpoint", "prune", "rebuild":
		return true
	}
	return false
}
