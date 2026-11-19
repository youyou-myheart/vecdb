package vector

import "time"

// Installed only after a confirmed owner renewal. The monotonic deadline starts
// before storage IO, so a delayed/unknown response cannot manufacture authority.
type replicaReadLease struct {
	owner    Ownership
	deadline time.Time
}

func (e *localPartition) grantReadLease(start time.Time, owner Ownership, ttl time.Duration) {
	if !owner.Ready || !sameOwner(e.owner, owner) || e.readRetired.Load() {
		return
	}
	e.readLease.Store(&replicaReadLease{owner: owner, deadline: leaseDeadline(start, ttl)})
}

func (e *localPartition) retireReads() {
	e.readRetired.Store(true)
	e.readLease.Store(nil)
}

func (e *localPartition) checkReadLease(req partitionRPC) error {
	l := e.readLease.Load()
	if e.readRetired.Load() || l == nil || !time.Now().Before(l.deadline) ||
		!sameOwner(l.owner, e.owner) || req.Partition != l.owner.Partition ||
		req.Epoch != l.owner.Epoch || req.Session != l.owner.Session {
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
