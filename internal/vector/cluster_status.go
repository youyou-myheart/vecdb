package vector

import (
	"context"
	"errors"
)

type ReplicaOwners struct {
	Owners []Ownership `json:"owners"`
}
type ClusterStatus struct {
	Routing           *ClusterRouting                    `json:"routing,omitempty"`
	DataProgress      map[string]DataProgress            `json:"data_progress,omitempty"`
	ConsumerRetention bool                               `json:"consumer_retention"`
	IndexReplication  string                             `json:"index_replication"`
	GraphHeads        map[IndexPartitionID]GraphHead     `json:"graph_heads,omitempty"`
	GraphPositions    map[IndexPartitionID]GraphPosition `json:"graph_positions,omitempty"`
	FailureDomain     string                             `json:"failure_domain"`
	RoutingCooldown   map[string]int64                   `json:"routing_cooldown_until_ms,omitempty"`
	LogHeads          map[IndexPartitionID]uint64        `json:"log_heads,omitempty"`
	ReplicaLag        map[IndexPartitionID]uint64        `json:"replica_lag,omitempty"`
	FullyReplicated   bool                               `json:"fully_replicated,omitempty"`
	ReplicaPlans      map[IndexPartitionID]ReplicaPlan   `json:"replica_plans,omitempty"`
	ReplicaOwners     map[IndexPartitionID]ReplicaOwners `json:"replica_owners,omitempty"`
	Replicas          int                                `json:"replicas,omitempty"`
	Leader            Ownership                          `json:"leader"`
	MasterAvailable   bool                               `json:"master_available"`
	Assignments       map[IndexPartitionID]Assignment    `json:"assignments"`
	LocalPartitions   []IndexPartitionID                 `json:"local_partitions"`
	Node              ClusterNode                        `json:"node"`
	Nodes             []ClusterNode                      `json:"nodes"`
	Owners            map[IndexPartitionID]Ownership     `json:"owners"`
	Ready             bool                               `json:"ready"`
}

func (c *ClusterService) Status(ctx context.Context) (ClusterStatus, error) {
	s := ClusterStatus{Assignments: map[IndexPartitionID]Assignment{}, LocalPartitions: []IndexPartitionID{}, Node: c.node, Owners: map[IndexPartitionID]Ownership{}, Ready: true}
	if c.layout.Routing != nil {
		s.Routing = c.layout.Routing
		var err error
		s.DataProgress, err = c.dataProgress(ctx)
		if err != nil {
			return s, err
		}
		for _, progress := range s.DataProgress {
			if !progress.Fresh {
				s.Ready = false
			}
		}
	}
	s.ConsumerRetention = c.consumerRetention
	s.IndexReplication = "independent"
	if c.graphReplication {
		s.IndexReplication = "graph"
		s.GraphHeads = map[IndexPartitionID]GraphHead{}
		s.GraphPositions = map[IndexPartitionID]GraphPosition{}
	}
	s.FailureDomain = c.coordinator.failureDomain
	s.RoutingCooldown = c.routingHealth()
	var e error
	s.Node.Draining = c.draining.Load()
	s.Leader, e = c.coordinator.Resolve(ctx, masterLeaseID)
	s.MasterAvailable = e == nil
	for _, entry := range c.entries() {
		entry.mu.RLock()
		if entry.service != nil {
			s.LocalPartitions = append(s.LocalPartitions, entry.owner.Partition)
		}
		entry.mu.RUnlock()
	}
	s.Nodes, e = c.coordinator.Nodes(ctx)
	if e != nil {
		return s, e
	}
	if c.replicas > 1 {
		s.FullyReplicated = true
		s.Replicas = c.replicas
		s.ReplicaPlans = map[IndexPartitionID]ReplicaPlan{}
		s.ReplicaOwners = map[IndexPartitionID]ReplicaOwners{}
		nodeByID := map[string]ClusterNode{}
		for _, n := range s.Nodes {
			nodeByID[n.ID+"/"+n.Session] = n
		}
		if c.hotReplicas() {
			s.LogHeads = map[IndexPartitionID]uint64{}
			s.ReplicaLag = map[IndexPartitionID]uint64{}
		}
		for _, b := range c.layout.Bindings {
			p, e := c.coordinator.Plan(ctx, b.Index)
			if e != nil {
				s.FullyReplicated = false
				s.Ready = false
				continue
			}
			s.ReplicaPlans[b.Index] = p
			j := graphJournal{c: c.coordinator, id: b.Index}
			if c.graphReplication {
				h, err := j.Head(ctx)
				if errors.Is(err, ErrConflict) {
					// During publisher takeover, the old run may be closing or
					// change while its tail is sampled. Keep status available,
					// but never report that unverified tail as current progress.
					s.Ready, s.FullyReplicated = false, false
					continue
				}
				if err != nil {
					return s, err
				}
				s.GraphHeads[b.Index] = h
			}
			var head uint64
			if c.hotReplicas() {
				if c.layout.Routing != nil && c.graphReplication {
					head = s.GraphHeads[b.Index].Committed.DataOffset
				} else {
					head, e = c.partitionHead(ctx, b.Index)
					if e != nil {
						return s, e
					}
				}
				s.LogHeads[b.Index] = head
			}
			domains := []ClusterNode{}
			found := false
			for _, a := range p.Serving {
				o, e := c.coordinator.Resolve(ctx, a.Partition)
				if e == nil {
					if c.graphReplication {
						if o.GraphPosition != nil {
							s.GraphPositions[o.Partition] = *o.GraphPosition
						} else if pos, err := j.Progress(ctx, o); err == nil {
							s.GraphPositions[o.Partition] = pos
						}
					}
					if c.hotReplicas() && head > o.Offset {
						s.ReplicaLag[o.Partition] = head - o.Offset
					}
					n := nodeByID[o.Node+"/"+o.Session]
					if canPlaceReplica(domains, n, c.coordinator.failureDomain) {
						domains = append(domains, n)
					}
					state := s.ReplicaOwners[b.Index]
					state.Owners = append(state.Owners, o)
					s.ReplicaOwners[b.Index] = state
					if !found {
						s.Owners[b.Index] = o
						found = true
					}
				}
			}
			if len(s.ReplicaOwners[b.Index].Owners) < c.replicas || (c.hotReplicas() && len(domains) < c.replicas) {
				s.FullyReplicated = false
			}
			if !found {
				s.Ready = false
			}
		}
		return s, nil
	}
	for _, b := range c.layout.Bindings {
		a, _ := c.coordinator.Assignment(ctx, b.Index)
		s.Assignments[b.Index] = a
		o, e := c.coordinator.Resolve(ctx, b.Index)
		if e != nil {
			s.Ready = false
		}
		s.Owners[b.Index] = o
	}
	return s, nil
}

type DataProgress struct {
	Known          bool      `json:"progress_known"`
	Fresh          bool      `json:"progress_fresh"`
	ObservedAtMS   int64     `json:"observed_at_ms"`
	ObservedEpoch  uint64    `json:"observed_epoch"`
	Head           uint64    `json:"head"`
	Completed      uint64    `json:"completed_lsn"`
	Floor          uint64    `json:"floor"`
	Owner          Ownership `json:"owner"`
	OwnerAvailable bool      `json:"owner_available"`
}
