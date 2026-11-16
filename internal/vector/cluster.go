package vector

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterConfig struct {
	Routing                     *ClusterRouting
	ConsumerRetention           bool
	IndexReplication            string
	HostID, Zone, FailureDomain string
	MemoryBudgetBytes           uint64
	Scheduler                   string
	Capacity                    float64
	BalanceCooldown             time.Duration
	Replicas                    int
	NodeID, AdvertiseURL, Token string
	LeaseTTL                    time.Duration
	Roles                       string
}
type ClusterService struct {
	observations       clusterObservations
	dispatchMu         sync.Mutex
	dispatchers        map[DataPartitionID]*dataDispatcher
	dispatchDone       chan struct{}
	consumerRetention  bool
	graphReplication   bool
	routingMu          sync.Mutex
	unhealthy          map[string]time.Time
	retirementWG       sync.WaitGroup
	pendingRetirements map[string]Ownership
	lastSample         time.Time
	lastLoad           map[string]ShardLoad
	replicas           int
	draining           atomic.Bool
	master             *Master
	worker             *Worker
	gateway            *Gateway
	*PartitionedService
	coordinator *Coordinator
	node        ClusterNode
	objects     ObjectAPI
	token       string
	root        *Repository
	mu          sync.Mutex
	locals      map[IndexPartitionID]*localPartition
	cancel      context.CancelFunc
	done        chan struct{}
	client      *http.Client
}

func OpenClusterService(ctx context.Context, root *Repository, objects ObjectAPI, count int, cfg ClusterConfig) (*ClusterService, error) {
	if cfg.Scheduler == "" {
		cfg.Scheduler = "hash"
	}
	if cfg.Scheduler != "hash" && cfg.Scheduler != "load" {
		return nil, errors.New("SCHEDULER must be hash or load")
	}
	if cfg.MemoryBudgetBytes > 0 && cfg.Scheduler != "load" {
		return nil, errors.New("memory placement budget requires load scheduler")
	}
	if cfg.Scheduler == "load" && cfg.Replicas != 2 {
		return nil, errors.New("load scheduler requires REPLICAS=2")
	}
	if cfg.Capacity == 0 {
		cfg.Capacity = 1
	}
	if math.IsNaN(cfg.Capacity) || math.IsInf(cfg.Capacity, 0) || cfg.Capacity < .1 || cfg.Capacity > 100 {
		return nil, errors.New("WORKER_CAPACITY must be .1..100")
	}
	if cfg.BalanceCooldown == 0 {
		cfg.BalanceCooldown = 10 * time.Second
	}
	if cfg.BalanceCooldown < time.Second {
		return nil, errors.New("balance cooldown must be >=1s")
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	if cfg.Replicas < 1 || cfg.Replicas > 2 {
		return nil, errors.New("REPLICAS must be 1 or 2")
	}
	if len(cfg.Token) < 16 {
		return nil, errors.New("CLUSTER_TOKEN must contain at least 16 characters")
	}
	if cfg.LeaseTTL < 3*time.Second || cfg.LeaseTTL > time.Minute {
		return nil, errors.New("lease TTL must be 3s..60s")
	}
	if cfg.FailureDomain == "" {
		cfg.FailureDomain = "node"
	}
	if cfg.FailureDomain != "node" && cfg.FailureDomain != "host" && cfg.FailureDomain != "zone" {
		return nil, errors.New("REPLICA_FAILURE_DOMAIN must be node, host or zone")
	}
	if cfg.HostID == "" {
		var err error
		cfg.HostID, err = os.Hostname()
		if err != nil {
			return nil, err
		}
	}
	if cfg.FailureDomain != "node" && cfg.Replicas != 2 {
		return nil, errors.New("host/zone active-active mode requires two replicas")
	}
	roles, roleErr := parseRoles(cfg.Roles)
	if roleErr != nil {
		return nil, roleErr
	}
	if cfg.FailureDomain == "zone" {
		for _, role := range roles {
			if role == RoleWorker && cfg.Zone == "" {
				return nil, errors.New("ZONE required for zone-isolated Workers")
			}
		}
	}
	node := ClusterNode{HostID: cfg.HostID, Zone: cfg.Zone, MemoryBudgetBytes: cfg.MemoryBudgetBytes, Capacity: cfg.Capacity, Roles: roles, ID: cfg.NodeID, Session: uuid.NewString(), URL: cfg.AdvertiseURL}
	if e := validateNode(node); e != nil {
		return nil, e
	}
	var layout PartitionLayout
	var e error
	if cfg.Routing != nil {
		if cfg.IndexReplication != "graph" || cfg.Replicas != 2 {
			return nil, errors.New("cluster routing requires graph replication and two replicas")
		}
		layout, e = root.ensureRoutedLayout(ctx, count, cfg.Routing, cfg.Scheduler, cfg.FailureDomain, cfg.ConsumerRetention)
	} else {
		layout, e = root.ensureLayout(ctx, count)
	}
	if e != nil {
		return nil, e
	}
	if cfg.IndexReplication != "" && cfg.IndexReplication != "independent" && cfg.IndexReplication != "graph" {
		return nil, errors.New("INDEX_REPLICATION must be independent or graph")
	}
	if cfg.IndexReplication == "graph" && cfg.Replicas != 2 {
		return nil, errors.New("graph replication requires two configured replicas")
	}
	if cfg.ConsumerRetention && cfg.IndexReplication != "graph" {
		return nil, errors.New("consumer retention requires graph replication")
	}
	if cfg.Routing != nil {
		// ensureRoutedLayout already admitted the isolated protocol.
	} else if cfg.IndexReplication == "graph" {
		e = root.ensureGraphProtocol(ctx, cfg.Scheduler, cfg.FailureDomain, cfg.ConsumerRetention)
	} else if cfg.FailureDomain != "node" {
		e = root.ensureTopologyMode(ctx, cfg.Scheduler, cfg.FailureDomain)
	} else if cfg.Scheduler == "load" {
		e = root.ensureLoadMode(ctx)
	} else {
		e = root.ensureClusterMode(ctx, cfg.Replicas)
	}
	if e != nil {
		return nil, e
	}
	if e = root.ensureScheduler(ctx, cfg.Scheduler, cfg.BalanceCooldown); e != nil {
		return nil, e
	}
	runCtx, cancel := context.WithCancel(ctx)
	c := &ClusterService{consumerRetention: cfg.ConsumerRetention, graphReplication: cfg.IndexReplication == "graph", lastSample: time.Now(), replicas: cfg.Replicas, PartitionedService: &PartitionedService{layout: layout, router: IdentityPartitionRouter{layout}, data: map[DataPartitionID]*Repository{}, indexes: map[IndexPartitionID]PartitionIndex{}, dimension: root.Dimension}, coordinator: &Coordinator{failureDomain: cfg.FailureDomain, loadBalance: cfg.Scheduler == "load", balanceCooldown: cfg.BalanceCooldown, root: root, ttl: cfg.LeaseTTL.Milliseconds()}, node: node, objects: objects, token: cfg.Token, root: root, locals: map[IndexPartitionID]*localPartition{}, cancel: cancel, done: make(chan struct{}), client: &http.Client{Timeout: 30 * time.Second}}
	if cfg.Routing != nil {
		c.router = clusterRouter{IdentityPartitionRouter{layout}, layout.Routing}
		c.dispatchers = map[DataPartitionID]*dataDispatcher{}
		c.dispatchDone = make(chan struct{})
	}
	if hasRole(node, RoleMaster) {
		c.master = &Master{replicas: cfg.Replicas, coordinator: c.coordinator, node: node, layout: layout}
	}
	if hasRole(node, RoleWorker) {
		c.worker = &Worker{cluster: c}
	}
	if hasRole(node, RoleGateway) {
		c.gateway = &Gateway{c.PartitionedService}
	}
	dataBindings := layout.Bindings
	if cfg.Routing != nil {
		dataBindings = sourceLayout(count).Bindings
		for _, b := range layout.Bindings {
			c.indexes[b.Index] = &remotePartition{cluster: c, id: b.Index}
		}
	}
	for _, b := range dataBindings {
		data := *root
		data.ReceiptPrefix = root.Prefix
		data.DataSnapshots = cfg.Routing != nil
		if count > 1 || cfg.Routing != nil {
			data.Prefix = fmt.Sprintf("%sdata/%04d/", root.Prefix, b.Data)
			if e = data.ensureSchema(ctx); e != nil {
				cancel()
				return nil, e
			}
		}
		c.data[b.Data] = &data
		if cfg.Routing == nil {
			c.indexes[b.Index] = &remotePartition{cluster: c, id: b.Index}
		}
	}
	if e = c.coordinator.Heartbeat(ctx, node); e != nil {
		cancel()
		return nil, e
	}
	if cfg.Routing != nil {
		go c.dispatchLoop(runCtx)
	}
	go c.run(runCtx, cfg.LeaseTTL/3)
	return c, nil
}
func (c *ClusterService) run(ctx context.Context, interval time.Duration) {
	defer close(c.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		c.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (c *ClusterService) reconcile(ctx context.Context) {
	c.flushRetirements(ctx)
	node := c.node
	node.Draining = c.draining.Load()
	if c.worker != nil {
		node.Load = c.sampleLoad()
	}
	if e := c.coordinator.Heartbeat(ctx, node); e != nil {
		if ctx.Err() == nil {
			log.Printf("cluster heartbeat failed: %v", e)
		}
		return
	}
	if c.master != nil {
		var nodes []ClusterNode
		var e error
		if c.replicas < 2 {
			nodes, e = c.coordinator.Nodes(ctx)
		}
		if e == nil {
			e = c.master.Tick(ctx, nodes)
		}
		if e != nil && !errors.Is(e, ErrConflict) && ctx.Err() == nil {
			log.Printf("master reconciliation failed: %v", e)
		}
	}
	if c.worker != nil {
		c.worker.Tick(ctx)
	}
}
func (c *ClusterService) Close() {
	c.cancel()
	<-c.done
	if c.dispatchDone != nil {
		<-c.dispatchDone
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, e := range c.entries() {
		_, _ = c.coordinator.Update(ctx, e.owner, false, true)
		c.remove(e)
	}
	c.retirementWG.Wait()
	c.flushRetirements(ctx)
	if c.master != nil {
		if leader, e := c.coordinator.Resolve(ctx, masterLeaseID); e == nil && leader.Node == c.node.ID && leader.Session == c.node.Session {
			_, _ = c.coordinator.Update(ctx, leader, false, true)
		}
	}
}
