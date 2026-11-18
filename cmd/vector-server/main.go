package main

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/vector-storage/internal/rpcserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/vector-storage/internal/vector"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type runningService interface {
	recordAPI
	searchAPI
	MaybeRebuild(context.Context) error
}

func main() {
	token := os.Getenv("API_TOKEN")
	if len(token) < 16 {
		log.Fatal("API_TOKEN must be set and contain at least 16 characters")
	}
	dim, e := strconv.Atoi(env("DIMENSION", "3"))
	if e != nil || dim < 1 || dim > 4096 {
		log.Fatal("invalid DIMENSION")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	repo, e := vector.NewRepository(strings.Split(env("PD_ENDPOINTS", "127.0.0.1:12379"), ","), env("NAMESPACE", "demo/default"), dim)
	if e != nil {
		log.Fatal(e)
	}
	defer repo.Client.Close()
	objects, e := vector.NewObjects(env("S3_ENDPOINT", "127.0.0.1:18333"), env("S3_ACCESS_KEY", "poc-access"), env("S3_SECRET_KEY", "poc-secret-local-only"), env("S3_BUCKET", "vector-index-poc"))
	if e != nil {
		log.Fatal(e)
	}
	partitions, e := strconv.Atoi(env("PARTITIONS", "1"))
	if e != nil {
		log.Fatal("invalid PARTITIONS")
	}
	var service runningService
	var cluster *vector.ClusterService
	if env("CLUSTER_MODE", "0") == "1" {
		ttl, err := time.ParseDuration(env("LEASE_TTL", "9s"))
		if err != nil {
			log.Fatal(err)
		}
		replicas, err := strconv.Atoi(env("REPLICAS", "1"))
		if err != nil {
			log.Fatal(err)
		}
		capacity, err := strconv.ParseFloat(env("WORKER_CAPACITY", "1"), 64)
		if err != nil {
			log.Fatal(err)
		}
		memoryBudget, err := strconv.ParseUint(env("WORKER_MEMORY_BUDGET_BYTES", "0"), 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		cooldown, err := time.ParseDuration(env("BALANCE_COOLDOWN", "10s"))
		if err != nil {
			log.Fatal(err)
		}
		checkpointBytes, err := strconv.ParseUint(env("GRAPH_CHECKPOINT_BYTES", "30000000"), 10, 64)
		if err != nil || checkpointBytes == 0 {
			log.Fatal("invalid GRAPH_CHECKPOINT_BYTES")
		}
		checkpointAge, err := time.ParseDuration(env("GRAPH_CHECKPOINT_AGE", "3m"))
		if err != nil {
			log.Fatal("invalid GRAPH_CHECKPOINT_AGE")
		}
		var routing *vector.ClusterRouting
		if path := os.Getenv("CLUSTER_ROUTING_FILE"); path != "" {
			raw, err := os.ReadFile(path)
			if err != nil {
				log.Fatal(err)
			}
			routing = &vector.ClusterRouting{}
			if err = json.Unmarshal(raw, routing); err != nil {
				log.Fatal(err)
			}
		}
		cluster, e = vector.OpenClusterService(ctx, repo, objects, partitions, vector.ClusterConfig{GraphCheckpointBytes: checkpointBytes, GraphCheckpointAge: checkpointAge, Routing: routing, ConsumerRetention: os.Getenv("CONSUMER_RETENTION") == "1", IndexReplication: env("INDEX_REPLICATION", "independent"), HostID: os.Getenv("HOST_ID"), Zone: os.Getenv("ZONE"), FailureDomain: env("REPLICA_FAILURE_DOMAIN", "node"), MemoryBudgetBytes: memoryBudget, Scheduler: env("SCHEDULER", "hash"), Capacity: capacity, BalanceCooldown: cooldown, Replicas: replicas, NodeID: os.Getenv("NODE_ID"), AdvertiseURL: os.Getenv("ADVERTISE_URL"), Token: os.Getenv("CLUSTER_TOKEN"), LeaseTTL: ttl, Roles: env("ROLES", "master,gateway,worker")})
		if e == nil {
			service = cluster
			defer cluster.Close()
		}
	} else {
		service, e = vector.OpenPartitionedService(ctx, repo, objects, partitions)
	}
	if e != nil {
		log.Fatal(e)
	}
	// Routed replicas start an ownership-scoped event consumer when loaded.
	// Legacy pull-based indexes still use the periodic synchronization loop.
	if cluster == nil || !cluster.EventDrivenIndexing() {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					c, cancel := context.WithTimeout(ctx, 10*time.Second)
					var e error
					if cluster != nil {
						e = cluster.SyncOwned(c)
					} else {
						e = service.Sync(c)
					}
					cancel()
					if e != nil {
						log.Printf("index sync failed: %v", e)
					}
				}
			}
		}()
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c, done := context.WithTimeout(ctx, 2*time.Minute)
				err := service.MaybeRebuild(c)
				done()
				if err != nil && !errors.Is(err, vector.ErrConflict) {
					log.Printf("index rebuild failed: %v", err)
				}
			}
		}
	}()
	var handler http.Handler = newHandler(service, service, dim, token, env("NAMESPACE", "demo/default"))
	if cluster != nil {
		handler = cluster.WrapHandler(handler)
	}
	if addr := os.Getenv("GRPC_LISTEN_ADDR"); addr != "" {
		backend, ok := service.(rpcserver.Backend)
		if !ok {
			log.Fatal("service does not support gRPC")
		}
		var options []grpc.ServerOption
		cert, key := os.Getenv("GRPC_TLS_CERT"), os.Getenv("GRPC_TLS_KEY")
		if cert != "" || key != "" {
			creds, err := credentials.NewServerTLSFromFile(cert, key)
			if err != nil {
				log.Fatal(err)
			}
			options = append(options, grpc.Creds(creds))
		}
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		rpc := rpcserver.New(backend, token, env("NAMESPACE", "demo/default"), cluster == nil || cluster.HasRole(vector.RoleGateway), options...)
		defer rpc.Stop()
		go func() {
			if err := rpc.Serve(listener); err != nil {
				log.Printf("gRPC stopped: %v", err)
				cancel()
			}
		}()
		go func() {
			<-ctx.Done()
			done := make(chan struct{})
			go func() { rpc.GracefulStop(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				rpc.Stop()
			}
		}()
		log.Printf("gRPC listening on %s", addr)
	}
	server := &http.Server{Addr: env("LISTEN_ADDR", "127.0.0.1:18090"), Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(c)
	}()
	log.Printf("listening on %s; HNSW with exact fallback", server.Addr)
	if e = server.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
		log.Fatal(e)
	}
}
