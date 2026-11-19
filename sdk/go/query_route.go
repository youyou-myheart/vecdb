package vectorstore

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type queryPartition struct {
	id       string
	distance float64
	count    uint64
	known    bool
}

func rankQuery(state *pb.ClusterStatus, vector []float64) []queryPartition {
	if state == nil || state.Routing == nil || state.Routing.Version != 1 || len(state.Routing.Centers) == 0 || len(state.Routing.Centers) > 64 {
		return nil
	}
	nodes := map[string]*pb.Node{}
	for _, n := range state.Nodes {
		nodes[n.Id] = n
	}
	rows := make([]queryPartition, 0, len(state.Routing.Centers))
	for i, center := range state.Routing.Centers {
		if len(center.Vector) != len(vector) {
			return nil
		}
		row := queryPartition{id: fmt.Sprintf("index-%04d", i)}
		for j, x := range vector {
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return nil
			}
			d := x - center.Vector[j]
			row.distance += d * d
		}
		if owners := state.ReplicaOwners[row.id]; owners != nil {
			for _, o := range owners.Owners {
				n := nodes[o.Node]
				if !o.Ready || n == nil || n.Session != o.Session {
					continue
				}
				if load := n.Load[row.id]; load != nil && load.RecordsKnown {
					row.known = true
					row.count = max(row.count, load.LiveRecords)
				}
			}
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].distance == rows[j].distance {
			return rows[i].id < rows[j].id
		}
		return rows[i].distance < rows[j].distance
	})
	return rows
}

func initialFanout(rows []queryPartition, k uint32) int {
	for _, r := range rows {
		if !r.known {
			return len(rows)
		}
	}
	var total uint64
	for i, r := range rows {
		total += r.count
		if total >= uint64(k) {
			return i + 1
		}
	}
	return len(rows)
}

func (c *Client) refreshQueries(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, min(c.timeout, 2*time.Second))
	defer cancel()
	state, err := c.VectorStoreClient.GetSearchRoutes(ctx, &pb.Empty{})
	if status.Code(err) == codes.Unimplemented {
		state, err = c.VectorStoreClient.GetCluster(ctx, &pb.Empty{})
	}
	if err == nil {
		c.queryMu.Lock()
		c.queryState = state
		c.queryUpdated = time.Now()
		c.queryMu.Unlock()
	}
}

func (c *Client) queryRoutes(ctx context.Context, vector []float64) ([]queryPartition, error) {
	c.queryMu.Lock()
	if c.queryClosed {
		c.queryMu.Unlock()
		return nil, status.Error(codes.Canceled, "client closed")
	}
	first := !c.queryStarted
	if first {
		c.queryStarted = true
		bg, cancel := context.WithCancel(context.Background())
		c.queryCancel = cancel
		c.queryWG.Add(1)
		go func() {
			defer c.queryWG.Done()
			timer := time.NewTicker(c.queryInterval)
			defer timer.Stop()
			for {
				select {
				case <-bg.Done():
					return
				case <-timer.C:
					c.refreshQueries(bg)
				}
			}
		}()
	}
	c.queryMu.Unlock()
	if first {
		c.refreshQueries(ctx)
	}
	c.queryMu.Lock()
	defer c.queryMu.Unlock()
	if time.Since(c.queryUpdated) > max(30*time.Second, 3*c.queryInterval) {
		return nil, nil
	}
	return rankQuery(c.queryState, vector), nil
}

// Search selects the nearest clusters covering the estimated K records. It reads
// eventual applied index views. Exact/min_visible queries retain all-shard search;
// explicit IndexPartitions restrict exactness to those partitions.
func (c *Client) Search(ctx context.Context, request *pb.SearchRequest, opts ...grpc.CallOption) (*pb.SearchResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "query required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	q := proto.Clone(request).(*pb.SearchRequest)
	var rows []queryPartition
	if !q.Exact && q.MinVisible == nil && len(q.IndexPartitions) == 0 {
		var err error
		rows, err = c.queryRoutes(ctx, q.Vector)
		if err != nil {
			return nil, err
		}
	}
	count := initialFanout(rows, q.K)
	for {
		if len(rows) > 0 {
			q.IndexPartitions = nil
			for _, r := range rows[:count] {
				q.IndexPartitions = append(q.IndexPartitions, r.id)
			}
		}
		out, err := c.VectorStoreClient.Search(ctx, q, opts...)
		if err != nil || len(rows) == 0 || len(out.Hits) >= int(q.K) || count == len(rows) {
			return out, err
		}
		count++
	}
}

// SearchAll keeps global shard coverage while still reading eventual index views.
func (c *Client) SearchAll(ctx context.Context, q *pb.SearchRequest, opts ...grpc.CallOption) (*pb.SearchResponse, error) {
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "query required")
	}
	all := proto.Clone(q).(*pb.SearchRequest)
	all.IndexPartitions = nil
	return c.VectorStoreClient.Search(ctx, all, opts...)
}

// SearchByID fetches A's current record once, then uses its vector for routing.
func (c *Client) SearchByID(ctx context.Context, id string, q *pb.SearchRequest, opts ...grpc.CallOption) (*pb.SearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	record, err := c.Get(ctx, &pb.GetRequest{Id: id}, opts...)
	if err != nil {
		return nil, err
	}
	if record.Deleted {
		return nil, status.Error(codes.NotFound, "query record deleted")
	}
	if q == nil {
		q = &pb.SearchRequest{K: 10}
	} else {
		q = proto.Clone(q).(*pb.SearchRequest)
	}
	q.Vector = record.Vector
	return c.Search(ctx, q, opts...)
}
