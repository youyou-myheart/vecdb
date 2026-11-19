package vectorstore

import (
	"context"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func queryTestState() *pb.ClusterStatus {
	s := &pb.ClusterStatus{Routing: &pb.ClusterRouting{Version: 1, Centers: []*pb.Center{{Vector: []float64{0}}, {Vector: []float64{10}}, {Vector: []float64{20}}}}, ReplicaOwners: map[string]*pb.ReplicaOwners{}}
	for i, id := range []string{"index-0000", "index-0001", "index-0002"} {
		count := []uint64{3, 5, 20}[i]
		node := &pb.Node{Id: id, Session: "s", Load: map[string]*pb.ShardLoad{id: {LiveRecords: count, RecordsKnown: true}}}
		s.Nodes = append(s.Nodes, node)
		s.ReplicaOwners[id] = &pb.ReplicaOwners{Owners: []*pb.Owner{{Node: id, Session: "s", Ready: true}, {Node: id, Session: "s", Ready: true}}}
	}
	return s
}

func TestQueryNearestAndReplicaCounts(t *testing.T) {
	rows := rankQuery(queryTestState(), []float64{0})
	if initialFanout(rows, 4) != 2 || initialFanout(rows, 2) != 1 {
		t.Fatal(rows)
	}
	if rankQuery(queryTestState(), []float64{19})[0].id != "index-0002" {
		t.Fatal("not nearest")
	}
	s := queryTestState()
	s.Nodes[0].Load["index-0000"].RecordsKnown = false
	if initialFanout(rankQuery(s, []float64{0}), 1) != 3 {
		t.Fatal("unknown count silently pruned")
	}
}

type queryTestServer struct {
	pb.UnimplementedVectorStoreServer
	refreshes atomic.Int64
	mu        sync.Mutex
	calls     [][]string
}

func (s *queryTestServer) GetSearchRoutes(context.Context, *pb.Empty) (*pb.ClusterStatus, error) {
	s.refreshes.Add(1)
	return queryTestState(), nil
}
func (s *queryTestServer) Get(context.Context, *pb.GetRequest) (*pb.Record, error) {
	return &pb.Record{Vector: []float64{19}}, nil
}
func (s *queryTestServer) Search(_ context.Context, q *pb.SearchRequest) (*pb.SearchResponse, error) {
	s.mu.Lock()
	s.calls = append(s.calls, append([]string(nil), q.IndexPartitions...))
	s.mu.Unlock()
	n := 1
	if len(q.IndexPartitions) == 3 || len(q.IndexPartitions) == 0 {
		n = int(q.K)
	}
	out := &pb.SearchResponse{}
	for i := 0; i < n; i++ {
		out.Hits = append(out.Hits, &pb.Hit{})
	}
	return out, nil
}
func TestSDKSearchExpandsAndRefreshes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	backend := &queryTestServer{}
	pb.RegisterVectorStoreServer(server, backend)
	go server.Serve(ln)
	defer server.Stop()
	c, err := Dial(context.Background(), ln.Addr().String(), "token", Options{Timeout: time.Second, SearchRefreshInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	request := &pb.SearchRequest{Vector: []float64{0}, K: 4}
	original := proto.Clone(request)
	out, err := c.Search(context.Background(), request)
	if err != nil || len(out.Hits) != 4 {
		t.Fatal(out, err)
	}
	if !proto.Equal(request, original) {
		t.Fatal("caller query mutated")
	}
	backend.mu.Lock()
	calls := append([][]string(nil), backend.calls...)
	backend.mu.Unlock()
	if !reflect.DeepEqual(calls, [][]string{{"index-0000", "index-0001"}, {"index-0000", "index-0001", "index-0002"}}) {
		t.Fatal(calls)
	}
	deadline := time.Now().Add(time.Second)
	for backend.refreshes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if backend.refreshes.Load() < 2 {
		t.Fatal("no periodic refresh")
	}
	if _, err = c.SearchByID(context.Background(), "A", &pb.SearchRequest{K: 1}); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	last := backend.calls[len(backend.calls)-1]
	backend.mu.Unlock()
	if !reflect.DeepEqual(last, []string{"index-0002"}) {
		t.Fatal(last)
	}
	if _, err = c.SearchAll(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	last = backend.calls[len(backend.calls)-1]
	backend.mu.Unlock()
	if len(last) != 0 {
		t.Fatal("SearchAll pruned shards", last)
	}
}
