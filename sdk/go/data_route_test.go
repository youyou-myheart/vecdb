package vectorstore

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testWriteNode struct {
	pb.UnimplementedVectorStoreServer
	endpoint string
	epoch    atomic.Uint64
	lookups  atomic.Int64
	calls    atomic.Int64
}

func (n *testWriteNode) ResolveWriteRoute(ctx context.Context, _ *pb.GetRequest) (*pb.WriteRoute, error) {
	n.lookups.Add(1)
	return &pb.WriteRoute{Partitions: 1, Epoch: n.epoch.Load(), Session: "writer", Namespace: "test", Endpoint: n.endpoint}, nil
}
func (n *testWriteNode) check(ctx context.Context, r *pb.WriteRoute) (*pb.MutationResponse, error) {
	if values := metadata.ValueFromIncomingContext(ctx, "authorization"); len(values) != 1 || values[0] != "Bearer sdk-token" {
		return nil, status.Error(codes.Unauthenticated, "missing token")
	}
	if r.GetEpoch() != n.epoch.Load() || r.GetSession() != "writer" {
		return nil, status.Error(codes.FailedPrecondition, "stale")
	}
	n.calls.Add(1)
	return &pb.MutationResponse{Durable: true}, nil
}
func (n *testWriteNode) Put(ctx context.Context, q *pb.PutRequest) (*pb.MutationResponse, error) {
	return n.check(ctx, q.Route)
}
func (n *testWriteNode) Delete(ctx context.Context, q *pb.DeleteRequest) (*pb.MutationResponse, error) {
	return n.check(ctx, q.Route)
}
func startWriteTestNode(t *testing.T, n *testWriteNode) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterVectorStoreServer(s, n)
	go s.Serve(ln)
	t.Cleanup(s.Stop)
	return ln.Addr().String()
}
func TestSDKDirectOwnerAndStaleRoute(t *testing.T) {
	w := &testWriteNode{}
	w.epoch.Store(1)
	gw := &testWriteNode{endpoint: startWriteTestNode(t, w)}
	gw.epoch.Store(1)
	c, err := Dial(context.Background(), startWriteTestNode(t, gw), "sdk-token", Options{Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Insert(context.Background(), "a", []float64{1}, nil, "r1"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Delete(context.Background(), &pb.DeleteRequest{Id: "b", RequestId: "r2"}); err != nil {
		t.Fatal(err)
	}
	if gw.lookups.Load() != 1 || gw.calls.Load() != 0 || w.calls.Load() != 2 {
		t.Fatal("writes did not reuse direct route")
	}
	w.epoch.Store(2)
	gw.epoch.Store(2)
	if _, err = c.Update(context.Background(), "a", []float64{1}, nil, 1, "r3"); err != nil {
		t.Fatal(err)
	}
	if gw.lookups.Load() != 2 || gw.calls.Load() != 0 || w.calls.Load() != 3 {
		t.Fatal("stale route not refreshed")
	}
}
