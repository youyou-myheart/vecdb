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
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testWriteNode struct {
	pb.UnimplementedVectorStoreServer
	endpoint string
	epoch    atomic.Uint64
	lookups  atomic.Int64
	calls    atomic.Int64
	failure  atomic.Int32
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
	if code := codes.Code(n.failure.Load()); code != codes.OK {
		return nil, status.Error(code, "temporary write failure")
	}
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
	link := c.routes[0].link
	w.epoch.Store(2)
	gw.epoch.Store(2)
	if _, err = c.Update(context.Background(), "a", []float64{1}, nil, 1, "r3"); err != nil {
		t.Fatal(err)
	}
	if gw.lookups.Load() != 2 || gw.calls.Load() != 0 || w.calls.Load() != 3 {
		t.Fatal("stale route not refreshed")
	}
	if c.routes[0].link != link {
		t.Fatal("same-endpoint ownership change replaced the connection")
	}
}

func TestSDKWriteFailuresRefreshRouteWithoutReconnecting(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			w := &testWriteNode{}
			w.epoch.Store(1)
			w.failure.Store(int32(code))
			gw := &testWriteNode{endpoint: startWriteTestNode(t, w)}
			gw.epoch.Store(1)
			c, err := Dial(context.Background(), startWriteTestNode(t, gw), "sdk-token", Options{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var link *dataRouteConnection
			for n := 0; n < 20; n++ {
				if _, err := c.Insert(context.Background(), "a", []float64{1}, nil, "request"); status.Code(err) != code {
					t.Fatal(err)
				}
				if link == nil {
					link = c.routes[0].link
				}
				if c.routes[0].link != link || link.conn.GetState() == connectivity.Shutdown || link.users != 0 {
					t.Fatal("failed request discarded transport or leaked a user")
				}
			}
			if w.calls.Load() != 20 || gw.lookups.Load() != 20 {
				t.Fatal("ambiguous writes were retried or route refresh skipped", w.calls.Load(), gw.lookups.Load())
			}
			w.failure.Store(0)
			if _, err := c.Insert(context.Background(), "a", []float64{1}, nil, "request"); err != nil {
				t.Fatal("transport did not recover", err)
			}
		})
	}
}

type fixedWriteResolver struct {
	pb.VectorStoreClient
	hint *pb.WriteRoute
}

func (r *fixedWriteResolver) ResolveWriteRoute(context.Context, *pb.GetRequest, ...grpc.CallOption) (*pb.WriteRoute, error) {
	return r.hint, nil
}

func TestSDKRouteRefreshKeepsInflightGenerationAndRetiresOldEndpoint(t *testing.T) {
	w := &testWriteNode{}
	w.epoch.Store(1)
	endpoint := startWriteTestNode(t, w)
	c, err := Dial(context.Background(), endpoint, "sdk-token", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	resolver := &fixedWriteResolver{hint: &pb.WriteRoute{Partitions: 1, Endpoint: endpoint, Epoch: 1}}
	c.VectorStoreClient = resolver
	old, err := c.acquireRoute(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := c.acquireRoute(context.Background(), "a")
	c.releaseRoute(second, status.Error(codes.Unavailable, "temporary"))
	resolver.hint = &pb.WriteRoute{Partitions: 1, Endpoint: endpoint, Epoch: 2}
	fresh, err := c.acquireRoute(context.Background(), "a")
	if err != nil || fresh.link != old.link || fresh.route.Epoch != 2 || old.route.Epoch != 1 {
		t.Fatal("refresh changed the inflight route or transport", err)
	}
	c.releaseRoute(old, status.Error(codes.DeadlineExceeded, "late failure"))
	if fresh.stale || fresh.link.conn.GetState() == connectivity.Shutdown {
		t.Fatal("late old request invalidated the refreshed route")
	}
	// Keep one old-endpoint request in flight across the endpoint change.
	hold, _ := c.acquireRoute(context.Background(), "a")
	c.releaseRoute(fresh, status.Error(codes.FailedPrecondition, "moved"))
	resolver.hint = &pb.WriteRoute{Partitions: 1, Endpoint: startWriteTestNode(t, &testWriteNode{}), Epoch: 3}
	moved, err := c.acquireRoute(context.Background(), "a")
	if err != nil || moved.link == hold.link || hold.link.conn.GetState() == connectivity.Shutdown {
		t.Fatal("endpoint replacement interrupted an inflight request", err)
	}
	c.releaseRoute(hold, nil)
	if hold.link.conn.GetState() != connectivity.Shutdown {
		t.Fatal("unused old endpoint was not closed")
	}
	c.releaseRoute(moved, nil)
	if err := c.Close(); err != nil || moved.link.conn.GetState() != connectivity.Shutdown {
		t.Fatal("client close leaked the current connection", err)
	}
	if _, err := c.acquireRoute(context.Background(), "a"); err != grpc.ErrClientConnClosing {
		t.Fatal("closed client accepted another route", err)
	}
}
