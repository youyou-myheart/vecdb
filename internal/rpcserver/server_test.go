package rpcserver_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"google.golang.org/grpc/credentials"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"example.com/vector-storage/internal/rpcserver"
	"example.com/vector-storage/internal/vector"
	sdk "example.com/vector-storage/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type backend struct{ rpcserver.Backend }

func (*backend) VectorDimension() int { return 2 }
func (*backend) Get(ctx context.Context, id string) (vector.Record, error) {
	if id == "missing" {
		return vector.Record{}, vector.ErrNotFound
	}
	if id == "slow" {
		<-ctx.Done()
		return vector.Record{}, ctx.Err()
	}
	return vector.Record{ID: id, Revision: 9007199254740993, Vector: []float64{1, 2}}, nil
}
func (*backend) Mutate(_ context.Context, id string, m vector.Mutation, del bool) (vector.Record, error) {
	if m.ExpectedRevision != nil && *m.ExpectedRevision != 0 {
		return vector.Record{}, vector.ErrConflict
	}
	return vector.Record{ID: id, Revision: 1, Deleted: del}, nil
}
func (*backend) Layout() vector.PartitionLayout {
	return vector.PartitionLayout{Version: 1, Epoch: 9007199254740993, DataPartitions: 1}
}
func (*backend) ListRecords(context.Context, vector.ListOptions) (vector.RecordPage, error) {
	return vector.RecordPage{Records: []vector.Record{{ID: "a", Vector: []float64{1, 2}}}, NextCursor: "next"}, nil
}
func (*backend) Sync(context.Context) error { return nil }
func (*backend) SearchContext(context.Context, vector.Search) ([]vector.Hit, error) {
	return []vector.Hit{{ID: "a", Revision: 1, Distance: 0}}, nil
}
func (*backend) Status(context.Context) (vector.ClusterStatus, error) {
	return vector.ClusterStatus{IndexReplication: "graph", GraphHeads: map[vector.IndexPartitionID]vector.GraphHead{"index-0000": {Term: 3, Committed: vector.GraphPosition{Sequence: 9007199254740993, DataOffset: 9007199254740994, Hash: "chain"}}}, GraphPositions: map[vector.IndexPartitionID]vector.GraphPosition{"replica": {Sequence: 7, DataOffset: 8, Hash: "old"}}, Ready: true, FailureDomain: "host", Node: vector.ClusterNode{HostID: "host-a", Zone: "zone-a", MemoryBudgetBytes: 1024}, Leader: vector.Ownership{Epoch: 9007199254740993, Phase: "Ready", LastError: "previous failure"}, ReplicaPlans: map[vector.IndexPartitionID]vector.ReplicaPlan{"index-0000": {Version: 7, TaskID: "task-7"}}, LogHeads: map[vector.IndexPartitionID]uint64{"index-0000": 9007199254740993}, ReplicaLag: map[vector.IndexPartitionID]uint64{"replica": 3}, RoutingCooldown: map[string]int64{"node/s": 1000}}, nil
}
func (*backend) Checkpoint(context.Context) (vector.Manifest, error) {
	return vector.Manifest{Generation: "snapshot", Offset: 9007199254740993, Partitions: map[string]vector.Manifest{"i": {Generation: "child"}}}, nil
}
func (*backend) Rebuild(context.Context) error { return nil }
func (*backend) PruneState(context.Context) (any, error) {
	return map[string]any{"partitions": map[string]uint64{"i": 9007199254740993}}, nil
}
func serve(t *testing.T, gateway bool) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := rpcserver.New(&backend{}, "rpc-unit-token-12345", "unit", gateway)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String()
}
func TestSDKAndGRPC(t *testing.T) {
	target := serve(t, true)
	ctx := context.Background()
	c, e := sdk.Dial(ctx, target, "rpc-unit-token-12345", sdk.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	r, e := c.Get(ctx, &pb.GetRequest{Id: "a"})
	if e != nil || r.Revision != 9007199254740993 {
		t.Fatalf("%v %v", r, e)
	}
	if _, e = c.Get(ctx, &pb.GetRequest{Id: "missing"}); status.Code(e) != codes.NotFound {
		t.Fatal(e)
	}
	if _, e = c.Insert(ctx, "a", []float64{1, 2}, nil, "insert"); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Update(ctx, "a", []float64{1, 2}, nil, 1, "update"); status.Code(e) != codes.Aborted {
		t.Fatal(e)
	}
	if _, e = c.Put(ctx, &pb.PutRequest{Id: "a", RequestId: "x", Vector: []float64{1}}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	if _, e = c.Search(ctx, &pb.SearchRequest{Vector: []float64{1, 2}, K: 0}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	hits, e := c.Search(ctx, &pb.SearchRequest{Vector: []float64{1, 2}, K: 1, MinVisible: &pb.Token{Id: "a", Revision: 1}})
	if e != nil || len(hits.Hits) != 1 {
		t.Fatalf("%v %v", hits, e)
	}
	page, e := c.List(ctx, &pb.ListRequest{})
	if e != nil || page.NextCursor != "next" {
		t.Fatalf("%v %v", page, e)
	}
	layout, e := c.GetLayout(ctx, &pb.Empty{})
	if e != nil || layout.Epoch != 9007199254740993 {
		t.Fatalf("%v %v", layout, e)
	}
	cluster, e := c.GetCluster(ctx, &pb.Empty{})
	if e != nil || cluster.IndexReplication != "graph" || cluster.GraphHeads["index-0000"].Committed.Sequence != 9007199254740993 || cluster.GraphPositions["replica"].Hash != "old" || cluster.Leader.Epoch != 9007199254740993 || cluster.FailureDomain != "host" || cluster.Node.HostId != "host-a" || cluster.ReplicaPlans["index-0000"].TaskId != "task-7" || cluster.LogHeads["index-0000"] != 9007199254740993 {
		t.Fatalf("%v %v", cluster, e)
	}
	manifest, e := c.Rebuild(ctx, &pb.Empty{})
	if e != nil || manifest.Offset != 9007199254740993 || manifest.Partitions["i"].Generation != "child" {
		t.Fatalf("%v %v", manifest, e)
	}
	floors, e := c.Prune(ctx, &pb.Empty{})
	if e != nil || floors.Partitions["i"] != 9007199254740993 {
		t.Fatalf("%v %v", floors, e)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, e = c.Get(short, &pb.GetRequest{Id: "slow"}); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
}
func TestAuthAndRole(t *testing.T) {
	target := serve(t, false)
	ctx := context.Background()
	conn, e := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	if _, e = pb.NewVectorStoreClient(conn).Health(ctx, &pb.Empty{}); status.Code(e) != codes.Unauthenticated {
		t.Fatal(e)
	}
	c, e := sdk.Dial(ctx, target, "rpc-unit-token-12345", sdk.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.Health(ctx, &pb.Empty{}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Get(ctx, &pb.GetRequest{Id: "a"}); status.Code(e) != codes.PermissionDenied {
		t.Fatal(e)
	}
}

func TestSDKWithTLS(t *testing.T) {
	certificateServer := httptest.NewTLSServer(nil)
	config := certificateServer.TLS.Clone()
	pool := x509.NewCertPool()
	pool.AddCert(certificateServer.Certificate())
	certificateServer.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpcserver.New(&backend{}, "rpc-unit-token-12345", "tls", true, grpc.Creds(credentials.NewTLS(config)))
	go server.Serve(listener)
	defer server.Stop()
	client, err := sdk.Dial(context.Background(), listener.Addr().String(), "rpc-unit-token-12345", sdk.Options{TLSConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err = client.Health(context.Background(), &pb.Empty{}); err != nil {
		t.Fatal(err)
	}
}

func (*backend) ReadChanges(_ context.Context, q vector.ChangeRequest) (vector.ChangePage, error) {
	if q.Stream != "data" && q.Stream != "graph" {
		return vector.ChangePage{}, vector.ErrInvalidSubscription
	}
	return vector.ChangePage{Partition: q.Partition, Stream: q.Stream, Next: 9007199254740993, Head: 9007199254740993, Data: []vector.LogEvent{{Sequence: 9007199254740993, Record: vector.Record{ID: "a", Revision: 3, Deleted: true}}}}, nil
}
func TestChangesGRPC(t *testing.T) {
	c, e := sdk.Dial(context.Background(), serve(t, true), "rpc-unit-token-12345", sdk.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	page, e := c.ReadChanges(context.Background(), &pb.ChangeRequest{Partition: "index-0000", Stream: "data", After: 9007199254740992, Limit: 1})
	if e != nil || page.Next != 9007199254740993 || len(page.Data) != 1 || !page.Data[0].Record.Deleted {
		t.Fatal(page, e)
	}
	if _, e = c.ReadChanges(context.Background(), &pb.ChangeRequest{Stream: "bad"}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	if _, e = c.ReadChanges(context.Background(), &pb.ChangeRequest{Stream: "data", Limit: 257}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	worker, e := sdk.Dial(context.Background(), serve(t, false), "rpc-unit-token-12345", sdk.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer worker.Close()
	if _, e = worker.ReadChanges(context.Background(), &pb.ChangeRequest{Stream: "data"}); status.Code(e) != codes.PermissionDenied {
		t.Fatal(e)
	}
}
