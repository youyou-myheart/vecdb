package vectorstore

import (
	"context"
	pb "example.com/vector-storage/api/vectorstore/v1"
	"os"
	"testing"
)

func TestSDKRealCluster(t *testing.T) {
	if os.Getenv("RPC_ACCEPTANCE") != "1" {
		t.Skip("real gRPC acceptance disabled")
	}
	ctx := context.Background()
	c, e := Dial(ctx, os.Getenv("RPC_TARGET"), os.Getenv("API_TOKEN"), Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	r, e := c.Get(ctx, &pb.GetRequest{Id: "python"})
	if e != nil || r.Revision != 2 {
		t.Fatalf("%v %v", r, e)
	}
	inserted, e := c.Insert(ctx, "golang", []float64{2, 2, 2}, map[string]string{"lang": "go"}, "go-insert")
	if e != nil {
		t.Fatal(e)
	}
	again, e := c.Insert(ctx, "golang", []float64{2, 2, 2}, map[string]string{"lang": "go"}, "go-insert")
	if e != nil || again.Token.Revision != inserted.Token.Revision {
		t.Fatalf("%v %v", again, e)
	}
	hits, e := c.Search(ctx, &pb.SearchRequest{Vector: []float64{2, 2, 2}, K: 1, MinVisible: inserted.Token})
	if e != nil || len(hits.Hits) != 1 || hits.Hits[0].Id != "golang" {
		t.Fatalf("%v %v", hits, e)
	}
	if _, e = c.Update(ctx, "golang", []float64{3, 3, 3}, nil, 1, "go-update"); e != nil {
		t.Fatal(e)
	}
}
