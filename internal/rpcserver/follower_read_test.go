package rpcserver_test

import (
	"context"
	"testing"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"example.com/vector-storage/internal/vector"
	sdk "example.com/vector-storage/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (*backend) FollowerSearch(ctx context.Context, q vector.Search) (vector.FollowerSearchResult, error) {
	switch q.Filter["mode"] {
	case "full":
		return vector.FollowerSearchResult{}, vector.ErrFollowerQueueFull
	case "unsupported":
		return vector.FollowerSearchResult{}, vector.ErrFollowerReadUnsupported
	case "slow":
		<-ctx.Done()
		return vector.FollowerSearchResult{}, ctx.Err()
	}
	return vector.FollowerSearchResult{Hits: []vector.Hit{{ID: "follower-result"}}, FollowerReads: []vector.FollowerReadInfo{{IndexPartition: "index-0000", Replica: "replica-b", Node: "worker-b", Session: "session-b", OwnerEpoch: 9007199254740993, Target: vector.GraphPosition{Sequence: 9007199254740993, Hash: "h"}, Applied: vector.GraphPosition{Sequence: 9007199254740994}, QueueMS: .2, LeaderCaptureMS: 1, WaitApplyMS: 10, QueryMS: 2}}}, nil
}

func TestFollowerSearchSDKAndGRPC(t *testing.T) {
	c, err := sdk.Dial(context.Background(), serve(t, true), "rpc-unit-token-12345", sdk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	q := &pb.SearchRequest{Vector: []float64{1, 2}, K: 1}
	r, err := c.FollowerSearch(context.Background(), q)
	if err != nil || len(r.Hits) != 1 || r.Hits[0].Id != "follower-result" || len(r.FollowerReads) != 1 {
		t.Fatal(r, err)
	}
	info := r.FollowerReads[0]
	if info.Target.Sequence != 9007199254740993 || info.OwnerEpoch != 9007199254740993 || info.WaitApplyMs != 10 || info.Replica != "replica-b" {
		t.Fatal(info)
	}
	for mode, want := range map[string]codes.Code{"full": codes.ResourceExhausted, "unsupported": codes.FailedPrecondition, "slow": codes.DeadlineExceeded} {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		q.Filter = map[string]string{"mode": mode}
		_, err = c.FollowerSearch(ctx, q)
		cancel()
		if status.Code(err) != want {
			t.Fatal(mode, err)
		}
	}
	q.Filter = nil
	q.MinVisible = &pb.Token{Id: "a", Revision: 1}
	if _, err = c.FollowerSearch(context.Background(), q); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	worker, err := sdk.Dial(context.Background(), serve(t, false), "rpc-unit-token-12345", sdk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if _, err = worker.FollowerSearch(context.Background(), q); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}
