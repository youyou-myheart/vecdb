package rpcserver

import (
	"context"
	pb "example.com/vector-storage/api/vectorstore/v1"
	"example.com/vector-storage/internal/vector"
	"testing"
)

type visibilityBackend struct {
	Backend
	barriers int
}

func (*visibilityBackend) VectorDimension() int         { return 2 }
func (*visibilityBackend) HandlesMinVisible() bool      { return true }
func (b *visibilityBackend) Sync(context.Context) error { b.barriers++; return nil }
func (b *visibilityBackend) SearchContext(ctx context.Context, q vector.Search) ([]vector.Hit, error) {
	if q.MinVisible != nil {
		if err := b.Sync(ctx); err != nil {
			return nil, err
		}
	}
	return []vector.Hit{{ID: "a", Revision: 1}}, nil
}
func TestRPCDoesNotRepeatBackendVisibilityBarrier(t *testing.T) {
	b := &visibilityBackend{}
	server := &Server{backend: b}
	_, err := server.Search(context.Background(), &pb.SearchRequest{Vector: []float64{1, 0}, K: 1, MinVisible: &pb.Token{Id: "a", Revision: 1}})
	if err != nil || b.barriers != 1 {
		t.Fatal(err, b.barriers)
	}
}
