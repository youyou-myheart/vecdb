package main

import (
	"context"
	"example.com/vector-storage/internal/vector"
	"testing"
)

type barrierBackend struct {
	*fakeBackend
	barriers int
}

func (b *barrierBackend) HandlesMinVisible() bool { return true }
func (b *barrierBackend) Sync(ctx context.Context) error {
	b.barriers++
	return b.fakeBackend.Sync(ctx)
}
func (b *barrierBackend) SearchContext(ctx context.Context, q vector.Search) ([]vector.Hit, error) {
	if q.MinVisible != nil {
		if err := b.Sync(ctx); err != nil {
			return nil, err
		}
	}
	return b.fakeBackend.Search(q)
}
func TestHTTPDoesNotRepeatBackendVisibilityBarrier(t *testing.T) {
	b := &barrierBackend{fakeBackend: backend()}
	if err := b.index.Apply([]vector.Record{{ID: "a", Revision: 1, Vector: []float64{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	w := request(newHandler(b, b, 2, testToken, "ns"), "POST", "/v1/search", `{"vector":[0,0],"k":1,"min_visible":{"id":"a","revision":1}}`, true)
	if w.Code != 200 || b.barriers != 1 {
		t.Fatal(w.Code, w.Body.String(), b.barriers)
	}
}
