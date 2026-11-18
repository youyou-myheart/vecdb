package main

import (
	"context"
	"testing"

	"example.com/vector-storage/internal/vector"
)

type followerBackend struct {
	*fakeBackend
	err error
}

func (b followerBackend) FollowerSearch(context.Context, vector.Search) (vector.FollowerSearchResult, error) {
	b.calls++
	return vector.FollowerSearchResult{Hits: []vector.Hit{{ID: "follower"}}}, b.err
}
func TestFollowerSearchHTTP(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{{nil, 200}, {context.DeadlineExceeded, 504}, {vector.ErrFollowerQueueFull, 429}, {vector.ErrFollowerReadUnsupported, 501}, {vector.ErrFenced, 503}} {
		b := followerBackend{backend(), tc.err}
		h := newHandler(b, b, 2, testToken, "ns")
		w := request(h, "POST", "/v1/follower-search", `{"vector":[1,2],"k":1}`, true)
		if w.Code != tc.status || b.calls != 1 {
			t.Fatal(w.Code, w.Body.String(), b.calls)
		}
	}
	b := followerBackend{backend(), nil}
	h := newHandler(b, b, 2, testToken, "ns")
	for _, tc := range []struct {
		method, body string
		auth         bool
		status       int
	}{
		{"POST", `{"vector":[1,2],"k":1}`, false, 401},
		{"GET", `{}`, true, 405},
		{"POST", `{"vector":[1,2],"k":1,"min_visible":{"id":"a","revision":1}}`, true, 400},
		{"POST", `{"vector":[1,2],"k":1,"ef_search":4097}`, true, 400},
	} {
		if w := request(h, tc.method, "/v1/follower-search", tc.body, tc.auth); w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if b.calls != 0 {
		t.Fatal("invalid request reached backend")
	}
}
