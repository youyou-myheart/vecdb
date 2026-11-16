package main

import (
	"context"
	"example.com/vector-storage/internal/vector"
	"testing"
)

type changesBackend struct {
	*fakeBackend
	err error
}

func (b changesBackend) ReadChanges(_ context.Context, q vector.ChangeRequest) (vector.ChangePage, error) {
	return vector.ChangePage{Partition: q.Partition, Stream: q.Stream, Next: q.After}, b.err
}
func TestChangesHTTP(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{{nil, 200}, {vector.ErrInvalidSubscription, 400}, {vector.ErrSubscriptionUnavailable, 501}} {
		b := changesBackend{backend(), tc.err}
		h := newHandler(b, b, 2, testToken, "test")
		w := request(h, "POST", "/v1/changes", `{"partition":"index-0000","stream":"graph","after":3}`, true)
		if w.Code != tc.code {
			t.Fatal(w.Code, w.Body.String())
		}
		if w = request(h, "POST", "/v1/changes", `{}`, false); w.Code != 401 {
			t.Fatal(w.Code)
		}
		if w = request(h, "POST", "/v1/changes", `{"unknown":1}`, true); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
}

func (b changesBackend) ManageConsumer(_ context.Context, q vector.ConsumerRequest) (vector.ConsumerState, error) {
	return vector.ConsumerState{Head: q.Ack}, b.err
}
func TestConsumerHTTP(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{{nil, 200}, {vector.ErrFenced, 409}, {vector.ErrConflict, 409}, {vector.ErrLogCompacted, 410}, {vector.ErrInvalidSubscription, 400}, {vector.ErrSubscriptionUnavailable, 501}} {
		b := changesBackend{backend(), tc.err}
		h := newHandler(b, b, 2, testToken, "test")
		w := request(h, "POST", "/v1/consumers", `{"partition":"index-0000","stream":"graph","action":"list"}`, true)
		if w.Code != tc.code {
			t.Fatal(w.Code, w.Body.String())
		}
		if w = request(h, "POST", "/v1/consumers", `{}`, false); w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
}
