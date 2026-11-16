package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type metricsBackend struct {
	*fakeBackend
	err error
}

func (m *metricsBackend) Metrics(context.Context) (string, error) { return "sample 1\n", m.err }
func TestMetricsHandler(t *testing.T) {
	m := &metricsBackend{fakeBackend: backend()}
	h := newHandler(m, m, 2, testToken, "ns")
	for _, tc := range []struct {
		method string
		auth   bool
		status int
	}{{"GET", false, 401}, {"POST", true, 405}, {"GET", true, 200}} {
		w := request(h, tc.method, "/metrics", "", tc.auth)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
		if w.Code == 200 && (!strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain;") || w.Body.String() != "sample 1\n") {
			t.Fatal(w)
		}
	}
	m.err = errors.New("storage unavailable")
	if w := request(h, "GET", "/metrics", "", true); w.Code != 503 {
		t.Fatal(w.Code)
	}
	f := backend()
	if w := request(newHandler(f, f, 2, testToken, "ns"), "GET", "/metrics", "", true); w.Code != 501 {
		t.Fatal(w.Code)
	}
}
