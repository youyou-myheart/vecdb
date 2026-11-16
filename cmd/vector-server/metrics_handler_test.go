package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type metricsBackend struct {
	*fakeBackend
	err, localErr error
}

func (m *metricsBackend) Metrics(context.Context) (string, error) { return "sample 1\n", m.err }
func (m *metricsBackend) LocalMetrics() (string, error)           { return "local_sample 1\n", m.localErr }
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
	for _, tc := range []struct {
		path   string
		auth   bool
		status int
	}{
		{"/metrics?scope=local", true, 200},
		{"/metrics?scope=local", false, 401},
		{"/metrics?scope=unknown", true, 400},
	} {
		w := request(h, "GET", tc.path, "", tc.auth)
		if w.Code != tc.status || (w.Code == 200 && w.Body.String() != "local_sample 1\n") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	f := backend()
	m.localErr = errors.New("metrics encoding failed")
	if w := request(h, "GET", "/metrics?scope=local", "", true); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if w := request(newHandler(f, f, 2, testToken, "ns"), "GET", "/metrics", "", true); w.Code != 501 {
		t.Fatal(w.Code)
	}
}
