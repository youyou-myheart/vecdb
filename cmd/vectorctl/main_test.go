package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMutationContracts(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	cases := []struct {
		name     string
		args     []string
		method   string
		revision any
	}{
		{"insert", []string{"--vector", "[1,2]"}, "PUT", float64(0)},
		{"update", []string{"--vector", "[2,3]", "--revision", "7"}, "PUT", float64(7)},
		{"delete", []string{"--revision", "7"}, "DELETE", float64(7)},
		{"upsert", []string{"--vector", "[2,3]"}, "PUT", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.method || r.URL.Path != "/v1/records/a ?#" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Errorf("request: %s %s", r.Method, r.URL)
				}
				var m map[string]any
				if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
					t.Error(err)
				}
				if m["request_id"] != "retry-key" || m["expected_revision"] != tc.revision {
					t.Errorf("mutation: %#v", m)
				}
				_, _ = w.Write([]byte(`{"token":{"revision":8}}`))
			}))
			defer srv.Close()
			args := append([]string{"--endpoint", srv.URL, "record", tc.name, "--id", "a ?#", "--request-id", "retry-key"}, tc.args...)
			var out bytes.Buffer
			if err := run(args, &out, &out); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRejectInvalidInputsBeforeNetwork(t *testing.T) {
	for _, args := range [][]string{
		{"record", "update", "--id", "a", "--vector", "[1]", "--request-id", "key"},
		{"record", "insert", "--id", "a", "--vector", "[1]"},
		{"record", "get", "--id", "../a"},
		{"record", "upsert", "--id", "a", "--request-id", "key", "--vector", "[1e999]"},
		{"record", "insert", "--id", "a", "--request-id", "key", "--vector", "[]"},
		{"search", "--vector", "[1]", "--visible-id", "a"},
		{"search", "--vector", "[1]", "--k", "0"},
		{"cluster", "scale", "--workers", "-1"},
		{"cluster", "status", "--workers", "2"},
	} {
		var out bytes.Buffer
		if err := run(args, &out, &out); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}
func TestSearchAndServerErrors(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q map[string]any
		_ = json.NewDecoder(r.Body).Decode(&q)
		if q["k"] != float64(3) || q["exact"] != true || q["min_visible"].(map[string]any)["revision"] != float64(2) {
			t.Errorf("query: %#v", q)
		}
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":"partition unavailable"}`))
	}))
	defer srv.Close()
	var out bytes.Buffer
	err := run([]string{"--endpoint", srv.URL, "search", "--vector", "[1,2]", "--k", "3", "--exact", "--filter", `{"tag":"a"}`, "--visible-id", "a", "--visible-revision", "2"}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") || out.Len() != 0 {
		t.Fatalf("error=%v output=%s", err, out.String())
	}
}
func TestClusterStatusPreservesLargeEpoch(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"leader":{"epoch":9007199254740993}}`))
	}))
	defer srv.Close()
	var out bytes.Buffer
	if err := run([]string{"--endpoint", srv.URL, "cluster", "leader"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "9007199254740993") {
		t.Fatal(out.String())
	}
}
func TestDoesNotFollowRedirect(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "http://127.0.0.1:1", 307) }))
	defer srv.Close()
	var out bytes.Buffer
	if err := run([]string{"--endpoint", srv.URL, "cluster", "status"}, &out, &out); err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatal(err)
	}
}

func TestStartValidatesCredentialsBeforeLaunching(t *testing.T) {
	for _, key := range []string{"API_TOKEN", "CLUSTER_TOKEN"} {
		for _, value := range []string{"", "short"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				t.Setenv("API_TOKEN", "valid-public-token")
				t.Setenv("CLUSTER_TOKEN", "valid-internal-token")
				t.Setenv(key, value)
				var out bytes.Buffer
				err := run([]string{"--socket", "/tmp/unused-control.sock", "cluster", "start", "--namespace", "TEST", "--launcher", "/does-not-exist.py"}, &out, &out)
				if err == nil || !strings.Contains(err.Error(), "export "+key+"=") || out.Len() != 0 {
					t.Fatalf("error=%v output=%s", err, out.String())
				}
			})
		}
	}
}

func TestListCommand(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/records" || r.URL.Query().Get("cursor") != "a+b/?" || r.URL.Query().Get("limit") != "2" || r.URL.Query().Get("include_deleted") != "true" {
			t.Errorf("request %s", r.URL)
		}
		_, _ = w.Write([]byte(`{"records":[],"next_cursor":"next"}`))
	}))
	defer srv.Close()
	var out bytes.Buffer
	if err := run([]string{"--endpoint", srv.URL, "record", "list", "--limit", "2", "--cursor", "a+b/?", "--include-deleted"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"next_cursor": "next"`) {
		t.Fatal(out.String())
	}
	if err := run([]string{"record", "list", "--limit", "0"}, &out, &out); err == nil {
		t.Fatal("invalid limit accepted")
	}
}

func TestMigrationCommands(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	for _, tc := range []struct {
		args             []string
		path, key, value string
	}{
		{[]string{"tasks", "--limit", "2", "--after", "a ?"}, "/v1/cluster/tasks", "after", "a ?"},
		{[]string{"task", "task ?"}, "/v1/cluster/task", "id", "task ?"},
		{[]string{"explain", "index-0000"}, "/v1/cluster/explain", "partition", "index-0000"},
		{[]string{"balance", "--dry-run"}, "/v1/cluster/balance", "", ""},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" || r.URL.Path != tc.path || r.URL.Query().Get(tc.key) != tc.value {
				t.Error(r.URL)
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		var out bytes.Buffer
		args := append([]string{"--endpoint", srv.URL, "cluster"}, tc.args...)
		if err := run(args, &out, &out); err != nil {
			t.Fatal(err)
		}
		srv.Close()
	}
	for _, args := range [][]string{{"balance"}, {"task"}, {"tasks", "--dry-run"}, {"explain", "a", "b"}, {"tasks", "--limit", "201"}} {
		var out bytes.Buffer
		if err := run(append([]string{"cluster"}, args...), &out, &out); err == nil {
			t.Fatal("invalid command accepted", args)
		}
	}
}

func TestChangeCursorCommand(t *testing.T) {
	t.Setenv("API_TOKEN", "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/changes" || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error(r.Method, r.URL)
		}
		var body struct {
			After     uint64 `json:"after"`
			Partition string `json:"partition"`
			Stream    string `json:"stream"`
		}
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
		}
		if body.After != 9007199254740993 || body.Partition != "index-0000" || body.Stream != "graph" {
			t.Error(body)
		}
		_, _ = w.Write([]byte(`{"next":9007199254740993,"head":9007199254740993}`))
	}))
	defer server.Close()
	var out bytes.Buffer
	if e := run([]string{"--endpoint", server.URL, "changes", "--partition", "index-0000", "--stream", "graph", "--after", "9007199254740993"}, &out, &out); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "9007199254740993") {
		t.Fatal(out.String())
	}
}
