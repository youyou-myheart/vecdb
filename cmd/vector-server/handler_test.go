package main

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/vector-storage/internal/vector"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testToken = "unit-test-token-12345"

type fakeBackend struct {
	getErr, mutateErr, syncErr, searchErr, checkpointErr error
	calls                                                int
	deleted                                              bool
	index                                                *vector.ExactIndex
	mutation                                             vector.Mutation
}

func (f *fakeBackend) Get(context.Context, string) (vector.Record, error) {
	f.calls++
	return vector.Record{ID: "a", Revision: 2}, f.getErr
}
func (f *fakeBackend) Mutate(_ context.Context, id string, m vector.Mutation, deleted bool) (vector.Record, error) {
	f.calls++
	f.deleted = deleted
	f.mutation = m
	return vector.Record{ID: id, Revision: 2}, f.mutateErr
}
func (f *fakeBackend) Sync(context.Context) error { f.calls++; return f.syncErr }
func (f *fakeBackend) Search(q vector.Search) ([]vector.Hit, error) {
	f.calls++
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	return f.index.Search(q)
}
func (f *fakeBackend) Checkpoint(context.Context) (vector.Manifest, error) {
	f.calls++
	return vector.Manifest{Generation: "g"}, f.checkpointErr
}
func backend() *fakeBackend { return &fakeBackend{index: vector.NewIndex(2)} }
func request(h http.Handler, method, path, body string, auth bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if auth {
		r.Header.Set("Authorization", "Bearer "+testToken)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestHandlerAuthAndMethods(t *testing.T) {
	for _, path := range []string{"/healthz", "/v1/records/a", "/v1/search", "/v1/checkpoint"} {
		t.Run("auth_"+path, func(t *testing.T) {
			f := backend()
			h := newHandler(f, f, 2, testToken, "ns")
			w := request(h, "POST", path, `{}`, false)
			if w.Code != 401 || f.calls != 0 {
				t.Fatal(w.Code, f.calls)
			}
		})
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"POST", "/healthz", 405}, {"POST", "/v1/records/a", 405}, {"GET", "/v1/search", 405}, {"GET", "/v1/checkpoint", 405}, {"GET", "/unknown", 404}, {"GET", "/v1/records/", 400}, {"GET", "/v1/records/a/b", 400}} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			f := backend()
			w := request(newHandler(f, f, 2, testToken, "ns"), tc.method, tc.path, `{}`, true)
			if w.Code != tc.status || f.calls != 0 {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}
func TestHandlerDecodeAndMutationValidation(t *testing.T) {
	for _, body := range []string{``, `null`, `{`, `{} {}`, `{"request_id":"r","vector":[1,2],"unknown":true}`, `{"request_id":"r","vector":[1]}`, `{"request_id":"r","vector":[1e16,0]}`, `{"vector":[1,2]}`, `{"request_id":123}`, `{"request_id":"` + strings.Repeat("x", 257) + `","vector":[1,2]}`, `{"request_id":"r","vector":[1,2],"payload":{"x":1}}`, `{"request_id":"r","vector":[1,2],"payload":{"x":"` + strings.Repeat("x", 1<<20) + `"}}`} {
		f := backend()
		w := request(newHandler(f, f, 2, testToken, "ns"), "PUT", "/v1/records/a", body, true)
		if w.Code != 400 || f.calls != 0 {
			t.Fatalf("body length %d: status %d calls %d", len(body), w.Code, f.calls)
		}
	}
	for _, body := range []string{`{}`, `{"request_id":"r","vector":[1,2]} trailing`} {
		f := backend()
		w := request(newHandler(f, f, 2, testToken, "ns"), "DELETE", "/v1/records/a", body, true)
		if w.Code != 400 || f.calls != 0 {
			t.Fatal(w.Code)
		}
	}
	f := backend()
	w := request(newHandler(f, f, 2, testToken, "ns"), "PUT", "/v1/records/"+strings.Repeat("x", 257), `{"request_id":"r","vector":[1,2]}`, true)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
func TestHandlerSuccess(t *testing.T) {
	f := backend()
	f.index.Apply([]vector.Record{{ID: "a", Revision: 2, Vector: []float64{0, 0}, Payload: map[string]string{"tag": "red"}}})
	h := newHandler(f, f, 2, testToken, "ns")
	for _, tc := range []struct{ method, path, body string }{{"GET", "/healthz", ""}, {"GET", "/v1/records/a", ""}, {"PUT", "/v1/records/a", `{"request_id":"r","vector":[1,2],"expected_revision":1}`}, {"DELETE", "/v1/records/a", `{"request_id":"d"}`}, {"POST", "/v1/search", `{"vector":[0,0],"k":1,"filter":{"tag":"red"},"min_visible":{"id":"a","revision":2}}`}, {"POST", "/v1/checkpoint", ""}} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			w := request(h, tc.method, tc.path, tc.body, true)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if w.Header().Get("Content-Type") != "application/json" {
				t.Fatal("content type")
			}
			var value map[string]any
			if e := json.Unmarshal(w.Body.Bytes(), &value); e != nil {
				t.Fatal(e)
			}
			if tc.path == "/v1/search" {
				if value["partial"] != false || len(value["hits"].([]any)) != 1 {
					t.Fatal(value)
				}
			}
			if tc.method == "DELETE" && !f.deleted {
				t.Fatal("delete flag missing")
			}
			if tc.method == "PUT" && (f.mutation.ExpectedRevision == nil || *f.mutation.ExpectedRevision != 1) {
				t.Fatal("precondition lost")
			}
		})
	}
}
func TestHandlerErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		setup                    func(*fakeBackend)
		status                   int
	}{
		{"missing", "GET", "/v1/records/a", "", func(f *fakeBackend) { f.getErr = vector.ErrNotFound }, 404},
		{"read_backend", "GET", "/v1/records/a", "", func(f *fakeBackend) { f.getErr = errors.New("offline") }, 503},
		{"conflict", "PUT", "/v1/records/a", `{"request_id":"r","vector":[1,2]}`, func(f *fakeBackend) { f.mutateErr = vector.ErrConflict }, 409},
		{"commit_unknown", "PUT", "/v1/records/a", `{"request_id":"r","vector":[1,2]}`, func(f *fakeBackend) { f.mutateErr = errors.New("retry same request_id") }, 503},
		{"sync", "POST", "/v1/search", `{"vector":[0,0],"k":1,"min_visible":{"id":"a","revision":1}}`, func(f *fakeBackend) { f.syncErr = errors.New("scan failure") }, 503},
		{"future_token", "POST", "/v1/search", `{"vector":[0,0],"k":1,"min_visible":{"id":"a","revision":1}}`, func(*fakeBackend) {}, 503},
		{"invalid_search", "POST", "/v1/search", `{"vector":[0,0],"k":0}`, func(*fakeBackend) {}, 400},
		{"bad_json", "POST", "/v1/search", `{`, func(*fakeBackend) {}, 400},
		{"checkpoint", "POST", "/v1/checkpoint", `{}`, func(f *fakeBackend) { f.checkpointErr = errors.New("upload failed") }, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := backend()
			tc.setup(f)
			w := request(newHandler(f, f, 2, testToken, "ns"), tc.method, tc.path, tc.body, true)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			var errBody map[string]string
			if e := json.Unmarshal(w.Body.Bytes(), &errBody); e != nil || errBody["error"] == "" {
				t.Fatal("error not explained")
			}
		})
	}
}
func TestEnv(t *testing.T) {
	t.Setenv("VECTOR_TEST_ENV", "")
	if env("VECTOR_TEST_ENV", "default") != "default" {
		t.Fatal("default")
	}
	t.Setenv("VECTOR_TEST_ENV", "configured")
	if env("VECTOR_TEST_ENV", "default") != "configured" {
		t.Fatal("override")
	}
}

type rebuildingBackend struct {
	*fakeBackend
	rebuildErr error
}

func (f *rebuildingBackend) Rebuild(context.Context) error { f.calls++; return f.rebuildErr }
func TestHandlerRebuild(t *testing.T) {
	for _, tc := range []struct {
		err, checkpointErr error
		status, calls      int
	}{{nil, nil, 200, 2}, {vector.ErrConflict, nil, 409, 1}, {errors.New("failed"), nil, 503, 1}, {nil, errors.New("publish failed"), 503, 2}} {
		f := &rebuildingBackend{backend(), tc.err}
		f.checkpointErr = tc.checkpointErr
		w := request(newHandler(f, f, 2, testToken, "ns"), "POST", "/v1/rebuild", "", true)
		if w.Code != tc.status || f.calls != tc.calls {
			t.Fatal(w.Code, f.calls, w.Body.String())
		}
	}
	f := backend()
	h := newHandler(f, f, 2, testToken, "ns")
	for _, tc := range []struct {
		method string
		auth   bool
		status int
	}{{"POST", false, 401}, {"GET", true, 405}, {"POST", true, 501}} {
		if w := request(h, tc.method, "/v1/rebuild", "", tc.auth); w.Code != tc.status {
			t.Fatal(w.Code)
		}
	}
}

type pruningBackend struct {
	*fakeBackend
	err error
}

func (f *pruningBackend) Prune(context.Context) (uint64, error) { f.calls++; return 123, f.err }
func TestHandlerPrune(t *testing.T) {
	for _, tc := range []struct {
		method string
		auth   bool
		err    error
		status int
	}{{"POST", true, nil, 200}, {"GET", true, nil, 405}, {"POST", false, nil, 401}, {"POST", true, errors.New("failed"), 503}} {
		f := &pruningBackend{backend(), tc.err}
		w := request(newHandler(f, f, 2, testToken, "ns"), tc.method, "/v1/log/prune", "", tc.auth)
		if w.Code != tc.status {
			t.Fatal(w.Code)
		}
	}
	f := backend()
	if w := request(newHandler(f, f, 2, testToken, "ns"), "POST", "/v1/log/prune", "", true); w.Code != 501 {
		t.Fatal(w.Code)
	}
}

type layoutBackend struct{ *fakeBackend }

func (f layoutBackend) Layout() vector.PartitionLayout {
	return vector.PartitionLayout{Version: 1, Epoch: 1, DataPartitions: 4}
}
func (f layoutBackend) PruneState(context.Context) (any, error) {
	return map[string]any{"partitions": map[string]uint64{"index-0000": 12}}, nil
}
func (f layoutBackend) SearchContext(ctx context.Context, q vector.Search) ([]vector.Hit, error) {
	return nil, vector.ErrPartitionUnavailable
}
func TestHandlerPartitionAPI(t *testing.T) {
	f := layoutBackend{backend()}
	h := newHandler(f, f, 2, testToken, "ns")
	for _, tc := range []struct {
		method, path string
		auth         bool
		code         int
	}{{"GET", "/v1/layout", true, 200}, {"POST", "/v1/layout", true, 405}, {"GET", "/v1/layout", false, 401}, {"POST", "/v1/log/prune", true, 200}, {"POST", "/v1/log/prune", false, 401}} {
		w := request(h, tc.method, tc.path, "", tc.auth)
		if w.Code != tc.code {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := request(h, "POST", "/v1/search", `{"vector":[0,0],"k":1}`, true)
	if w.Code != 503 {
		t.Fatal("partial failure not mapped", w.Code)
	}
}

type listBackend struct {
	*fakeBackend
	opts vector.ListOptions
	err  error
}

func (f *listBackend) ListRecords(_ context.Context, o vector.ListOptions) (vector.RecordPage, error) {
	f.opts = o
	return vector.RecordPage{Records: []vector.Record{{ID: "a", Vector: []float64{1, 2}}}}, f.err
}
func TestListEndpoint(t *testing.T) {
	f := &listBackend{fakeBackend: backend()}
	h := newHandler(f, f, 2, testToken, "ns")
	if w := request(h, "GET", "/v1/records?limit=2&cursor=abc&include_deleted=true", "", true); w.Code != 200 || f.opts.Limit != 2 || !f.opts.IncludeDeleted || f.opts.Cursor != "abc" {
		t.Fatalf("%d %v", w.Code, f.opts)
	}
	for _, q := range []string{"limit=0", "limit=1001", "limit=a", "include_deleted=bad", "limit=2&limit=3", "unknown=1"} {
		if w := request(h, "GET", "/v1/records?"+q, "", true); w.Code != 400 {
			t.Fatalf("%s %d", q, w.Code)
		}
	}
	if w := request(h, "GET", "/v1/records", "", false); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := request(h, "POST", "/v1/records", "", true); w.Code != 405 {
		t.Fatal(w.Code)
	}
	f.err = vector.ErrInvalidList
	if w := request(h, "GET", "/v1/records", "", true); w.Code != 400 {
		t.Fatal(w.Code)
	}
	f.err = errors.New("storage failed")
	if w := request(h, "GET", "/v1/records", "", true); w.Code != 503 {
		t.Fatal(w.Code)
	}
}
