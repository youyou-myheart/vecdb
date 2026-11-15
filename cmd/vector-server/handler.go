package main

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/vector-storage/internal/vector"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type recordAPI interface {
	Get(context.Context, string) (vector.Record, error)
	Mutate(context.Context, string, vector.Mutation, bool) (vector.Record, error)
}
type searchAPI interface {
	Sync(context.Context) error
	Search(vector.Search) ([]vector.Hit, error)
	Checkpoint(context.Context) (vector.Manifest, error)
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("expected a single JSON object")
	}
	return nil
}
func newHandler(repo recordAPI, service searchAPI, dim int, token, namespace string) http.Handler {
	mux := http.NewServeMux()
	registerMigrationHandlers(mux, service)
	registerSubscriptionHandler(mux, service)
	mux.HandleFunc("/v1/drain", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		d, ok := service.(interface{ Drain(context.Context) error })
		if !ok {
			reply(w, 501, map[string]string{"error": "drain unavailable"})
			return
		}
		if err := d.Drain(r.Context()); err != nil {
			reply(w, 409, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, map[string]bool{"draining": true})
	})
	mux.HandleFunc("/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		provider, ok := service.(interface {
			Status(context.Context) (vector.ClusterStatus, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": "cluster unavailable"})
			return
		}
		status, err := provider.Status(r.Context())
		if err != nil {
			reply(w, 503, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, status)
	})
	mux.HandleFunc("/v1/layout", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		provider, ok := service.(interface{ Layout() vector.PartitionLayout })
		if !ok {
			reply(w, 501, map[string]string{"error": "layout unavailable"})
			return
		}
		reply(w, 200, provider.Layout())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		reply(w, 200, map[string]string{"status": "ready", "index": "hnsw-exact", "namespace": namespace})
	})
	mux.HandleFunc("/v1/records", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		query := r.URL.Query()
		for key, values := range query {
			if (key != "limit" && key != "cursor" && key != "include_deleted") || len(values) != 1 {
				reply(w, 400, map[string]string{"error": "invalid list parameters"})
				return
			}
		}
		limit := 100
		var err error
		if query.Has("limit") {
			limit, err = strconv.Atoi(query.Get("limit"))
		}
		deleted := false
		if err == nil && query.Has("include_deleted") {
			deleted, err = strconv.ParseBool(query.Get("include_deleted"))
		}
		if err != nil || limit < 1 || limit > 1000 {
			reply(w, 400, map[string]string{"error": "limit must be 1..1000; include_deleted must be boolean"})
			return
		}
		lister, ok := repo.(interface {
			ListRecords(context.Context, vector.ListOptions) (vector.RecordPage, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": "list unavailable"})
			return
		}
		page, err := lister.ListRecords(r.Context(), vector.ListOptions{Limit: limit, Cursor: query.Get("cursor"), IncludeDeleted: deleted})
		if err != nil {
			status := 503
			if errors.Is(err, vector.ErrInvalidList) {
				status = 400
			}
			reply(w, status, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, page)
	})
	mux.HandleFunc("/v1/records/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/v1/records/")
		if id == "" || strings.Contains(id, "/") {
			reply(w, 400, map[string]string{"error": "invalid id"})
			return
		}
		if r.Method == "GET" {
			rec, e := repo.Get(r.Context(), id)
			if e != nil {
				status := 503
				if errors.Is(e, vector.ErrNotFound) {
					status = 404
				}
				reply(w, status, map[string]string{"error": e.Error()})
				return
			}
			reply(w, 200, rec)
			return
		}
		if r.Method != "PUT" && r.Method != "DELETE" {
			w.WriteHeader(405)
			return
		}
		var m vector.Mutation
		if e := decode(w, r, &m); e != nil {
			reply(w, 400, map[string]string{"error": e.Error()})
			return
		}
		if m.RequestID == "" || len(m.RequestID) > 256 || len(id) > 256 {
			reply(w, 400, map[string]string{"error": "invalid request_id or id"})
			return
		}
		if r.Method == "PUT" {
			if e := vector.ValidateVector(m.Vector, dim); e != nil {
				reply(w, 400, map[string]string{"error": e.Error()})
				return
			}
		}
		rec, e := repo.Mutate(r.Context(), id, m, r.Method == "DELETE")
		if e != nil {
			status := 503
			if errors.Is(e, vector.ErrConflict) {
				status = 409
			}
			reply(w, status, map[string]string{"error": e.Error()})
			return
		}
		reply(w, 200, map[string]any{"durable": true, "token": vector.Token{ID: rec.ID, Revision: rec.Revision}})
	})
	mux.HandleFunc("/v1/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		var q vector.Search
		if e := decode(w, r, &q); e != nil {
			reply(w, 400, map[string]string{"error": e.Error()})
			return
		}
		if q.MinVisible != nil {
			if e := service.Sync(r.Context()); e != nil {
				reply(w, 503, map[string]string{"error": e.Error()})
				return
			}
		}
		var hits []vector.Hit
		var e error
		if searcher, ok := service.(interface {
			SearchContext(context.Context, vector.Search) ([]vector.Hit, error)
		}); ok {
			hits, e = searcher.SearchContext(r.Context(), q)
		} else {
			hits, e = service.Search(q)
		}
		if e != nil {
			status := 400
			if errors.Is(e, vector.ErrNotVisible) || errors.Is(e, vector.ErrPartitionUnavailable) {
				status = 503
			}
			reply(w, status, map[string]string{"error": e.Error()})
			return
		}
		reply(w, 200, map[string]any{"hits": hits, "partial": false})
	})
	mux.HandleFunc("/v1/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		m, e := service.Checkpoint(r.Context())
		if e != nil {
			reply(w, 503, map[string]string{"error": e.Error()})
			return
		}
		reply(w, 200, m)
	})
	mux.HandleFunc("/v1/log/prune", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		if pruner, ok := service.(interface {
			PruneState(context.Context) (any, error)
		}); ok {
			state, err := pruner.PruneState(r.Context())
			if err != nil {
				reply(w, 503, map[string]string{"error": err.Error()})
				return
			}
			reply(w, 200, state)
			return
		}
		pruner, ok := service.(interface {
			Prune(context.Context) (uint64, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": "prune unavailable"})
			return
		}
		floor, err := pruner.Prune(r.Context())
		if err != nil {
			reply(w, 503, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, map[string]uint64{"retained_after": floor})
	})
	mux.HandleFunc("/v1/rebuild", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		rebuilder, ok := service.(interface{ Rebuild(context.Context) error })
		if !ok {
			reply(w, 501, map[string]string{"error": "rebuild unavailable"})
			return
		}
		if err := rebuilder.Rebuild(r.Context()); err != nil {
			status := 503
			if errors.Is(err, vector.ErrConflict) {
				status = 409
			}
			reply(w, status, map[string]string{"error": err.Error()})
			return
		}
		m, err := service.Checkpoint(r.Context())
		if err != nil {
			reply(w, 503, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, m)
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			reply(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		c, done := context.WithTimeout(r.Context(), 30*time.Second)
		defer done()
		mux.ServeHTTP(w, r.WithContext(c))
	})
	return handler
}
