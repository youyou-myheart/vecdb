package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"example.com/vector-storage/internal/vector"
	tikverr "github.com/tikv/client-go/v2/error"
)

type migrationAPI interface {
	Tasks(context.Context, string, int) (vector.TaskPage, error)
	Task(context.Context, string) (vector.MigrationTask, error)
	Preview(context.Context) (vector.BalancePreview, error)
}

func registerMigrationHandlers(mux *http.ServeMux, service searchAPI) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		api, ok := service.(migrationAPI)
		if !ok {
			reply(w, 501, map[string]string{"error": "migration control unavailable"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		var result any
		var err error
		switch r.URL.Path {
		case "/v1/cluster/tasks":
			limit := 50
			if raw := r.URL.Query().Get("limit"); raw != "" {
				limit, err = strconv.Atoi(raw)
			}
			if err != nil || limit < 1 || limit > 200 {
				reply(w, 400, map[string]string{"error": "limit must be 1..200"})
				return
			}
			result, err = api.Tasks(ctx, r.URL.Query().Get("after"), limit)
		case "/v1/cluster/task":
			id := r.URL.Query().Get("id")
			if id == "" {
				reply(w, 400, map[string]string{"error": "id required"})
				return
			}
			result, err = api.Task(ctx, id)
		case "/v1/cluster/balance", "/v1/cluster/explain":
			var preview vector.BalancePreview
			preview, err = api.Preview(ctx)
			result = preview
			if err == nil && r.URL.Path == "/v1/cluster/explain" {
				id := r.URL.Query().Get("partition")
				found := false
				for _, p := range preview.Partitions {
					if string(p.Partition) == id {
						preview.Partitions = []vector.PlacementExplanation{p}
						result = preview
						found = true
						break
					}
				}
				if !found {
					reply(w, 404, map[string]string{"error": "partition not found"})
					return
				}
			}
		}
		if err != nil {
			code := 503
			if errors.Is(err, vector.ErrTaskQuery) {
				code = 400
			}
			if tikverr.IsErrNotFound(err) {
				code = 404
			}
			reply(w, code, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, result)
	}
	for _, path := range []string{"/v1/cluster/tasks", "/v1/cluster/task", "/v1/cluster/balance", "/v1/cluster/explain"} {
		mux.HandleFunc(path, handler)
	}
}
