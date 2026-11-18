package main

import (
	"context"
	"errors"
	"net/http"

	"example.com/vector-storage/internal/vector"
)

func registerFollowerSearch(mux *http.ServeMux, service searchAPI, dimension int) {
	mux.HandleFunc("/v1/follower-search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var q vector.Search
		if err := decode(w, r, &q); err != nil {
			reply(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if err := vector.ValidateFollowerSearch(q, dimension); err != nil {
			reply(w, 400, map[string]string{"error": err.Error()})
			return
		}
		backend, ok := service.(interface {
			FollowerSearch(context.Context, vector.Search) (vector.FollowerSearchResult, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": vector.ErrFollowerReadUnsupported.Error()})
			return
		}
		result, err := backend.FollowerSearch(r.Context(), q)
		if err != nil {
			code := http.StatusServiceUnavailable
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				code = http.StatusGatewayTimeout
			case errors.Is(err, vector.ErrFollowerQueueFull):
				code = http.StatusTooManyRequests
			case errors.Is(err, vector.ErrFollowerReadUnsupported):
				code = http.StatusNotImplemented
			}
			reply(w, code, map[string]string{"error": err.Error()})
			return
		}
		reply(w, 200, result)
	})
}
