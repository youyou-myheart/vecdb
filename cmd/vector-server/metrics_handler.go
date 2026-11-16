package main

import (
	"context"
	"net/http"
)

func registerMetricsHandler(mux *http.ServeMux, service searchAPI) {
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		provider, ok := service.(interface {
			Metrics(context.Context) (string, error)
		})
		if !ok {
			reply(w, 501, map[string]string{"error": "cluster metrics unavailable"})
			return
		}
		body, err := provider.Metrics(r.Context())
		if err != nil {
			reply(w, 503, map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(body))
	})
}
