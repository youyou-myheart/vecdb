package vector

import (
	"net/http"
)

// Gateway uses the stable data router and ownership-aware index RPC proxies.
// It does not participate in scheduling and need not hold any HNSW graph.
type Gateway struct{ *PartitionedService }

func (c *ClusterService) HasRole(role string) bool { return hasRole(c.node, role) }
func (c *ClusterService) WrapHandler(public http.Handler) http.Handler {
	internal := c.InternalHandler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/partition" {
			if c.worker == nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			internal.ServeHTTP(w, r)
			return
		}
		switch r.URL.Path {
		case "/v1/cluster/tasks", "/v1/cluster/task", "/v1/cluster/balance", "/v1/cluster/explain", "/v1/drain", "/healthz", "/v1/cluster", "/v1/layout":
			public.ServeHTTP(w, r)
			return
		}
		if c.gateway == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"gateway role required"}`))
			return
		}
		public.ServeHTTP(w, r)
	})
}
