package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/polygate/polygate/internal/metrics"
)

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	engineName := r.URL.Query().Get("engine")
	if engineName == "" {
		engineName = "postgres"
	}

	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	engine, err := s.Router.Route(engineName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	result, err := engine.Execute(r.Context(), q)
	dur := time.Since(start)

	metrics.QueryDuration.WithLabelValues(engineName).Observe(dur.Seconds())

	if err != nil {
		http.Error(w, "query error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":            true,
		"result":        result,
		"query_time_ms": dur.Milliseconds(),
	})
}
