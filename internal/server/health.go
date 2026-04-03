package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	results := s.SinkSet.PingAll(r.Context())

	allHealthy := true
	status := make(map[string]string, len(results))
	for name, err := range results {
		if err != nil {
			allHealthy = false
			status[name] = err.Error()
		} else {
			status[name] = "ok"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if !allHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(map[string]any{
		"ok":    allHealthy,
		"sinks": status,
	})
}
