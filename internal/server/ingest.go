package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/polygate/polygate/internal/batcher"
	"github.com/polygate/polygate/internal/model"
)

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	if table == "" {
		http.Error(w, "query parameter 'table' is required", http.StatusBadRequest)
		return
	}

	// Check schema exists.
	ts, ok := s.Registry.Get(table)
	if !ok {
		http.Error(w, "no schema registered for table: "+table+". Register via POST /schema first.", http.StatusBadRequest)
		return
	}

	var records []model.Record
	var err error

	contentType := r.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(contentType, "application/x-protobuf"),
		strings.HasPrefix(contentType, "application/protobuf"):
		// Protobuf binary ingestion.
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			http.Error(w, "failed to read body: "+readErr.Error(), http.StatusBadRequest)
			return
		}
		records, err = ts.DecodeProtobuf(body)
		if err != nil {
			http.Error(w, "protobuf decode error: "+err.Error(), http.StatusBadRequest)
			return
		}

	default:
		// Default: JSON.
		if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
			http.Error(w, "invalid JSON array: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if len(records) == 0 {
		http.Error(w, "empty records", http.StatusBadRequest)
		return
	}

	submitted := s.Batcher.Submit(batcher.IngestRequest{
		Table:   table,
		Records: records,
	})
	if !submitted {
		http.Error(w, "ingestion buffer full, try again later", http.StatusTooManyRequests)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"records": len(records),
		"format":  formatName(contentType),
	})
}

func formatName(ct string) string {
	if strings.Contains(ct, "protobuf") {
		return "protobuf"
	}
	return "json"
}
