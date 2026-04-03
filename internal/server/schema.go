package server

import (
	"encoding/json"
	"net/http"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/schema"
)

func (s *Server) handleCreateSchema(w http.ResponseWriter, r *http.Request) {
	var ts schema.TableSchema
	if err := json.NewDecoder(r.Body).Decode(&ts); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.Registry.Register(&ts); err != nil {
		http.Error(w, "schema validation error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Create tables/indexes in target sinks.
	if err := s.SinkSet.CreateTable(r.Context(), &ts); err != nil {
		http.Error(w, "failed to create table: "+err.Error(), http.StatusInternalServerError)
		return
	}

	metrics.SchemaRegistrations.Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"ok":    true,
		"table": ts.Table,
		"sinks": ts.Sinks,
	})
}

func (s *Server) handleListSchemas(w http.ResponseWriter, r *http.Request) {
	schemas := s.Registry.List()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schemas)
}

func (s *Server) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	ts, ok := s.Registry.Get(table)
	if !ok {
		http.Error(w, "schema not found for table: "+table, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ts)
}

func (s *Server) handleGetProto(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	ts, ok := s.Registry.Get(table)
	if !ok {
		http.Error(w, "schema not found for table: "+table, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Disposition", "attachment; filename="+table+".proto")
	w.Write([]byte(ts.ProtoDefinition()))
}
