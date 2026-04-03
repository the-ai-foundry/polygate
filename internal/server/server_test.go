package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/polygate/polygate/internal/batcher"
	"github.com/polygate/polygate/internal/query"
	"github.com/polygate/polygate/internal/schema"
	"github.com/polygate/polygate/internal/sink"
)

func newTestServer() *Server {
	enabled := []string{"postgres", "clickhouse"}
	reg := schema.NewRegistry(enabled)
	sinkSet := sink.NewSinkSet(map[string]sink.Sink{}, reg)
	bat := batcher.New(sinkSet, reg, 5000, 1000, 1)
	router := query.NewRouter()

	return &Server{
		Batcher:  bat,
		Registry: reg,
		SinkSet:  sinkSet,
		Router:   router,
	}
}

func TestHealthz(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestCreateAndGetSchema(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	// Create schema.
	body := `{
		"table": "events",
		"columns": {"id": "int64", "name": "string"},
		"sinks": ["postgres"],
		"primary_key": ["id"]
	}`
	req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("create schema: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// Get schema.
	req = httptest.NewRequest(http.MethodGet, "/schema/events", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("get schema: expected 200, got %d", w.Code)
	}

	var ts schema.TableSchema
	json.NewDecoder(w.Body).Decode(&ts)
	if ts.Table != "events" {
		t.Errorf("expected table=events, got %s", ts.Table)
	}
}

func TestCreateSchemaValidation(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	// Missing table name.
	body := `{"columns": {"id": "int64"}}`
	req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing table, got %d", w.Code)
	}
}

func TestGetSchemaNotFound(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	req := httptest.NewRequest(http.MethodGet, "/schema/nonexistent", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestListSchemas(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	// Register two schemas.
	for _, table := range []string{"a", "b"} {
		body, _ := json.Marshal(map[string]any{
			"table":   table,
			"columns": map[string]string{"id": "int64"},
			"sinks":   []string{"postgres"},
		})
		req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/schema", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var schemas []schema.TableSchema
	json.NewDecoder(w.Body).Decode(&schemas)
	if len(schemas) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(schemas))
	}
}

func TestGetProto(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	// Register schema.
	body := `{"table": "events", "columns": {"id": "int64", "name": "string"}, "sinks": ["postgres"]}`
	req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Get proto.
	req = httptest.NewRequest(http.MethodGet, "/schema/events/proto", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/plain" {
		t.Errorf("expected text/plain, got %s", ct)
	}
	body2 := w.Body.String()
	if !bytes.Contains([]byte(body2), []byte("message Events")) {
		t.Error("proto definition missing Events message")
	}
}

func TestIngestWithoutSchema(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	body := `[{"id": 1}]`
	req := httptest.NewRequest(http.MethodPost, "/ingest?table=unknown", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestIngestMissingTable(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	body := `[{"id": 1}]`
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestIngestSuccess(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	// Register schema first.
	schemaBody := `{"table": "t1", "columns": {"id": "int64"}, "sinks": ["postgres"]}`
	req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewBufferString(schemaBody))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Ingest.
	body := `[{"id": 1}, {"id": 2}]`
	req = httptest.NewRequest(http.MethodPost, "/ingest?table=t1", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["records"] != float64(2) {
		t.Errorf("expected records=2, got %v", resp["records"])
	}
}

func TestIngestEmptyArray(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	schemaBody := `{"table": "t1", "columns": {"id": "int64"}, "sinks": ["postgres"]}`
	req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewBufferString(schemaBody))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	body := `[]`
	req = httptest.NewRequest(http.MethodPost, "/ingest?table=t1", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty array, got %d", w.Code)
	}
}

func TestQueryMissingEngine(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	// No engine registered, should fail.
	req := httptest.NewRequest(http.MethodGet, "/query?engine=postgres&q=SELECT+1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown engine, got %d", w.Code)
	}
}

func TestQueryMissingQ(t *testing.T) {
	srv := newTestServer()
	mux := srv.NewMux()

	req := httptest.NewRequest(http.MethodGet, "/query?engine=postgres", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing q, got %d", w.Code)
	}
}
