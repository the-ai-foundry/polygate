package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/polygate/polygate/internal/batcher"
	"github.com/polygate/polygate/internal/query"
	"github.com/polygate/polygate/internal/schema"
	"github.com/polygate/polygate/internal/sink"
)

// Server holds dependencies for all HTTP handlers.
type Server struct {
	Batcher  *batcher.Batcher
	Registry *schema.Registry
	SinkSet  *sink.SinkSet
	Router   *query.Router
}

// NewMux creates the HTTP handler with all routes registered.
func (s *Server) NewMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /schema", s.handleCreateSchema)
	mux.HandleFunc("GET /schema", s.handleListSchemas)
	mux.HandleFunc("GET /schema/{table}/proto", s.handleGetProto)
	mux.HandleFunc("GET /schema/{table}", s.handleGetSchema)
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("GET /query", s.handleQuery)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	return mux
}

// ListenAndServe starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	mux := s.NewMux()
	handler := withMiddleware(mux)

	srv := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http server shutdown error", "error", err)
		}
	}()

	slog.Info("http server starting", "addr", addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
