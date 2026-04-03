package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/polygate/polygate/internal/batcher"
	"github.com/polygate/polygate/internal/query"
	"github.com/polygate/polygate/internal/schema"
	"github.com/polygate/polygate/internal/sink"
)

// Server wraps an MCP server with Polygate dependencies.
type Server struct {
	mcp      *mcpserver.MCPServer
	registry *schema.Registry
	sinkSet  *sink.SinkSet
	router   *query.Router
	bat      *batcher.Batcher
}

// New creates a new MCP server with all resources and tools registered.
func New(registry *schema.Registry, sinkSet *sink.SinkSet, router *query.Router, bat *batcher.Batcher) *Server {
	s := &Server{
		mcp: mcpserver.NewMCPServer(
			"polygate",
			"1.0.0",
			mcpserver.WithResourceCapabilities(true, true),
			mcpserver.WithToolCapabilities(true),
		),
		registry: registry,
		sinkSet:  sinkSet,
		router:   router,
		bat:      bat,
	}

	s.registerResources()
	s.registerTools()

	return s
}

// RunStdio starts the MCP server using stdio transport (for local integrations like Claude Code).
func (s *Server) RunStdio() error {
	slog.Info("MCP server starting (stdio transport)")
	return mcpserver.ServeStdio(s.mcp)
}

// RunHTTP starts the MCP server using HTTP transport (for remote integrations).
func (s *Server) RunHTTP(ctx context.Context, addr string) error {
	slog.Info("MCP server starting (HTTP transport)", "addr", addr)
	httpServer := mcpserver.NewStreamableHTTPServer(s.mcp)

	srv := &http.Server{
		Addr:    addr,
		Handler: httpServer,
	}

	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("MCP HTTP server error: %w", err)
	}
	return nil
}
