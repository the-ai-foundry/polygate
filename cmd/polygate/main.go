package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/polygate/polygate/internal/batcher"
	"github.com/polygate/polygate/internal/config"
	polymcp "github.com/polygate/polygate/internal/mcp"
	"github.com/polygate/polygate/internal/query"
	"github.com/polygate/polygate/internal/schema"
	"github.com/polygate/polygate/internal/server"
	"github.com/polygate/polygate/internal/sink"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	slog.Info("config loaded", "sinks", len(cfg.Sinks))

	// Collect enabled sink names.
	var enabledSinkNames []string
	for name, sc := range cfg.Sinks {
		if sc.Enabled {
			enabledSinkNames = append(enabledSinkNames, name)
		}
	}

	registry := schema.NewRegistry(enabledSinkNames)

	// Initialize sinks.
	sinks := make(map[string]sink.Sink)
	for name, sc := range cfg.Sinks {
		if !sc.Enabled {
			continue
		}
		s, err := initSink(name, sc)
		if err != nil {
			slog.Error("failed to initialize sink", "sink", name, "error", err)
			os.Exit(1)
		}
		sinks[name] = s
		slog.Info("sink initialized", "sink", name)
	}

	sinkSet := sink.NewSinkSet(sinks, registry)

	// Setup context with signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start batcher.
	bat := batcher.New(sinkSet, registry, cfg.Batcher.MaxSize, cfg.Batcher.BufferSize, cfg.Batcher.FlushPeriod)
	go bat.Run(ctx)

	// Setup query router.
	router := query.NewRouter()

	// Register Trino engine if enabled.
	if cfg.Trino.Enabled && cfg.Trino.URL != "" {
		router.Register(query.NewTrinoEngine(cfg.Trino.URL))
		slog.Info("trino engine registered", "url", cfg.Trino.URL)
	}

	// Setup HTTP server.
	srv := &server.Server{
		Batcher:  bat,
		Registry: registry,
		SinkSet:  sinkSet,
		Router:   router,
	}

	// Start metrics server.
	if cfg.Metrics.Enabled {
		go func() {
			metricsMux := http.NewServeMux()
			metricsMux.Handle("GET /metrics", promhttp.Handler())
			slog.Info("metrics server starting", "addr", cfg.Metrics.Bind)
			if err := http.ListenAndServe(cfg.Metrics.Bind, metricsMux); err != nil {
				slog.Error("metrics server error", "error", err)
			}
		}()
	}

	// Start MCP server if enabled.
	if cfg.MCP.Enabled {
		mcpServer := polymcp.New(registry, sinkSet, router, bat)
		switch cfg.MCP.Transport {
		case "http":
			port := cfg.MCP.HTTPPort
			if port == 0 {
				port = 8090
			}
			go func() {
				if err := mcpServer.RunHTTP(ctx, fmt.Sprintf("0.0.0.0:%d", port)); err != nil {
					slog.Error("MCP HTTP server error", "error", err)
				}
			}()
		default: // stdio
			go func() {
				if err := mcpServer.RunStdio(); err != nil {
					slog.Error("MCP stdio server error", "error", err)
				}
			}()
		}
	}

	// Start HTTP server (blocks until shutdown).
	slog.Info("polygate started", "bind", cfg.Server.Bind)
	go func() {
		if err := srv.ListenAndServe(ctx, cfg.Server.Bind); err != nil {
			slog.Error("http server error", "error", err)
			cancel()
		}
	}()

	// Block until shutdown.
	<-ctx.Done()

	slog.Info("shutting down")
	sinkSet.CloseAll()
	slog.Info("shutdown complete")
}

func initSink(name string, sc config.SinkConfig) (sink.Sink, error) {
	ctx := context.Background()
	switch name {
	case "postgres":
		return sink.NewPostgresSink(ctx, sc.DSN, sc.MaxConns, sc.BulkSize, sc.MaxRetries, sc.RetryBaseMS)
	case "clickhouse":
		return sink.NewClickHouseSink(sc.DSN, sc.TimeoutSecs, sc.BulkSize, sc.MaxRetries, sc.RetryBaseMS), nil
	case "elasticsearch":
		return sink.NewElasticsearchSink(sc.DSN, sc.TimeoutSecs, sc.BulkSize, sc.MaxRetries, sc.RetryBaseMS), nil
	case "mongodb":
		return sink.NewMongoDBSink(ctx, sc.DSN, sc.BulkSize, sc.MaxRetries, sc.RetryBaseMS)
	case "questdb":
		return sink.NewQuestDBSink(sc.DSN, sc.TimeoutSecs, sc.BulkSize, sc.MaxRetries, sc.RetryBaseMS), nil
	default:
		slog.Warn("unknown sink type, skipping", "sink", name)
		return nil, nil
	}
}
