package mcp

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/polygate/polygate/internal/schema"
)

const apiDocumentation = `# Polygate API Reference

## Schema Management

### POST /schema — Register a table schema
Creates the table/index in all target sinks.

Request body:
{
  "table": "events",
  "columns": {"id": "int64", "name": "string", "ts": "timestamp"},
  "sinks": ["postgres", "clickhouse"],
  "primary_key": ["id"],
  "indexes": [{"columns": ["name"]}],
  "partition_by": "ts",
  "designated_timestamp": "ts",
  "overrides": {"questdb": {"name": "symbol"}, "clickhouse": {"name": "LowCardinality(String)"}}
}

Response (201): {"ok": true, "table": "events", "sinks": ["postgres", "clickhouse"]}

### GET /schema — List all registered schemas
Response (200): [{"table": "events", "columns": {...}, ...}]

### GET /schema/{table} — Get specific schema
Response (200): {"table": "events", "columns": {...}, ...}
Response (404): schema not found

### GET /schema/{table}/proto — Download .proto file
Response (200): text/plain proto3 definition

## Data Ingestion

### POST /ingest?table={name} — Ingest records
Content-Type: application/json (JSON array) or application/x-protobuf (binary)

JSON example:
curl -X POST "localhost:8080/ingest?table=events" \
  -d '[{"id": 1, "name": "click", "ts": "2026-04-03T12:00:00Z"}]'

Response (202): {"ok": true, "records": 1, "format": "json"}
Error (400): table not registered
Error (429): buffer full

## Query

### GET /query?engine={name}&q={sql} — Execute query
Engines: postgres, clickhouse, elasticsearch, mongodb, questdb, trino

SQL engines (postgres, clickhouse, questdb, trino):
  /query?engine=postgres&q=SELECT * FROM events

Elasticsearch (query_string or JSON DSL):
  /query?engine=elasticsearch&q=name:click

MongoDB (JSON filter):
  /query?engine=mongodb&q={"collection":"events","filter":{"name":"click"}}

Trino cross-database:
  /query?engine=trino&q=SELECT * FROM postgres.polygate.events
  Naming: {catalog}.{schema}.{table}
  Catalogs: postgres, clickhouse, elasticsearch, mongodb, questdb

Response (200): {"ok": true, "result": {"columns": [...], "rows": [...], "engine": "..."}, "query_time_ms": 12}

### Sorting: &sort=column:direction
  /query?engine=postgres&q=SELECT * FROM events&sort=ts:desc
  /query?engine=postgres&q=SELECT * FROM events&sort=ts:desc,name:asc

### Pagination: &limit= and &offset=
  /query?engine=postgres&q=SELECT * FROM events&limit=100&offset=200
  Response includes next_page when more results available:
  {"next_page": {"offset": 300, "limit": 100}}

### GET /query?...&stream=true — SSE streaming
Stream large result sets as Server-Sent Events (one page per event).

Parameters:
  stream=true — enable SSE
  page_size=1000 — rows per event (default 1000, max 10000)
  table=events — required for QuestDB (CSV schema lookup)

All 6 engines support streaming:
  PostgreSQL: server-side cursor (DECLARE/FETCH)
  ClickHouse: JSONEachRow chunked streaming
  Elasticsearch: PIT (Point in Time) + search_after pagination
  MongoDB: cursor with BatchSize
  QuestDB: /exp CSV streaming + schema type coercion
  Trino: nextUri page following

SSE format:
  data: {"columns":[...],"rows":[...],"page":1,"done":false,"engine":"postgres"}
  data: {"columns":[...],"rows":[...],"page":2,"done":true,"engine":"postgres"}
  event: done

## Health

### GET /healthz — Liveness check
Response (200): {"ok": true}

### GET /readyz — Readiness check (pings all sinks)
Response (200): {"ok": true, "sinks": {"postgres": "ok", ...}}
Response (503): one or more sinks unhealthy

## Metrics

### GET :9090/metrics — Prometheus metrics
Key metrics: polygate_ingest_records_total, polygate_sink_write_duration_seconds,
polygate_sink_errors_total, polygate_query_duration_seconds, polygate_batcher_queue_size
`

func textResource(uri, mimeType, text string) []mcp.ResourceContents {
	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      uri,
			MIMEType: mimeType,
			Text:     text,
		},
	}
}

func (s *Server) registerResources() {
	s.mcp.AddResource(mcp.NewResource(
		"polygate://api",
		"Polygate API reference — all endpoints, methods, parameters, and examples",
		mcp.WithResourceDescription("Complete API documentation for Polygate REST endpoints"),
		mcp.WithMIMEType("text/markdown"),
	), s.handleAPIResource)

	s.mcp.AddResource(mcp.NewResource(
		"polygate://schemas",
		"All registered table schemas",
		mcp.WithResourceDescription("List of all table schemas with columns, types, sinks, indexes"),
		mcp.WithMIMEType("application/json"),
	), s.handleSchemasResource)

	s.mcp.AddResource(mcp.NewResource(
		"polygate://sinks",
		"Enabled database sinks and their health status",
		mcp.WithResourceDescription("List of enabled sinks with connectivity status"),
		mcp.WithMIMEType("application/json"),
	), s.handleSinksResource)

	s.mcp.AddResource(mcp.NewResource(
		"polygate://types",
		"Portable type system — all types and their per-database native mappings",
		mcp.WithResourceDescription("Type mapping reference: polygate types to native DB types"),
		mcp.WithMIMEType("application/json"),
	), s.handleTypesResource)

	s.mcp.AddResourceTemplate(mcp.NewResourceTemplate(
		"polygate://schemas/{table}",
		"Schema for a specific table",
		mcp.WithTemplateDescription("Table schema with columns, types, sinks, indexes, and overrides"),
		mcp.WithTemplateMIMEType("application/json"),
	), s.handleSchemaByTableResource)

	s.mcp.AddResourceTemplate(mcp.NewResourceTemplate(
		"polygate://schemas/{table}/proto",
		"Auto-generated .proto file for a table",
		mcp.WithTemplateDescription("Protocol Buffers definition auto-generated from the table schema"),
		mcp.WithTemplateMIMEType("text/plain"),
	), s.handleSchemaProtoResource)
}

func (s *Server) handleAPIResource(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	return textResource(req.Params.URI, "text/markdown", apiDocumentation), nil
}

func (s *Server) handleSchemasResource(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	schemas := s.registry.List()
	data, _ := json.MarshalIndent(schemas, "", "  ")
	return textResource(req.Params.URI, "application/json", string(data)), nil
}

func (s *Server) handleSchemaByTableResource(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	table := extractURIParam(req.Params.URI, "polygate://schemas/", "")
	ts, ok := s.registry.Get(table)
	if !ok {
		return nil, errTableNotFound(table)
	}
	data, _ := json.MarshalIndent(ts, "", "  ")
	return textResource(req.Params.URI, "application/json", string(data)), nil
}

func (s *Server) handleSchemaProtoResource(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	table := extractURIParam(req.Params.URI, "polygate://schemas/", "/proto")
	ts, ok := s.registry.Get(table)
	if !ok {
		return nil, errTableNotFound(table)
	}
	return textResource(req.Params.URI, "text/plain", ts.ProtoDefinition()), nil
}

func (s *Server) handleSinksResource(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	results := s.sinkSet.PingAll(ctx)
	status := make(map[string]string, len(results))
	for name, err := range results {
		if err != nil {
			status[name] = err.Error()
		} else {
			status[name] = "ok"
		}
	}
	data, _ := json.MarshalIndent(status, "", "  ")
	return textResource(req.Params.URI, "application/json", string(data)), nil
}

func (s *Server) handleTypesResource(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	types := map[string]map[string]string{
		"string":    {"postgres": "TEXT", "clickhouse": "String", "questdb": "STRING", "elasticsearch": "keyword", "mongodb": "native"},
		"text":      {"postgres": "TEXT", "clickhouse": "String", "questdb": "STRING", "elasticsearch": "text", "mongodb": "native"},
		"int32":     {"postgres": "INTEGER", "clickhouse": "Int32", "questdb": "INT", "elasticsearch": "integer", "mongodb": "native"},
		"int64":     {"postgres": "BIGINT", "clickhouse": "Int64", "questdb": "LONG", "elasticsearch": "long", "mongodb": "native"},
		"float32":   {"postgres": "REAL", "clickhouse": "Float32", "questdb": "FLOAT", "elasticsearch": "float", "mongodb": "native"},
		"float64":   {"postgres": "DOUBLE PRECISION", "clickhouse": "Float64", "questdb": "DOUBLE", "elasticsearch": "double", "mongodb": "native"},
		"bool":      {"postgres": "BOOLEAN", "clickhouse": "Bool", "questdb": "BOOLEAN", "elasticsearch": "boolean", "mongodb": "native"},
		"timestamp": {"postgres": "TIMESTAMPTZ", "clickhouse": "DateTime64(3)", "questdb": "TIMESTAMP", "elasticsearch": "date", "mongodb": "native"},
		"json":      {"postgres": "JSONB", "clickhouse": "String", "questdb": "STRING", "elasticsearch": "object", "mongodb": "native"},
		"bytes":     {"postgres": "BYTEA", "clickhouse": "String", "questdb": "STRING", "elasticsearch": "binary", "mongodb": "native"},
	}
	data, _ := json.MarshalIndent(types, "", "  ")
	return textResource(req.Params.URI, "application/json", string(data)), nil
}

func extractURIParam(uri, prefix, suffix string) string {
	s := uri
	if len(prefix) > 0 && len(s) > len(prefix) {
		s = s[len(prefix):]
	}
	if len(suffix) > 0 && len(s) > len(suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

func errTableNotFound(table string) error {
	return &schema.TableNotFoundError{Table: table}
}
