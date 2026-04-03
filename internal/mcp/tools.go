package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/polygate/polygate/internal/batcher"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

func (s *Server) registerTools() {
	s.mcp.AddTool(mcp.NewTool("register_schema",
		mcp.WithDescription("Register a new table schema. Creates the table in all target database sinks."),
		mcp.WithString("schema_json", mcp.Required(), mcp.Description(
			`JSON object with: table (string, required), columns (object {name: type}, required), `+
				`sinks (string[], optional), primary_key (string[], optional), `+
				`indexes (array of {columns: string[]}, optional), partition_by (string, optional), `+
				`designated_timestamp (string, optional), overrides (object {sink: {col: nativeType}}, optional). `+
				`Types: string, text, int32, int64, float32, float64, bool, timestamp, json, bytes`,
		)),
	), s.handleRegisterSchema)

	s.mcp.AddTool(mcp.NewTool("ingest",
		mcp.WithDescription("Ingest JSON records into a registered table. Records are buffered and bulk-written to all configured sinks."),
		mcp.WithString("table", mcp.Required(), mcp.Description("Table name (must be registered via register_schema first)")),
		mcp.WithString("records_json", mcp.Required(), mcp.Description("JSON array of record objects")),
	), s.handleIngest)

	s.mcp.AddTool(mcp.NewTool("query",
		mcp.WithDescription("Execute a query against a specific database engine. Engines: postgres, clickhouse, elasticsearch, mongodb, questdb, trino."),
		mcp.WithString("engine", mcp.Required(), mcp.Description("Database engine to query")),
		mcp.WithString("q", mcp.Required(), mcp.Description("Query string (SQL for postgres/clickhouse/questdb/trino, JSON DSL for elasticsearch/mongodb)")),
	), s.handleQuery)

	s.mcp.AddTool(mcp.NewTool("health_check",
		mcp.WithDescription("Check connectivity to all enabled database sinks"),
	), s.handleHealthCheck)
}

func (s *Server) handleRegisterSchema(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	schemaJSON, err := req.RequireString("schema_json")
	if err != nil {
		return mcp.NewToolResultError("schema_json is required"), nil
	}

	var ts schema.TableSchema
	if err := json.Unmarshal([]byte(schemaJSON), &ts); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid schema JSON: %s", err)), nil
	}

	if err := s.registry.Register(&ts); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("schema validation error: %s", err)), nil
	}

	if err := s.sinkSet.CreateTable(ctx, &ts); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to create table: %s", err)), nil
	}

	result := map[string]any{"ok": true, "table": ts.Table, "sinks": ts.Sinks}
	data, _ := json.Marshal(result)
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) handleIngest(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	table, err := req.RequireString("table")
	if err != nil {
		return mcp.NewToolResultError("table is required"), nil
	}

	if _, ok := s.registry.Get(table); !ok {
		return mcp.NewToolResultError(fmt.Sprintf("no schema registered for table: %s", table)), nil
	}

	recordsJSON, err := req.RequireString("records_json")
	if err != nil {
		return mcp.NewToolResultError("records_json is required"), nil
	}

	var records []model.Record
	if err := json.Unmarshal([]byte(recordsJSON), &records); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid records JSON: %s", err)), nil
	}

	if len(records) == 0 {
		return mcp.NewToolResultError("records must be a non-empty array"), nil
	}

	submitted := s.bat.Submit(batcher.IngestRequest{
		Table:   table,
		Records: records,
	})
	if !submitted {
		return mcp.NewToolResultError("ingestion buffer full, try again later"), nil
	}

	result := map[string]any{"ok": true, "records": len(records)}
	data, _ := json.Marshal(result)
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) handleQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	engineName, err := req.RequireString("engine")
	if err != nil {
		return mcp.NewToolResultError("engine is required"), nil
	}

	q, err := req.RequireString("q")
	if err != nil {
		return mcp.NewToolResultError("q is required"), nil
	}

	engine, routeErr := s.router.Route(engineName)
	if routeErr != nil {
		return mcp.NewToolResultError(fmt.Sprintf("unknown engine: %s", engineName)), nil
	}

	result, execErr := engine.Execute(ctx, q)
	if execErr != nil {
		return mcp.NewToolResultError(fmt.Sprintf("query error: %s", execErr)), nil
	}

	data, _ := json.MarshalIndent(result, "", "  ")
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) handleHealthCheck(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	results := s.sinkSet.PingAll(ctx)
	status := make(map[string]string, len(results))
	allOk := true
	for name, err := range results {
		if err != nil {
			allOk = false
			status[name] = err.Error()
		} else {
			status[name] = "ok"
		}
	}

	resp := map[string]any{"ok": allOk, "sinks": status}
	data, _ := json.MarshalIndent(resp, "", "  ")
	return mcp.NewToolResultText(string(data)), nil
}
