# Polygate

A multi-database data gateway that ingests data via REST API and fans out bulk writes to multiple databases simultaneously. Query any backend through a unified query interface with optional SSE streaming.

## Supported Databases

| Database | Write Method | Query Method | SSE Streaming |
|---|---|---|---|
| PostgreSQL | `COPY` protocol (pgx) | SQL | Server-side cursor (`DECLARE/FETCH`) |
| ClickHouse | `INSERT FORMAT JSONEachRow` (HTTP) | SQL | Chunked `JSONEachRow` streaming |
| Elasticsearch | `_bulk` API (NDJSON) | Query DSL / query_string | Scroll API (`scroll_id`) |
| MongoDB | `insertMany(ordered: false)` | JSON filter | Cursor with `BatchSize` |
| QuestDB | ILP over TCP (InfluxDB Line Protocol) | SQL via REST | `/exp` CSV streaming |
| Trino | — (read-only federation) | SQL | `nextUri` page following |

## Quick Start

```bash
# Start all databases (PG, CH, ES, Mongo, QuestDB, Trino)
docker compose up -d

# Build and run
make run

# Or run directly
go run ./cmd/polygate -config config.example.yaml
```

Polygate starts on `:8080` (API) and `:9090` (Prometheus metrics).

## Architecture

```
                    POST /ingest
                         │
                    ┌─────▼─────┐
                    │  Batcher   │  ← accumulates records, flushes by size or time
                    └─────┬─────┘
                          │
                    ┌─────▼─────┐
                    │  Coerce    │  ← converts JSON types to native types using schema
                    └─────┬─────┘
                          │
                    ┌─────▼─────┐
                    │  SinkSet   │  ← concurrent fan-out via errgroup
                    └─────┬─────┘
                          │
         ┌────────┬───────┼────────┬──────────┐
         ▼        ▼       ▼        ▼          ▼
     PostgreSQL  CH    Elastic   Mongo     QuestDB

                    GET /query?engine=X
                         │
                    ┌─────▼─────┐
                    │  Router    │  ← routes to the specified engine
                    └─────┬─────┘
                          │
         ┌────────┬───────┼────────┬──────────┬──────┐
         ▼        ▼       ▼        ▼          ▼      ▼
     PostgreSQL  CH    Elastic   Mongo     QuestDB  Trino
                                                  (federation)
```

## API Reference

### Schema Management

Schemas must be registered before data can be ingested. The schema defines columns, types, target sinks, indexes, and optional per-database type overrides.

#### `POST /schema` — Register a table schema

Creates the table/index in all target sinks.

```bash
curl -X POST localhost:8080/schema \
  -H "Content-Type: application/json" \
  -d '{
    "table": "events",
    "columns": {
      "id": "int64",
      "name": "string",
      "ts": "timestamp",
      "amount": "float64"
    },
    "sinks": ["postgres", "clickhouse"],
    "primary_key": ["id"],
    "indexes": [
      {"columns": ["name"]},
      {"columns": ["ts"]}
    ],
    "partition_by": "ts",
    "overrides": {
      "clickhouse": {"name": "LowCardinality(String)"}
    }
  }'
```

Response (`201 Created`):
```json
{
  "ok": true,
  "table": "events",
  "sinks": ["postgres", "clickhouse"]
}
```

#### `GET /schema` — List all schemas

```bash
curl localhost:8080/schema
```

#### `GET /schema/{table}` — Get a specific schema

```bash
curl localhost:8080/schema/events
```

#### `GET /schema/{table}/proto` — Download auto-generated `.proto` file

```bash
curl localhost:8080/schema/events/proto -o events.proto
```

### Data Ingestion

#### `POST /ingest?table={name}` — Ingest records

**JSON format** (default):
```bash
curl -X POST "localhost:8080/ingest?table=events" \
  -H "Content-Type: application/json" \
  -d '[
    {"id": 1, "name": "click", "ts": "2026-04-03T12:00:00Z", "amount": 9.99},
    {"id": 2, "name": "view", "ts": "2026-04-03T12:01:00Z", "amount": 0}
  ]'
```

**Protobuf format** (binary, higher throughput):
```bash
curl -X POST "localhost:8080/ingest?table=events" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @batch.bin
```

Response (`202 Accepted`):
```json
{
  "ok": true,
  "records": 2,
  "format": "json"
}
```

Error responses:
- `400` — table not registered, invalid JSON, protobuf decode error, empty records
- `429` — ingestion buffer full (backpressure)

### Querying

#### `GET /query?engine={name}&q={query}` — Execute a query

**PostgreSQL / ClickHouse / QuestDB / Trino** (SQL):
```bash
curl "localhost:8080/query?engine=postgres&q=SELECT * FROM events LIMIT 10"
curl "localhost:8080/query?engine=clickhouse&q=SELECT count() FROM events"
curl "localhost:8080/query?engine=questdb&q=SELECT * FROM events"
curl "localhost:8080/query?engine=trino&q=SELECT * FROM postgres.polygate.events"
```

**Elasticsearch** (query_string or raw JSON DSL):
```bash
curl "localhost:8080/query?engine=elasticsearch&q=name:click"
```

**MongoDB** (JSON filter):
```bash
curl "localhost:8080/query?engine=mongodb&q={\"collection\":\"events\",\"filter\":{\"name\":\"click\"},\"limit\":10}"
```

Response:
```json
{
  "ok": true,
  "result": {
    "columns": ["id", "name", "ts", "amount"],
    "rows": [
      {"id": 1, "name": "click", "ts": "2026-04-03T12:00:00Z", "amount": 9.99}
    ],
    "engine": "postgres"
  },
  "query_time_ms": 12
}
```

#### SSE Streaming — `GET /query?...&stream=true`

Stream large result sets as Server-Sent Events. Each event contains a page of rows.

```bash
curl -N "localhost:8080/query?engine=postgres&q=SELECT * FROM events&stream=true&page_size=1000"
```

Parameters:
- `stream=true` — enable SSE streaming
- `page_size=1000` — rows per SSE event (default 1000, max 10000)
- `table=events` — required for QuestDB streaming (CSV schema lookup)

SSE event format:
```
data: {"columns":["id","name"],"rows":[...],"page":1,"done":false,"engine":"postgres"}

data: {"columns":["id","name"],"rows":[...],"page":2,"done":true,"engine":"postgres"}

event: done
data: {}
```

Streaming mechanism per engine:

| Engine | How it streams |
|---|---|
| PostgreSQL | Server-side cursor (`DECLARE cursor FOR ... ; FETCH N`) |
| ClickHouse | `FORMAT JSONEachRow` — one JSON object per line |
| Elasticsearch | Scroll API with `scroll_id` pagination |
| MongoDB | Cursor with `BatchSize` iteration |
| QuestDB | `/exp` CSV endpoint + schema-based type coercion |
| Trino | `nextUri` page following (native pagination) |

#### Trino Cross-Database Queries

Trino enables federated SQL across all databases:

```bash
# Join PostgreSQL and ClickHouse data
curl "localhost:8080/query?engine=trino&q=SELECT o.customer, m.total FROM postgres.polygate.orders o JOIN clickhouse.default.metrics m ON o.id = m.order_id"

# Query Elasticsearch via SQL
curl "localhost:8080/query?engine=trino&q=SELECT * FROM elasticsearch.default.events"

# Query MongoDB via SQL
curl "localhost:8080/query?engine=trino&q=SELECT * FROM mongodb.polygate.users"

# Query QuestDB via SQL (through PG wire protocol)
curl "localhost:8080/query?engine=trino&q=SELECT * FROM questdb.qdb.sensor_readings"
```

Naming convention: `{catalog}.{schema}.{table}`

### Health Checks

```bash
# Liveness — always returns 200
curl localhost:8080/healthz

# Readiness — pings all enabled sinks
curl localhost:8080/readyz
```

### Prometheus Metrics

Available at `:9090/metrics`:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `polygate_ingest_records_total` | counter | — | Total records received |
| `polygate_ingest_batches_total` | counter | — | Total batches flushed |
| `polygate_sink_write_duration_seconds` | histogram | sink, table | Write latency per sink |
| `polygate_sink_write_rows_total` | counter | sink, table | Rows written per sink |
| `polygate_sink_errors_total` | counter | sink | Write errors per sink |
| `polygate_sink_healthy` | gauge | sink | Sink health (1=up, 0=down) |
| `polygate_query_duration_seconds` | histogram | engine | Query latency per engine |
| `polygate_batcher_queue_size` | gauge | — | Current batcher buffer size |
| `polygate_schema_registrations_total` | counter | — | Total schema registrations |

## Configuration

See [`config.example.yaml`](config.example.yaml) for a complete example.

```yaml
server:
  bind: "0.0.0.0:8080"

metrics:
  enabled: true
  bind: "0.0.0.0:9090"

batcher:
  max_size: 5000
  flush_period: 1s
  buffer_size: 100000

sinks:
  postgres:
    enabled: true
    dsn: "postgres://user:pass@localhost:5432/polygate"
    bulk_size: 5000
    max_conns: 10
    max_retries: 3
    retry_base_ms: 100
    timeout_secs: 30

  clickhouse:
    enabled: true
    dsn: "http://localhost:8123"
    bulk_size: 10000

  elasticsearch:
    enabled: true
    dsn: "http://localhost:9200"
    bulk_size: 5000

  mongodb:
    enabled: true
    dsn: "mongodb://localhost:27017/polygate"
    bulk_size: 5000

  questdb:
    enabled: true
    dsn: "localhost:9009"              # ILP TCP (ingestion)
    rest_url: "http://localhost:9003"  # REST API (DDL + queries)
    bulk_size: 10000

# Trino federated query engine (optional)
trino:
  enabled: true
  url: "http://localhost:8085"

# MCP server for LLM integrations (optional)
mcp:
  enabled: false
  transport: "stdio"      # "stdio" (Claude Code, Cursor) or "http" (remote)
  http_port: 8090
```

## MCP Server (Model Context Protocol)

Polygate includes an MCP server that exposes schemas and tools for LLM integrations. This allows Claude, ChatGPT, Cursor, and other AI tools to discover and interact with your databases through natural language.

### Resources (read-only context)

| Resource URI | Description |
|---|---|
| `polygate://api` | Full API documentation |
| `polygate://schemas` | All registered table schemas |
| `polygate://schemas/{table}` | Specific table schema |
| `polygate://schemas/{table}/proto` | Auto-generated .proto file |
| `polygate://sinks` | Enabled sinks with health status |
| `polygate://types` | Portable type system reference |

### Tools (executable actions)

| Tool | Description |
|---|---|
| `register_schema` | Register a new table schema and create tables |
| `ingest` | Ingest JSON records into a table |
| `query` | Execute a query against any engine |
| `health_check` | Check all sink connectivity |

### Setup with Claude Code

```json
{
  "mcpServers": {
    "polygate": {
      "command": "./bin/polygate",
      "args": ["-config", "config.yaml"]
    }
  }
}
```

Enable in config:
```yaml
mcp:
  enabled: true
  transport: "stdio"
```

### Setup for Remote Access (HTTP)

```yaml
mcp:
  enabled: true
  transport: "http"
  http_port: 8090
```

## Protobuf Ingestion

Polygate auto-generates protobuf schemas from your table definitions for binary ingestion (5-10x faster than JSON).

```bash
# 1. Register schema
curl -X POST localhost:8080/schema -d '{
  "table": "metrics",
  "columns": {"host": "string", "cpu": "float64", "ts": "timestamp"},
  "sinks": ["questdb", "clickhouse"]
}'

# 2. Download .proto file
curl localhost:8080/schema/metrics/proto -o metrics.proto

# 3. Compile for your language
protoc --go_out=. metrics.proto

# 4. Send binary data
curl -X POST "localhost:8080/ingest?table=metrics" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @batch.bin
```

## Deployment Profiles

Don't need all 5 databases? Use a lightweight profile that only starts what you need:

| Profile | Command | Databases | RAM Needed |
|---|---|---|---|
| **Full stack** | `make run` | PG + CH + ES + Mongo + QuestDB + Trino | ~32 GB |
| **PG + MongoDB** | `make pg-mongo` | PostgreSQL + MongoDB | ~6 GB |
| **MongoDB + ES** | `make mongo-es` | MongoDB + Elasticsearch | ~6 GB |
| **PG + ES** | `make pg-es` | PostgreSQL + Elasticsearch | ~8 GB |

Each profile includes a matching docker-compose and config file:

```bash
# Start PostgreSQL + MongoDB only
make pg-mongo

# Or manually:
docker compose -f docker-compose.pg-mongo.yml up -d
./bin/polygate -config config.pg-mongo.yaml
```

Only enabled sinks are initialized — disabled databases use zero memory and no connections. If a schema references a disabled sink, registration returns an error. If `sinks` is omitted from a schema, it defaults to whichever sinks are enabled.

### Custom profiles

Create your own by copying any config file and toggling `enabled`:

```yaml
sinks:
  postgres:
    enabled: true
    dsn: "postgres://..."
  clickhouse:
    enabled: true
    dsn: "http://..."
  elasticsearch:
    enabled: false      # disabled — skipped entirely
  mongodb:
    enabled: false      # disabled
  questdb:
    enabled: false      # disabled
```

## Building

```bash
make build          # Build binary to bin/polygate
make test           # Run unit tests
make lint           # Run golangci-lint
make docker-up      # Start all databases (full stack)
make docker-down    # Stop databases
make docker-clean   # Stop and remove volumes
make run            # Build and run full stack
make pg-mongo       # Build and run PG + MongoDB profile
make mongo-es       # Build and run MongoDB + ES profile
make pg-es          # Build and run PG + ES profile
```

## Project Structure

```
polygate/
  cmd/polygate/main.go              # Entrypoint, wiring, graceful shutdown
  internal/
    config/config.go                 # YAML config structs + validation
    model/record.go                  # type Record = map[string]any
    schema/
      schema.go                      # Schema registry (in-memory)
      types.go                       # Portable type system + per-DB mapping
      coerce.go                      # JSON → native type coercion
      proto.go                       # Auto-generated protobuf support
    sink/
      sink.go                        # Sink interface + SinkSet fan-out
      retry.go                       # Exponential backoff retry
      postgres.go                    # PostgreSQL COPY writer
      clickhouse.go                  # ClickHouse HTTP bulk writer
      elasticsearch.go               # Elasticsearch _bulk writer
      mongodb.go                     # MongoDB insertMany writer
      questdb.go                     # QuestDB ILP TCP writer + REST DDL
    batcher/batcher.go               # Per-table batching (size + time flush)
    query/
      engine.go                      # QueryEngine interface
      stream.go                      # StreamEngine interface (SSE)
      router.go                      # Engine routing by name
      pg_engine.go                   # PostgreSQL (cursor streaming)
      ch_engine.go                   # ClickHouse (JSONEachRow streaming)
      es_engine.go                   # Elasticsearch (scroll streaming)
      mongo_engine.go                # MongoDB (cursor streaming)
      quest_engine.go                # QuestDB (CSV streaming)
      trino_engine.go                # Trino (page streaming)
    mcp/
      mcp.go                         # MCP server setup (stdio + HTTP)
      resources.go                   # MCP resources (schemas, API docs, types)
      tools.go                       # MCP tools (register, ingest, query)
    server/
      server.go                      # HTTP server + routes
      ingest.go                      # POST /ingest (JSON + protobuf)
      schema.go                      # POST/GET /schema + proto download
      query.go                       # GET /query (+ SSE streaming)
      health.go                      # /healthz + /readyz
      middleware.go                  # Request logging + panic recovery
    metrics/metrics.go               # Prometheus metric definitions
  trino/                             # Trino config + catalog connectors
  docker-compose.yml                 # All databases + Trino for local dev
  config.example.yaml                # Ready-to-use configuration
  Makefile                           # Build targets
  docs/
    schema.md                        # Schema reference + examples
    playbook.md                      # Step-by-step testing guide
```
