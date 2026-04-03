# Polygate

A multi-database data gateway that ingests data via REST API and fans out bulk writes to multiple databases simultaneously. Query any backend through a unified query interface.

## Supported Databases

| Database | Write Method | Query Method |
|---|---|---|
| PostgreSQL | `COPY` protocol (pgx) | SQL |
| ClickHouse | `INSERT FORMAT JSONEachRow` (HTTP) | SQL |
| Elasticsearch | `_bulk` API (NDJSON) | Query DSL / query_string |
| MongoDB | `insertMany(ordered: false)` | JSON filter |
| QuestDB | ILP over TCP (InfluxDB Line Protocol) | SQL via REST |

## Quick Start

```bash
# Start all 5 databases
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
                    ┌─────▼─────┐
                    │  Engine    │  ← executes query against the target DB
                    └───────────┘
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
    ]
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

Returns:
```protobuf
syntax = "proto3";

message Events {
  double amount = 1;
  int64 id = 2;
  string name = 3;
  int64 ts = 4;
}

message EventsBatch {
  repeated Events records = 1;
}
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

**PostgreSQL / ClickHouse / QuestDB** (SQL):
```bash
curl "localhost:8080/query?engine=postgres&q=SELECT * FROM events LIMIT 10"
curl "localhost:8080/query?engine=clickhouse&q=SELECT count() FROM events"
curl "localhost:8080/query?engine=questdb&q=SELECT * FROM events"
```

**Elasticsearch** (query_string or raw JSON DSL):
```bash
# Simple query string
curl "localhost:8080/query?engine=elasticsearch&q=name:click"

# Raw JSON DSL
curl "localhost:8080/query?engine=elasticsearch&q={\"query\":{\"match\":{\"name\":\"click\"}}}"
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

### Health Checks

```bash
# Liveness — always returns 200
curl localhost:8080/healthz

# Readiness — pings all enabled sinks
curl localhost:8080/readyz
```

Readiness response:
```json
{
  "ok": true,
  "sinks": {
    "postgres": "ok",
    "clickhouse": "ok",
    "elasticsearch": "ok",
    "mongodb": "ok",
    "questdb": "ok"
  }
}
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
  bind: "0.0.0.0:8080"          # API listen address

metrics:
  enabled: true
  bind: "0.0.0.0:9090"          # Prometheus metrics address

batcher:
  max_size: 5000                 # Records per batch before flush
  flush_period: 1s               # Time-based flush interval
  buffer_size: 100000            # Channel buffer capacity

sinks:
  postgres:
    enabled: true
    dsn: "postgres://user:pass@localhost:5432/polygate"
    bulk_size: 5000              # Rows per COPY batch
    max_conns: 10                # Connection pool size
    max_retries: 3               # Retry attempts on failure
    retry_base_ms: 100           # Base backoff (doubles each retry)
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
    dsn: "localhost:9009"         # ILP TCP endpoint
    bulk_size: 10000
```

### Environment Variable Password Injection

```yaml
sinks:
  postgres:
    dsn: "postgres://user:@localhost:5432/db"   # empty password placeholder
    password_env: "PG_PASSWORD"                  # injected from $PG_PASSWORD
```

### Sink-Specific Defaults

Sinks that are not listed or have `enabled: false` are skipped. Each sink inherits sensible defaults if values are omitted:

| Setting | Default |
|---|---|
| `bulk_size` | 5000 |
| `max_retries` | 3 |
| `retry_base_ms` | 100 |
| `max_conns` | 10 |
| `timeout_secs` | 30 |

## Protobuf Ingestion

Polygate auto-generates protobuf schemas from your table definitions. This enables binary ingestion that is 5-10x faster to parse than JSON.

### Workflow

```bash
# 1. Register schema (normal JSON)
curl -X POST localhost:8080/schema -d '{
  "table": "metrics",
  "columns": {"host": "string", "cpu": "float64", "mem": "float64", "ts": "timestamp"},
  "sinks": ["questdb", "clickhouse"]
}'

# 2. Download auto-generated .proto file
curl localhost:8080/schema/metrics/proto -o metrics.proto

# 3. Compile for your language
protoc --go_out=. metrics.proto       # Go
protoc --python_out=. metrics.proto   # Python
protoc --java_out=. metrics.proto     # Java

# 4. Send binary data
curl -X POST "localhost:8080/ingest?table=metrics" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @batch.bin
```

### Type Mapping

| Polygate type | Proto type | Notes |
|---|---|---|
| `string`, `text`, `json` | `string` | JSON stored as string |
| `int32` | `int32` | |
| `int64` | `int64` | |
| `timestamp` | `int64` | Unix nanoseconds |
| `float32` | `float` | |
| `float64` | `double` | |
| `bool` | `bool` | |
| `bytes` | `bytes` | |

## Building

```bash
make build          # Build binary to bin/polygate
make test           # Run unit tests
make lint           # Run golangci-lint
make docker-up      # Start all databases
make docker-down    # Stop databases
make docker-clean   # Stop and remove volumes
make run            # Build and run with example config
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
      questdb.go                     # QuestDB ILP TCP writer
    batcher/batcher.go               # Per-table batching (size + time flush)
    query/
      engine.go                      # QueryEngine interface
      router.go                      # Engine routing by name
      pg_engine.go                   # PostgreSQL query executor
      ch_engine.go                   # ClickHouse query executor
      es_engine.go                   # Elasticsearch query executor
      mongo_engine.go                # MongoDB query executor
      quest_engine.go                # QuestDB query executor
    server/
      server.go                      # HTTP server + routes
      ingest.go                      # POST /ingest (JSON + protobuf)
      schema.go                      # POST/GET /schema + proto download
      query.go                       # GET /query
      health.go                      # /healthz + /readyz
      middleware.go                  # Request logging + panic recovery
    metrics/metrics.go               # Prometheus metric definitions
  docker-compose.yml                 # All 5 databases for local dev
  config.example.yaml                # Ready-to-use configuration
  Makefile                           # Build targets
```
