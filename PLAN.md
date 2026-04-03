# Polygate — Multi-Database Data Gateway

## Context

We want to build a Go project inspired by TIDX's architecture (fan-out writes, query routing) but generalized for any data across 5 databases: PostgreSQL, ClickHouse, Elasticsearch, MongoDB, and QuestDB. No such unified tool exists today — this fills a gap between write-focused tools (Benthos) and read-focused tools (Trino).

## Project Structure

```
polygate/
  cmd/polygate/main.go              # Entrypoint, wiring, graceful shutdown
  internal/
    config/config.go                 # YAML config structs, loader, validation
    model/record.go                  # type Record = map[string]any
    schema/
      schema.go                      # Schema registry (in-memory + PG persistence)
      types.go                       # Portable type system + per-DB DDL translation
      coerce.go                      # Ingestion-time type coercion (JSON → native types)
    sink/
      sink.go                        # Sink interface + SinkSet (errgroup fan-out)
      retry.go                       # Shared retry with exponential backoff
      postgres.go                    # pgx CopyFrom bulk writer
      clickhouse.go                  # HTTP POST JSONEachRow
      elasticsearch.go              # _bulk NDJSON API
      mongodb.go                    # insertMany
      questdb.go                    # ILP over TCP
    batcher/batcher.go               # Channel-fed buffer, size+time flush triggers
    query/
      engine.go                      # QueryEngine interface
      router.go                      # Engine registry, route by name
      pg_engine.go                   # PG query executor
      ch_engine.go                   # CH query executor
      es_engine.go                   # ES query executor
      mongo_engine.go                # Mongo query executor
      quest_engine.go                # QuestDB query executor
    server/
      server.go                      # HTTP server, route setup
      ingest.go                      # POST /ingest — push to batcher
      schema.go                      # POST /schema — register table schema
      query.go                       # GET /query?engine=X&q=...
      health.go                      # /healthz, /readyz
      middleware.go                  # Logging, recovery, request ID
    metrics/metrics.go               # Prometheus counters/histograms/gauges
  docker-compose.yml                 # All 5 DBs for local dev
  config.example.yaml                # Documented example config
  Makefile                           # build, test, lint, docker-up
  go.mod
```

## Key Interfaces

### Sink (write path)
```go
type Sink interface {
    Name() string
    WriteBatch(ctx context.Context, table string, records []Record) error
    Ping(ctx context.Context) error
    Close() error
}
```

### SinkSet (fan-out via errgroup — adapted from TIDX's tokio::try_join!)
```go
type SinkSet struct {
    sinks  map[string]Sink   // all enabled sinks, keyed by name
    schema *schema.Registry  // lookup table's configured sinks
}

func (s *SinkSet) WriteBatch(ctx context.Context, table string, records []Record) error {
    targetSinks := s.schema.SinksFor(table) // e.g. ["postgres", "clickhouse"]
    g, ctx := errgroup.WithContext(ctx)
    for _, name := range targetSinks {
        sk := s.sinks[name]
        g.Go(func() error { return sk.WriteBatch(ctx, table, records) })
    }
    return g.Wait()
}
```

### QueryEngine (read path)
```go
type QueryEngine interface {
    Name() string
    Execute(ctx context.Context, query string, params map[string]any) (*Result, error)
    Ping(ctx context.Context) error
}
```

## Schema Management

Tables must be registered via `POST /schema` before data can be ingested. No auto-create.

### Schema Registration Endpoint

```
POST /schema
{
  "table": "events",
  "columns": {
    "id": "int64",
    "name": "string",
    "ts": "timestamp",
    "amount": "float64",
    "metadata": "json"
  },
  "sinks": ["postgres", "clickhouse", "elasticsearch"],
  "primary_key": ["id"],
  "indexes": [
    {"columns": ["name"]},
    {"columns": ["ts", "amount"]}
  ],
  "partition_by": "ts",
  "designated_timestamp": "ts",
  "overrides": {
    "questdb": {
      "name": "symbol"
    },
    "clickhouse": {
      "name": "LowCardinality(String)"
    },
    "postgres": {
      "name": "VARCHAR(255)"
    }
  }
}
```

- `sinks` — which databases receive data for this table. Only these sinks get DDL + writes.
- If omitted, defaults to all enabled sinks.
- Tables are only created in the specified sinks.
- The `SinkSet.WriteBatch()` filters to the table's configured sinks at write time.
- `indexes` — list of secondary indexes. Applied to PG (`CREATE INDEX`) and MongoDB (`createIndex`). Ignored by ES (auto-indexed), QuestDB (auto-indexed), CH (use `primary_key`/`ORDER BY`).
- `overrides` — per-sink native type overrides. When set, the raw native type string is used directly in DDL instead of the portable type mapping. Examples: QuestDB `symbol`, CH `LowCardinality(String)`, PG `VARCHAR(50)`, ES `keyword` vs `text`.

### DDL type resolution order

```go
func (s *TableSchema) ColumnType(col string, sinkName string) string {
    // 1. Check sink-specific override first
    if override, ok := s.Overrides[sinkName][col]; ok {
        return override  // raw native type, passed directly to DDL
    }
    // 2. Fall back to portable type → DB-specific mapping
    return portableToNative(s.Columns[col], sinkName)
}
```

### Portable Type System

| Polygate type | PG | CH | QuestDB | ES | MongoDB |
|---|---|---|---|---|---|
| `string` | `TEXT` | `String` | `STRING` | `keyword` | (native) |
| `text` | `TEXT` | `String` | `STRING` | `text` (analyzed) | (native) |
| `int32` | `INTEGER` | `Int32` | `INT` | `integer` | (native) |
| `int64` | `BIGINT` | `Int64` | `LONG` | `long` | (native) |
| `float32` | `REAL` | `Float32` | `FLOAT` | `float` | (native) |
| `float64` | `DOUBLE PRECISION` | `Float64` | `DOUBLE` | `double` | (native) |
| `bool` | `BOOLEAN` | `Bool` | `BOOLEAN` | `boolean` | (native) |
| `timestamp` | `TIMESTAMPTZ` | `DateTime64(3)` | `TIMESTAMP` | `date` | (native) |
| `json` | `JSONB` | `String` | `STRING` | `object` | (native) |
| `bytes` | `BYTEA` | `String` | `STRING` | `binary` | (native) |

### How it works

1. `POST /schema` → schema stored in-memory registry (`map[string]TableSchema`)
2. For each **enabled** schema-aware sink (PG, CH, QuestDB): generate DB-specific `CREATE TABLE` DDL and execute it
3. For Elasticsearch: create index with mapping
4. For MongoDB: no DDL needed (schema-free), but schema is stored for type coercion
5. `POST /ingest?table=events` → checks registry. If table not registered → **400 error**
6. Schemas persisted to PG `_polygate_schemas` table so they survive restarts

### Ingestion-time type coercion (`internal/schema/coerce.go`)

JSON only has strings, numbers, bools, and nulls. The schema drives type coercion
before data reaches the sinks. This ensures every DB receives properly typed values
(e.g., MongoDB gets BSON DateTime, not a string; QuestDB gets a numeric timestamp).

```
POST /ingest → validate schema exists → push to batcher
                                              ↓
                                    coerce batch using schema
                                              ↓
                                    SinkSet.WriteBatch()
```

Coercion rules:
- `timestamp`: string (RFC3339/ISO8601) or number (unix epoch) → `time.Time`
- `int32/int64`: string or float → integer
- `float32/float64`: string or int → float
- `bool`: string ("true"/"false"/"1"/"0") → bool
- `bytes`: hex string "0x..." → `[]byte`
- `string/text/json`: pass through as-is

Records with coercion errors are rejected with a descriptive error (row index + column name).

### Schema hints

- `primary_key` → used by PG (`PRIMARY KEY`) and CH (`ORDER BY`)
- `partition_by` → used by CH (`PARTITION BY toYYYYMM(col)`)
- `designated_timestamp` → required by QuestDB for time-series partitioning

## Bulk Insert Strategy Per DB

| DB | Method | Chunk Size |
|---|---|---|
| PostgreSQL | `pgx.CopyFrom` (COPY protocol) | 5,000 |
| ClickHouse | HTTP POST `FORMAT JSONEachRow` | 10,000 |
| Elasticsearch | `POST /_bulk` (NDJSON) | 5,000 |
| MongoDB | `InsertMany(ordered: false)` | 5,000 |
| QuestDB | ILP over TCP (line protocol) | 10,000 |

## Batcher Design

- Records arrive via buffered channel from HTTP handler
- Per-table accumulation: `map[string][]Record`
- Dual-trigger flush: `len(batch) >= MaxSize` OR `time.Ticker` fires
- On flush → `SinkSet.WriteBatch()` (concurrent fan-out)
- Backpressure: channel full → HTTP returns 429
- Graceful shutdown: drain channel, final flush

## API Endpoints

- `POST /schema` — register table schema, creates tables in all enabled sinks
- `GET /schema` — list all registered schemas
- `GET /schema/{table}` — get schema for a specific table
- `POST /ingest?table=events` — accepts JSON array, returns 202 Accepted. **Requires schema to be registered first (400 if not).**
- `GET /query?engine=clickhouse&q=SELECT...` — routes to engine, returns JSON result
- `GET /healthz` — always 200
- `GET /readyz` — pings all enabled sinks

## Configuration (YAML)

```yaml
server:
  bind: "0.0.0.0:8080"
batcher:
  max_size: 5000
  flush_period: 1s
  buffer_size: 100000
sinks:
  postgres:
    enabled: true
    dsn: "postgres://user:pass@localhost:5432/polygate"
    bulk_size: 5000
    max_retries: 3
  clickhouse:
    enabled: true
    dsn: "http://localhost:8123"
    bulk_size: 10000
  elasticsearch:
    enabled: true
    dsn: "http://localhost:9200"
  mongodb:
    enabled: true
    dsn: "mongodb://localhost:27017/polygate"
  questdb:
    enabled: true
    dsn: "localhost:9009"
```

## Go Dependencies

| Package | Purpose |
|---|---|
| `github.com/jackc/pgx/v5` | PG driver with COPY |
| `go.mongodb.org/mongo-driver/v2` | MongoDB |
| `github.com/prometheus/client_golang` | Metrics |
| `golang.org/x/sync/errgroup` | Concurrent fan-out |
| `gopkg.in/yaml.v3` | Config |
| `log/slog` (stdlib) | Structured logging |
| `net/http` (stdlib) | CH + ES + HTTP server |

No drivers needed for ClickHouse, Elasticsearch, or QuestDB — their protocols (HTTP/TCP) are simple enough with stdlib.

## Implementation Phases

### Phase 1: Skeleton
1. `go mod init`, main.go with signal handling
2. Config structs + YAML loader
3. Record type + portable type system
4. Schema registry + DDL generation per DB
5. Sink interface + SinkSet + retry utility
6. Metrics definitions

### Phase 2: Sink Implementations
6. PostgreSQL sink (pgx CopyFrom)
7. ClickHouse sink (HTTP JSONEachRow)
8. Elasticsearch sink (_bulk NDJSON)
9. MongoDB sink (insertMany)
10. QuestDB sink (ILP TCP)

### Phase 3: Batcher + Ingest + Schema API
11. Batcher with dual-trigger flush
12. HTTP server + POST /schema handler (schema registration + DDL execution)
13. POST /ingest handler (validates table is registered, pushes to batcher)

### Phase 4: Query Routing
13. QueryEngine interface + Router
14. Per-engine query executors (5 files)
15. GET /query handler

### Phase 5: Health + Polish
16. Health endpoints
17. Middleware (logging, recovery)
18. Wire everything in main.go

### Phase 6: DevOps
19. docker-compose.yml (all 5 DBs)
20. config.example.yaml
21. Makefile

## Error Handling

- **Ingest**: 202 Accepted immediately. Write failures logged + metricked, don't block ingestion.
- **Fan-out mode** (configurable):
  - `fail_fast`: any sink failure fails the batch (like TIDX)
  - `best_effort`: log errors, continue with remaining sinks
- **Retry**: transient errors (network, 5xx) retried with exponential backoff. Permanent errors (400) fail immediately.
- **Backpressure**: batcher channel full → 429 Too Many Requests

## Verification

1. `docker compose up -d` — start all 5 DBs
2. `go run cmd/polygate/main.go -config config.example.yaml`
3. Register schema first:
   `curl -X POST localhost:8080/schema -d '{"table":"test","columns":{"id":"int64","name":"string"},"primary_key":["id"]}'` → 201
4. `curl -X POST localhost:8080/ingest?table=test -d '[{"id":1,"name":"foo"}]'` → 202
5. `curl "localhost:8080/query?engine=postgres&q=SELECT * FROM test"` → JSON rows
6. Verify ingest without schema fails:
   `curl -X POST localhost:8080/ingest?table=unknown -d '[{"id":1}]'` → 400
5. `curl localhost:8080/readyz` → 200 with all sinks healthy
6. `curl localhost:9090/metrics` → Prometheus metrics with sink labels
7. `go test ./...` — unit tests pass
