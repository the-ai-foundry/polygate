# Schema Reference

Polygate requires table schemas to be registered before data can be ingested. Schemas define the column types, target databases, indexes, and optional per-database type overrides.

## Schema Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `table` | string | Yes | Table/collection/index name |
| `columns` | map[string]type | Yes | Column names and their portable types |
| `sinks` | string[] | No | Target databases. Defaults to all enabled sinks |
| `primary_key` | string[] | No | Primary key columns (PG: `PRIMARY KEY`, CH: `ORDER BY`) |
| `indexes` | object[] | No | Secondary indexes (PG: `CREATE INDEX`, MongoDB: `createIndex`) |
| `partition_by` | string | No | Partition column (CH: `PARTITION BY toYYYYMM(col)`) |
| `designated_timestamp` | string | No | QuestDB designated timestamp column |
| `overrides` | map[sink]map[col]type | No | Per-sink native type overrides |

## Portable Type System

| Polygate Type | PostgreSQL | ClickHouse | QuestDB | Elasticsearch | MongoDB |
|---|---|---|---|---|---|
| `string` | `TEXT` | `String` | `STRING` | `keyword` | native |
| `text` | `TEXT` | `String` | `STRING` | `text` (analyzed) | native |
| `int32` | `INTEGER` | `Int32` | `INT` | `integer` | native |
| `int64` | `BIGINT` | `Int64` | `LONG` | `long` | native |
| `float32` | `REAL` | `Float32` | `FLOAT` | `float` | native |
| `float64` | `DOUBLE PRECISION` | `Float64` | `DOUBLE` | `double` | native |
| `bool` | `BOOLEAN` | `Bool` | `BOOLEAN` | `boolean` | native |
| `timestamp` | `TIMESTAMPTZ` | `DateTime64(3)` | `TIMESTAMP` | `date` | native |
| `json` | `JSONB` | `String` | `STRING` | `object` | native |
| `bytes` | `BYTEA` | `String` | `STRING` | `binary` | native |

## Type Overrides

When the portable type doesn't match what you need for a specific database, use `overrides` to specify the exact native type. Overrides take priority over the portable type mapping.

```json
{
  "overrides": {
    "questdb": {
      "source": "symbol",
      "status": "symbol"
    },
    "clickhouse": {
      "status": "LowCardinality(String)",
      "payload": "JSON"
    },
    "postgres": {
      "name": "VARCHAR(255)",
      "status": "VARCHAR(50)"
    },
    "elasticsearch": {
      "description": "text"
    }
  }
}
```

### Common Override Use Cases

| Database | Native Type | Use Case |
|---|---|---|
| QuestDB | `symbol` | Low-cardinality strings (enum-like), indexed automatically |
| ClickHouse | `LowCardinality(String)` | Same — low-cardinality string optimization |
| ClickHouse | `JSON` | Native JSON column (CH 23.1+) |
| ClickHouse | `Nullable(Int64)` | Nullable integer |
| PostgreSQL | `VARCHAR(N)` | Length-limited strings |
| PostgreSQL | `NUMERIC(18,6)` | Fixed-precision decimals |
| Elasticsearch | `text` | Full-text analyzed field (vs `keyword` for exact match) |

## Type Coercion at Ingestion

JSON has limited types (string, number, boolean, null). When data arrives via `POST /ingest`, Polygate coerces values to the correct Go types before writing to sinks:

| Portable Type | Accepted JSON values | Coerced to |
|---|---|---|
| `timestamp` | `"2026-04-03T12:00:00Z"`, `1712150400` | `time.Time` |
| `int32/int64` | `42`, `"42"`, `42.0` | `int64` |
| `float32/float64` | `3.14`, `"3.14"`, `3` | `float64` |
| `bool` | `true`, `"true"`, `"1"`, `"yes"` | `bool` |
| `bytes` | `"0xdeadbeef"`, `"deadbeef"` | `[]byte` |
| `string/text/json` | any string | pass-through |

### Timestamp Formats

The following timestamp formats are accepted (in order of precedence):

1. `2006-01-02T15:04:05.999999999Z07:00` (RFC3339Nano)
2. `2006-01-02T15:04:05Z07:00` (RFC3339)
3. `2006-01-02T15:04:05` (datetime without timezone)
4. `2006-01-02 15:04:05` (datetime with space)
5. `2006-01-02` (date only)
6. `1712150400` or `1712150400.123` (unix epoch seconds as string)
7. `1712150400` (unix epoch seconds as number)

## DDL Generation

When a schema is registered, Polygate generates and executes database-specific DDL:

### PostgreSQL
```sql
CREATE TABLE IF NOT EXISTS "events" (
  "amount" DOUBLE PRECISION,
  "id" BIGINT,
  "name" TEXT,
  "ts" TIMESTAMPTZ,
  PRIMARY KEY ("id")
);
CREATE INDEX IF NOT EXISTS "idx_events_name" ON "events" ("name");
CREATE INDEX IF NOT EXISTS "idx_events_ts" ON "events" ("ts");
```

### ClickHouse
```sql
CREATE TABLE IF NOT EXISTS events (
  amount Float64,
  id Int64,
  name String,
  ts DateTime64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(ts)
```

### Elasticsearch
```json
PUT /events
{
  "mappings": {
    "properties": {
      "amount": {"type": "double"},
      "id": {"type": "long"},
      "name": {"type": "keyword"},
      "ts": {"type": "date"}
    }
  }
}
```

### MongoDB
No DDL — collections are created on first write. Indexes are created:
```javascript
db.events.createIndex({"name": 1})
db.events.createIndex({"ts": 1})
```

### QuestDB
No explicit DDL — tables are created from ILP data. The `designated_timestamp` is used as the ILP timestamp field.

## Examples

### Example 1: Web Analytics Events

Write to PostgreSQL (point lookups) and ClickHouse (aggregations).

```bash
curl -X POST localhost:8080/schema -d '{
  "table": "page_views",
  "columns": {
    "id": "int64",
    "user_id": "string",
    "url": "text",
    "referrer": "text",
    "country": "string",
    "device": "string",
    "ts": "timestamp"
  },
  "sinks": ["postgres", "clickhouse"],
  "primary_key": ["id"],
  "indexes": [
    {"columns": ["user_id"]},
    {"columns": ["ts"]}
  ],
  "partition_by": "ts",
  "overrides": {
    "clickhouse": {
      "country": "LowCardinality(String)",
      "device": "LowCardinality(String)"
    }
  }
}'
```

Ingest:
```bash
curl -X POST "localhost:8080/ingest?table=page_views" -d '[
  {
    "id": 1,
    "user_id": "u_abc123",
    "url": "/products/shoes",
    "referrer": "https://google.com",
    "country": "US",
    "device": "mobile",
    "ts": "2026-04-03T14:30:00Z"
  }
]'
```

Query:
```bash
# Point lookup (PostgreSQL)
curl "localhost:8080/query?engine=postgres&q=SELECT * FROM page_views WHERE user_id = 'u_abc123'"

# Aggregation (ClickHouse)
curl "localhost:8080/query?engine=clickhouse&q=SELECT country, count() FROM page_views GROUP BY country ORDER BY count() DESC"
```

### Example 2: IoT Sensor Metrics

Write to QuestDB (time-series) and Elasticsearch (search by sensor).

```bash
curl -X POST localhost:8080/schema -d '{
  "table": "sensor_readings",
  "columns": {
    "sensor_id": "string",
    "location": "string",
    "temperature": "float64",
    "humidity": "float64",
    "battery": "float32",
    "ts": "timestamp"
  },
  "sinks": ["questdb", "elasticsearch"],
  "designated_timestamp": "ts",
  "overrides": {
    "questdb": {
      "sensor_id": "symbol",
      "location": "symbol"
    }
  }
}'
```

Ingest:
```bash
curl -X POST "localhost:8080/ingest?table=sensor_readings" -d '[
  {"sensor_id": "s-001", "location": "warehouse-a", "temperature": 22.5, "humidity": 45.2, "battery": 0.87, "ts": "2026-04-03T14:30:00Z"},
  {"sensor_id": "s-002", "location": "warehouse-b", "temperature": 19.8, "humidity": 62.1, "battery": 0.93, "ts": "2026-04-03T14:30:00Z"}
]'
```

Query:
```bash
# Time-series query (QuestDB)
curl "localhost:8080/query?engine=questdb&q=SELECT sensor_id, avg(temperature) FROM sensor_readings SAMPLE BY 1h"

# Search by location (Elasticsearch)
curl "localhost:8080/query?engine=elasticsearch&q=location:warehouse-a"
```

### Example 3: User Profiles (Document Store)

Write to MongoDB (flexible documents) and PostgreSQL (relational queries).

```bash
curl -X POST localhost:8080/schema -d '{
  "table": "users",
  "columns": {
    "user_id": "string",
    "email": "string",
    "name": "string",
    "metadata": "json",
    "created_at": "timestamp"
  },
  "sinks": ["mongodb", "postgres"],
  "primary_key": ["user_id"],
  "indexes": [
    {"columns": ["email"]}
  ]
}'
```

Ingest:
```bash
curl -X POST "localhost:8080/ingest?table=users" -d '[
  {
    "user_id": "u_001",
    "email": "alice@example.com",
    "name": "Alice",
    "metadata": {"plan": "pro", "features": ["dark_mode", "api_access"]},
    "created_at": "2026-01-15T09:00:00Z"
  }
]'
```

Query:
```bash
# Relational (PostgreSQL)
curl "localhost:8080/query?engine=postgres&q=SELECT user_id, email FROM users WHERE created_at > '2026-01-01'"

# Document (MongoDB)
curl "localhost:8080/query?engine=mongodb&q={\"collection\":\"users\",\"filter\":{\"email\":\"alice@example.com\"}}"
```

### Example 4: All 5 Databases

Write to everything — useful for a data lake pattern.

```bash
curl -X POST localhost:8080/schema -d '{
  "table": "orders",
  "columns": {
    "order_id": "int64",
    "customer_id": "string",
    "product": "string",
    "quantity": "int32",
    "price": "float64",
    "status": "string",
    "notes": "text",
    "ts": "timestamp"
  },
  "sinks": ["postgres", "clickhouse", "elasticsearch", "mongodb", "questdb"],
  "primary_key": ["order_id"],
  "indexes": [
    {"columns": ["customer_id"]},
    {"columns": ["status"]}
  ],
  "partition_by": "ts",
  "designated_timestamp": "ts",
  "overrides": {
    "questdb": {
      "customer_id": "symbol",
      "product": "symbol",
      "status": "symbol"
    },
    "clickhouse": {
      "status": "LowCardinality(String)",
      "product": "LowCardinality(String)"
    },
    "elasticsearch": {
      "notes": "text"
    }
  }
}'
```

Each database serves different query patterns:

```bash
# OLTP lookup (PostgreSQL)
curl "localhost:8080/query?engine=postgres&q=SELECT * FROM orders WHERE order_id = 42"

# Analytics (ClickHouse)
curl "localhost:8080/query?engine=clickhouse&q=SELECT product, sum(price * quantity) AS revenue FROM orders GROUP BY product ORDER BY revenue DESC"

# Full-text search (Elasticsearch)
curl "localhost:8080/query?engine=elasticsearch&q=notes:urgent"

# Document query (MongoDB)
curl "localhost:8080/query?engine=mongodb&q={\"collection\":\"orders\",\"filter\":{\"status\":\"shipped\"},\"limit\":50}"

# Time-series (QuestDB)
curl "localhost:8080/query?engine=questdb&q=SELECT sum(price * quantity) FROM orders SAMPLE BY 1d"
```
