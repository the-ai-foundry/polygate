# Polygate Playbook

Step-by-step guide to test the full pipeline: start databases, register a schema, ingest data, and query it back.

## Prerequisites

```bash
# Start all 5 databases
docker compose up -d

# Wait for databases to be ready (~10-15 seconds)
docker compose ps

# Build and start polygate
make run
```

Polygate runs on `localhost:8080` (API) and `localhost:9090` (metrics).

---

## Step 1: Health Check

Verify polygate is running and all sinks are healthy.

```bash
# Liveness
curl -s localhost:8080/healthz | jq
```
```json
{"ok": true}
```

```bash
# Readiness (pings all databases)
curl -s localhost:8080/readyz | jq
```
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

---

## Step 2: Register Schema

Register a table schema. This creates the table/index in all target sinks.

```bash
curl -s -X POST localhost:8080/schema \
  -H "Content-Type: application/json" \
  -d '{
    "table": "orders",
    "columns": {
      "id": "int64",
      "customer": "string",
      "product": "string",
      "quantity": "int32",
      "price": "float64",
      "ts": "timestamp"
    },
    "sinks": ["postgres", "clickhouse"],
    "primary_key": ["id"],
    "indexes": [
      {"columns": ["customer"]}
    ],
    "partition_by": "ts"
  }' | jq
```
```json
{
  "ok": true,
  "table": "orders",
  "sinks": ["postgres", "clickhouse"]
}
```

### Verify schema was saved

```bash
curl -s localhost:8080/schema/orders | jq
```

### Download auto-generated .proto file

```bash
curl -s localhost:8080/schema/orders/proto
```
```protobuf
syntax = "proto3";

message Orders {
  string customer = 1;
  int64 id = 2;
  double price = 3;
  string product = 4;
  int32 quantity = 5;
  int64 ts = 6;
}

message OrdersBatch {
  repeated Orders records = 1;
}
```

---

## Step 3: Ingest Data

Send records. Timestamps can be RFC3339 strings or unix epoch numbers.

```bash
curl -s -X POST "localhost:8080/ingest?table=orders" \
  -H "Content-Type: application/json" \
  -d '[
    {"id": 1, "customer": "alice",   "product": "laptop",   "quantity": 1, "price": 999.99, "ts": "2026-04-03T10:00:00Z"},
    {"id": 2, "customer": "bob",     "product": "keyboard", "quantity": 2, "price": 49.99,  "ts": "2026-04-03T10:05:00Z"},
    {"id": 3, "customer": "alice",   "product": "mouse",    "quantity": 1, "price": 29.99,  "ts": "2026-04-03T10:10:00Z"},
    {"id": 4, "customer": "charlie", "product": "monitor",  "quantity": 1, "price": 399.99, "ts": "2026-04-03T10:15:00Z"},
    {"id": 5, "customer": "bob",     "product": "laptop",   "quantity": 1, "price": 999.99, "ts": "2026-04-03T10:20:00Z"}
  ]' | jq
```
```json
{
  "ok": true,
  "records": 5,
  "format": "json"
}
```

### Verify: ingest without schema fails

```bash
curl -s -X POST "localhost:8080/ingest?table=nonexistent" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1}]'
```
```
no schema registered for table: nonexistent. Register via POST /schema first.
```

---

## Step 4: Query Data

Wait 1-2 seconds for the batcher to flush, then query.

### Select all records (PostgreSQL)

```bash
curl -s "localhost:8080/query?engine=postgres&q=SELECT+*+FROM+orders+ORDER+BY+id" | jq
```

### Filter by customer (PostgreSQL)

```bash
curl -s "localhost:8080/query?engine=postgres&q=SELECT+*+FROM+orders+WHERE+customer='alice'" | jq
```

### Aggregate total revenue (ClickHouse)

```bash
curl -s "localhost:8080/query?engine=clickhouse&q=SELECT+product,+sum(price*quantity)+AS+revenue+FROM+orders+GROUP+BY+product+ORDER+BY+revenue+DESC" | jq
```

### Count orders per customer (ClickHouse)

```bash
curl -s "localhost:8080/query?engine=clickhouse&q=SELECT+customer,+count()+AS+order_count+FROM+orders+GROUP+BY+customer" | jq
```

Expected response format:
```json
{
  "ok": true,
  "result": {
    "columns": ["customer", "order_count"],
    "rows": [
      {"customer": "alice", "order_count": "2"},
      {"customer": "bob", "order_count": "2"},
      {"customer": "charlie", "order_count": "1"}
    ],
    "engine": "clickhouse"
  },
  "query_time_ms": 15
}
```

---

## Step 5: Check Metrics

```bash
curl -s localhost:9090/metrics | grep polygate
```

Key metrics to look for:
```
polygate_ingest_records_total 5
polygate_ingest_batches_total 1
polygate_sink_write_rows_total{sink="postgres",table="orders"} 5
polygate_sink_write_rows_total{sink="clickhouse",table="orders"} 5
polygate_schema_registrations_total 1
```

---

## Full Test Script

Copy and run this as a single script:

```bash
#!/bin/bash
set -e

BASE="http://localhost:8080"

echo "=== Health Check ==="
curl -sf $BASE/healthz | jq

echo -e "\n=== Register Schema ==="
curl -sf -X POST $BASE/schema \
  -H "Content-Type: application/json" \
  -d '{
    "table": "test_orders",
    "columns": {
      "id": "int64",
      "customer": "string",
      "product": "string",
      "price": "float64",
      "ts": "timestamp"
    },
    "sinks": ["postgres", "clickhouse"],
    "primary_key": ["id"]
  }' | jq

echo -e "\n=== Verify Schema ==="
curl -sf $BASE/schema/test_orders | jq

echo -e "\n=== Download Proto ==="
curl -sf $BASE/schema/test_orders/proto

echo -e "\n=== Ingest Records ==="
curl -sf -X POST "$BASE/ingest?table=test_orders" \
  -H "Content-Type: application/json" \
  -d '[
    {"id": 1, "customer": "alice", "product": "laptop", "price": 999.99, "ts": "2026-04-03T10:00:00Z"},
    {"id": 2, "customer": "bob",   "product": "mouse",  "price": 29.99,  "ts": "2026-04-03T10:05:00Z"},
    {"id": 3, "customer": "alice", "product": "keyboard","price": 49.99, "ts": "2026-04-03T10:10:00Z"}
  ]' | jq

echo -e "\n=== Wait for batcher flush ==="
sleep 2

echo -e "\n=== Query: All records (PostgreSQL) ==="
curl -sf "$BASE/query?engine=postgres&q=SELECT+*+FROM+test_orders+ORDER+BY+id" | jq

echo -e "\n=== Query: Filter (PostgreSQL) ==="
curl -sf "$BASE/query?engine=postgres&q=SELECT+*+FROM+test_orders+WHERE+customer='alice'" | jq

echo -e "\n=== Query: Aggregate (ClickHouse) ==="
curl -sf "$BASE/query?engine=clickhouse&q=SELECT+customer,+count()+AS+cnt,+sum(price)+AS+total+FROM+test_orders+GROUP+BY+customer" | jq

echo -e "\n=== Verify ingest without schema fails ==="
curl -s -o /dev/null -w "Status: %{http_code} (expected 400)\n" \
  -X POST "$BASE/ingest?table=unknown" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1}]'

echo -e "\n=== Metrics ==="
curl -sf localhost:9090/metrics | grep "polygate_" | head -20

echo -e "\n=== Done ==="
```

Save as `test.sh` and run:
```bash
chmod +x test.sh
./test.sh
```

---

## Cleanup

```bash
# Stop polygate (Ctrl+C)

# Stop and remove all databases + data
docker compose down -v
```
