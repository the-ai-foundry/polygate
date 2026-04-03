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
