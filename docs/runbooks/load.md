# Load Testing Runbook

Phase 5 (spec #44, T4). k6 drives the **REST gateway** (the real external
surface: JWT auth + rate limiting included) at low-hundreds rps on a small
hot set. Correctness under load — the no-oversell invariant — is asserted by
`scripts/audit-oversell.sql`, not just latency numbers.

## Prerequisites

- Fresh-ish stack: `docker compose up -d postgres kafka connect` + migrations
  applied (see `docs/agents` or CI's "Apply migrations" step).
- Gateway running locally with a raised load-profile limit and a known
  secret:

  ```bash
  GATEWAY_RATE_PER_TENANT=500 GATEWAY_BURST_PER_TENANT=1000 \
  GATEWAY_JWT_SECRET=load-secret \
  STOCK_ADDR=localhost:50051 RESERVATION_ADDR=localhost:50052 \
  ORDER_ADDR=localhost:50053 go run ./cmd/gateway
  ```

- Services: `go run ./cmd/stock`, `./cmd/reservation`, `./cmd/order`
  (saga scenario B also needs them; A needs stock + reservation only).
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) on PATH.

## Seed the hot set

One tenant + one hot SKU with enough stock for the run (or less, if you
*want* to exercise the contention path):

```sql
-- As taper_app on taper_db, tenant 11111111-1111-4111-8111-111111111111:
INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
VALUES ('11111111-1111-4111-8111-111111111111', 'LOAD-SKU', 'W1', 100000)
ON CONFLICT (tenant_id, sku_id, warehouse_id)
DO UPDATE SET available_qty = EXCLUDED.available_qty;
```

## Run

```bash
# Scenario A: direct reserve path (pure contention on stock_db)
k6 run -e GATEWAY=http://localhost:8080 -e SECRET=load-secret \
  -e TENANT=11111111-1111-4111-8111-111111111111 -e RATE=200 \
  load/reserve-path.js

# Scenario B: full saga (payment sandbox + reservation + allocation)
k6 run -e GATEWAY=http://localhost:8080 -e SECRET=load-secret \
  -e TENANT=11111111-1111-4111-8111-111111111111 -e RATE=100 \
  load/saga-path.js
```

Thresholds fail the run when p95/p99 exceed 100ms or error rate ≥ 1%.

## Assert correctness (mandatory)

After the run drains (sweepers/fulfillment caught up):

```bash
docker exec -i taper-postgres psql -U postgres -d taper_db \
  -v on_error_stop=1 < scripts/audit-oversell.sql
```

Zero rows = no oversell, no silent loss. Any row is a **correctness bug**,
regardless of how pretty the latency numbers were.

## Results

Record one block per run (scenario, RATE, date, commit):

| Scenario | Rate | p50 | p95 | p99 | Errors | Oversell audit |
|---|---|---|---|---|---|---|
| _pending first recorded run_ | | | | | | |

The spec's goal is **p99 < 100ms** on the reservation path at low-hundreds
rps. Latency numbers without the oversell audit column are not results.
