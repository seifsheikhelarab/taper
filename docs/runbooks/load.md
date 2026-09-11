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
- Scenario B also needs the fulfillment consumer running (`go run
  ./cmd/fulfillment`) — it drives `CONFIRMED`; `run-load.sh` starts it.
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) on PATH.
  Run k6 **on the host**, not via Docker Desktop: the `host.docker.internal`
  proxy adds seconds per request on Windows and the numbers stop measuring
  the stack.

## Seed the hot set

One tenant + one hot SKU with enough stock for the run (or less, if you
*want* to exercise the contention path). The `run-load.sh` script also seeds
50 spread SKUs (`LOAD-SKU-0`..`LOAD-SKU-49`) for the realistic-traffic mode:

```sql
-- As taper_app on taper_db, tenant 11111111-1111-4111-8111-111111111111:
INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
VALUES ('11111111-1111-4111-8111-111111111111', 'LOAD-SKU', 'W1', 100000)
ON CONFLICT (tenant_id, sku_id, warehouse_id)
DO UPDATE SET available_qty = EXCLUDED.available_qty;

INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
SELECT '11111111-1111-4111-8111-111111111111', 'LOAD-SKU-' || g, 'W1', 100000
FROM generate_series(0, 49) g
ON CONFLICT (tenant_id, sku_id, warehouse_id)
DO UPDATE SET available_qty = EXCLUDED.available_qty;
```

**Two modes** (set `SPREAD_SKUS` when invoking k6, or via `run-load.sh`):

- `SPREAD_SKUS=50` (default): requests spread across 50 SKUs — realistic
  traffic; this is the mode whose numbers belong in the results table.
- `SPREAD_SKUS=1`: every request hits `LOAD-SKU` — deliberate single-row
  contention. One `SELECT FOR UPDATE` row serializes its transactions, so
  throughput caps near ~20–25 rps per row and latency grows linearly with
  arrival rate. Use it to *measure the ceiling*, never to judge the system.

## Run

```bash
# Scenario A: direct reserve path (spread across 50 SKUs)
k6 run -e GATEWAY=http://localhost:8080 -e SECRET=load-secret \
  -e TENANT=11111111-1111-4111-8111-111111111111 -e RATE=200 \
  -e SPREAD_SKUS=50 load/reserve-path.js

# Scenario B: full saga (payment sandbox + reservation + allocation)
k6 run -e GATEWAY=http://localhost:8080 -e SECRET=load-secret \
  -e TENANT=11111111-1111-4111-8111-111111111111 -e RATE=100 \
  -e SPREAD_SKUS=50 load/saga-path.js

# Scenario A contention-ceiling mode (single row; expect queues):
#   ... -e SPREAD_SKUS=1 load/reserve-path.js
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

First recorded runs (2026-09-11, commit with `feat(load): spread-SKU mode`,
Windows 11 + Docker Desktop, Postgres container on the 5433 secondary port,
`SPREAD_SKUS=50`, 60s each, all five binaries local, `synchronous_commit=on`):

| Scenario | Rate | p50 | p95 | p99 | Errors | Oversell audit |
|---|---|---|---|---|---|---|
| A: reserve path | 50 rps | 129ms | 1.55s | 3.51s | 0% | PASS (0 rows) |
| B: full saga | 20 rps | 199ms | 1.27s | 1.58s | 0% (sagas 100% CONFIRMED) | PASS (0 rows) |

Overload reference (same stack, 200 rps demanded): after the pool fix below
the system delivered ~117 rps with **0% errors** and p50 ≈ 2.1s; demand above
the fsync-bound ceiling turns into queueing latency, not failed requests —
and the no-oversell audit still passed during the pathological 78%-deadline-
abort run recorded before the fix.

### What the numbers actually measure

- **The bottleneck is Postgres commit fsync on the Docker Desktop VM disk,
  not application code.** Isolation probe at the same 50 rps with
  `synchronous_commit=off` (reverted immediately after): p50 129ms →
  **7.15ms**, p95 1.55s → **21.55ms**. Sequential reserves are ~40–50ms;
  a raw pgx ping over the same path is ~1.4ms.
- **pgx's default pool ceiling was a real bug found by these runs**:
  `pgxpool.New` sizes `MaxConns = max(4, numCPU)` = 8, capping stock at
  ~58 rps with seconds of `Acquire` queueing. Fixed by `database.OpenPool`
  (explicit 32, `DB_POOL_MAX_CONNS` override); capacity doubled.
- **Single-SKU mode is physics, not a defect**: one `SELECT FOR UPDATE` row
  serializes its transactions (~20–25 rps/row ceiling). Use `SPREAD_SKUS=1`
  only to measure that ceiling.
- The spec's p99 < 100ms goal is **not met on this Docker-Desktop-for-Windows
  environment** because every commit pays VM-disk fsync latency under
  concurrency. Re-record on Linux (native or WSL2 backend) before judging
  the application against the goal; the fsync probe above bounds the app's
  own contribution at roughly the 7ms/22ms class.

Latency numbers without the oversell audit column are not results.
