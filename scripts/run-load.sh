#!/usr/bin/env bash
# Self-contained load run (spec #44, T4): builds and starts the service
# stack as background processes, seeds the hot set, runs the k6 scenario(s)
# via Docker, executes the mandatory no-oversell audit, and tears down.
#
# Usage:
#   scripts/run-load.sh [reserve|saga|both] [rate] [duration]
#
# Env overrides:
#   PG_PORT   container postgres host port (default 5433, the secondary
#             mapping, so a locally installed Postgres cannot shadow it)
#   SECRET    gateway sandbox JWT secret (default load-secret)
#   K6_IMAGE  k6 image (default grafana/k6:1.2.1)
set -euo pipefail

scenario="${1:-both}"
rate="${2:-200}"
duration="${3:-60s}"
pg_port="${PG_PORT:-5433}"
secret="${SECRET:-load-secret}"
k6_image="${K6_IMAGE:-grafana/k6:1.2.1}"
tenant="11111111-1111-4111-8111-111111111111"

cd "$(dirname "$0")/.."

command -v go >/dev/null || { echo "go required" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker required" >&2; exit 1; }

echo "== building binaries =="
tmp="$(mktemp -d)"
trap 'kill ${pids[*]} 2>/dev/null || true; rm -rf "$tmp"' EXIT
go build -o "$tmp" ./cmd/stock ./cmd/reservation ./cmd/order ./cmd/fulfillment ./cmd/gateway

pg="localhost:${pg_port}"
echo "== starting stack (postgres at ${pg}) =="
STOCK_DATABASE_URL="postgres://taper_app:taperapp@${pg}/taper_db" \
  STOCK_SWEEPER_DATABASE_URL="postgres://taper_sweeper:tapersweeper@${pg}/taper_db" \
  "$tmp/stock" &
pids+=($!)
RESERVATION_DATABASE_URL="postgres://taper_app:taperapp@${pg}/reservation_db" \
  SWEEPER_DATABASE_URL="postgres://taper_sweeper:tapersweeper@${pg}/reservation_db" \
  STOCK_ADDR=localhost:50051 "$tmp/reservation" &
pids+=($!)
ORDER_DATABASE_URL="postgres://taper_app:taperapp@${pg}/order_db" \
  ORDER_SWEEPER_DATABASE_URL="postgres://taper_sweeper:tapersweeper@${pg}/order_db" \
  RESERVATION_ADDR=localhost:50052 STOCK_ADDR=localhost:50051 "$tmp/order" &
pids+=($!)
# The fulfillment consumer drives CONFIRMED: without it scenario B's saga
# follow-up GET would hang in ALLOCATED forever.
KAFKA_BROKERS=localhost:29092 STOCK_ADDR=localhost:50051 "$tmp/fulfillment" &
pids+=($!)
GATEWAY_RATE_PER_TENANT=500 GATEWAY_BURST_PER_TENANT=1000 \
  GATEWAY_JWT_SECRET="$secret" STOCK_ADDR=localhost:50051 \
  RESERVATION_ADDR=localhost:50052 ORDER_ADDR=localhost:50053 "$tmp/gateway" &
pids+=($!)

echo "== waiting for gateway /healthz =="
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8080/healthz >/dev/null 2>&1; then break; fi
  sleep 0.5
done
curl -sf http://localhost:8080/healthz >/dev/null || { echo "gateway never became healthy" >&2; exit 1; }

echo "== seeding hot set =="
docker compose exec -T postgres psql -U postgres -d taper_db -c \
  "INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
   VALUES ('${tenant}', 'LOAD-SKU', 'W1', 100000)
   ON CONFLICT (tenant_id, sku_id, warehouse_id)
   DO UPDATE SET available_qty = EXCLUDED.available_qty, reserved_qty = 0, allocated_qty = 0" >/dev/null
# Spread SKUs (LOAD-SKU-0 .. LOAD-SKU-49): the realistic-traffic hot set used
# when SPREAD_SKUS>1; seeded regardless so either scenario mode works.
docker compose exec -T postgres psql -U postgres -d taper_db -c \
  "INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
   SELECT '${tenant}', 'LOAD-SKU-' || g, 'W1', 100000 FROM generate_series(0, 49) g
   ON CONFLICT (tenant_id, sku_id, warehouse_id)
   DO UPDATE SET available_qty = EXCLUDED.available_qty, reserved_qty = 0, allocated_qty = 0" >/dev/null

run_k6() {
  # Prefer native k6: it hits the gateway directly. The Docker fallback goes
  # through Docker Desktop's host.docker.internal proxy, which on Windows
  # adds seconds per request — its numbers measure the proxy, not the stack.
  local k6args=("run" "-e" "GATEWAY=http://localhost:8080" "-e" "SECRET=$secret" \
    "-e" "TENANT=$tenant" "-e" "RATE=$rate" "-e" "DURATION=$duration" \
    "-e" "SPREAD_SKUS=${SPREAD_SKUS:-50}" "load/$1")
  if command -v k6 >/dev/null 2>&1; then
    k6 "${k6args[@]}"
    return
  fi
  echo "WARN: native k6 not found; falling back to Docker (numbers include the Docker proxy)" >&2
  MSYS_NO_PATHCONV=1 docker run --rm --add-host=host.docker.internal:host-gateway \
    -e GATEWAY=http://host.docker.internal:8080 -e SECRET="$secret" \
    -e TENANT="$tenant" -e RATE="$rate" -e DURATION="$duration" \
    -e SPREAD_SKUS="${SPREAD_SKUS:-50}" \
    -v "$(pwd -W)/load:/scripts" "$k6_image" run "/scripts/$1"
}

case "$scenario" in
  reserve) run_k6 reserve-path.js || k6_rc=1 ;;
  saga)    run_k6 saga-path.js || k6_rc=1 ;;
  both)    run_k6 reserve-path.js || k6_rc=1; run_k6 saga-path.js || k6_rc=1 ;;
  *) echo "unknown scenario: $scenario" >&2; exit 1 ;;
esac

# The audit runs even when latency thresholds fail: correctness and latency
# are separate gates, and a failed run is exactly when you want the invariant
# checked.
echo "== no-oversell audit (mandatory) =="
audit_out="$(docker compose exec -T postgres psql -t -U postgres -d taper_db \
  -v on_error_stop=1 < scripts/audit-oversell.sql)"
echo "$audit_out"
violations="$(echo "$audit_out" | grep -c '[[:alnum:]]' || true)"
if [ "$violations" -gt 0 ]; then
  echo "FAIL: no-oversell invariant violated ($violations rows)" >&2
  exit 1
fi
echo "PASS: no oversell, no negative buckets"
exit "${k6_rc:-0}"
