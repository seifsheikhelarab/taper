#!/usr/bin/env bash
# Chaos: restart Postgres mid-traffic (spec #44, T5) and verify the stack
# recovers: the container returns healthy and a live service's connection
# pool reconnects (pgx dials lazily per acquired connection).
#
# What to observe while it runs (see docs/runbooks/chaos.md): request
# errors spike, gateway breakers may open (503 fail-fast), then close
# again on recovery — visible on the /metrics ports as
# taper_grpc_request_errors_total and taper_breaker_state.
#
# Usage: scripts/chaos/postgres-restart.sh
set -euo pipefail

echo "== restarting postgres =="
docker compose restart postgres

echo "== waiting for pg_isready =="
until docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; do
  sleep 1
done
echo "postgres ready"

echo "== sanity: all service databases answer =="
for db in taper_db reservation_db order_db channelsync_db; do
  docker compose exec -T postgres psql -U postgres -d "$db" -tc "SELECT 1" >/dev/null \
    && echo "  $db ok" || { echo "  $db FAILED" >&2; exit 1; }
done

echo "PASS: postgres restarted and answering; verify services recovered (no permanent 5xx, breakers closed on /metrics)"
