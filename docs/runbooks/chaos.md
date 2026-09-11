# Chaos Runbook

Phase 5 (spec #44, T5). Kill real infrastructure and real service processes;
require the system's invariants to survive. **The invariant** (ADR-0001):
every seeded unit stays accounted for — after recovery and quiescence:

```
available + allocated == seeded   &&   reserved == 0
```

No orphaned holds, no lost stock, no oversell — regardless of where the
failure landed.

## Go chaos tests (stock process kills)

Gated behind `CHAOS=1` because they kill real subprocesses (never per-push):

```bash
CHAOS=1 go test -v -run TestChaos ./integration/ -timeout 12m
```

- `TestChaosStockDownFailsFast` — stock dead before traffic: the saga
  fails immediately (Fail-Fast Policy), zero partial state.
- `TestChaosKillStockMidSaga` — stock killed **after the saga's first
  durable write** (guaranteed mid-saga), then restarted. Convergence is
  asserted via the order service's crash-recovery resume loop
  (`SAGA_RESUME_ENABLED=1`) with the reservation TTL sweeper as the
  independent backstop. Observed on 2026-09-11 @ 6c7b31c: the killed
  saga's reservation transaction rolled back atomically with the process
  (`available=10 reserved=0 allocated=0, saga=FAILED` for a 10-unit seed).

Both tests target the compose postgres on host port **5433** (secondary
mapping) so a locally installed Postgres cannot shadow the container.

## Infrastructure restarts

With the compose stack up and services running:

```bash
scripts/chaos/kafka-restart.sh     # broker bounce: groups rejoin, offsets preserved
scripts/chaos/postgres-restart.sh  # DB bounce: pools reconnect, breakers close again
```

Watch during the run:

- `taper_grpc_request_errors_total` / `taper_breaker_state` on the
  `/metrics` ports (9101–9106) — errors spike, breakers may open, then
  close on recovery.
- Consumer-group lag returns to 0 after the Kafka restart
  (`kafka-consumer-groups.sh --describe` output in the script).
- After Postgres restarts, the next request through each service
  reconnects transparently (pgx lazily dials per acquired connection).

## Assert invariants afterwards

```bash
docker exec -i taper-postgres psql -U postgres -d taper_db \
  -v on_error_stop=1 < scripts/audit-oversell.sql
```

Zero rows = no oversell, no negative buckets — the same mandatory gate as
after load runs.
