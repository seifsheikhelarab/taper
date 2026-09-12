# Incident Response Runbook (spec #52, US26)

For the paged-on-call engineer. Companion docs: `slo.md` (what pages mean),
`backup-restore.md` (Postgres recovery), `chaos.md` (how failures were
rehearsed).

## Severity ladder

| Sev | Definition | Response |
|---|---|---|
| SEV1 | Oversell possible, data corruption, or full outage of the order path | page immediately, all hands |
| SEV2 | Order path degraded (sagas failing, breakers open) or one consumer down | page on-call, 15 min ack |
| SEV3 | Single replica unhealthy, background job failures with retries | ticket next business day |

SEV1 triggers the invariant audit (`scripts/audit-oversell.sql`) **before**
any fix is declared done.

## First 5 minutes (any page)

1. **Who is hurt?** `taper_grpc_request_errors_total` by `method` and `code`
   on the gateway — is it one tenant, one method, or everything?
2. **Breakers:** `taper_breaker_state` — any dependency at 2 (open)?
3. **Readiness:** `up` / `/readyz` per service — who is not ready?
4. **Logs:** structured JSON (stdout) carries `trace_id`; grab one from a
   failing request and open it in Jaeger to see which hop failed.
5. **Declare** severity in the incident channel before fixing anything.

## Plays

### Postgres down / unhealthy

- Signature: every service's `/readyz` fails `postgres:*`; breakers on all
  order-path dependencies open.
- Check: `kubectl -n taper get pods -l app=postgres` (or
  `docker compose ps postgres`), then `pg_isready`.
- Restart is safe (idempotency + saga resume make replay exactly-once);
  prefer failover to a restored copy only via `backup-restore.md`.

### Kafka down / consumer stalled

- Signature: fulfillment/channelsync `/readyz` fails `kafka-*`; stock and
  order paths stay green; outbox grows.
- Kafka restart is safe: at-least-once with DLQ envelopes; consumers replay
  from committed offsets. Watch lag: `kafka-consumer-groups --describe`.
- If the outbox table grows unbounded: the prune worker is env-gated
  (`OUTBOX_PRUNE_ENABLED`) — check it is on and its logs.

### Downstream gRPC service down (stock / reservation / order)

- Signature: `TaperBreakerOpen` on that dependency; gateway returns 503
  (Fail-Fast Policy, CONTEXT.md) instead of queuing.
- Restart the failing service; probes must flip ready before traffic
  resumes. In-flight saga steps are idempotently keyed (`<key>:reserve`,
  `:charge`, `:allocate`, `:confirm`), so replays are exactly-once.

### Draining for maintenance

- Planned: flip the dependency off (or `SetDegraded(true)` on the affected
  service), watch upstream drain (`/readyz` 503 stops routing), do the
  work, restore. Liveness (`/healthz`) stays 200 throughout — that is the
  point.

## Comms template

```
[SEVn] <one-line impact> — <time UTC>
Impact: <who/what is affected, quantified>
State:  <breakers/readiness snapshot>
Action: <current step> (owner: <name>)
Next:   <next update at +15m or sooner>
```

## Post-incident

- Run `scripts/audit-oversell.sql` and attach output.
- Timeline from structured logs (JSON, trace-correlated) + alerts.
- Blameless writeup within 48h; action items as tickets.
