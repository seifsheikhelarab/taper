# Taper on Kubernetes (spec #52, US23)

Plain manifests mirroring the docker-compose topology; compose remains the
dev path. Apply order matters (config first, stateful data, then services):

```bash
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/config.yaml
kubectl apply -f deploy/postgres.yaml
kubectl apply -f deploy/kafka.yaml
kubectl apply -f deploy/services.yaml
kubectl apply -f deploy/monitoring.yaml
```

Sizing/replica notes:

- The gateway and every gRPC dial use client-side `round_robin`
  (`pkg/grpcx`), so scaling a Service's Deployment (`stock: 2+`) spreads
  RPCs per-call without a proxy hop — same as `docker compose --scale`.
- The reservation TTL sweeper, stock reconciliation, saga resume, and the
  outbox/idempotency prunes are advisory-lock-guarded or idempotently keyed,
  so running 2+ replicas is safe (prompt-stop semantics live in
  `pkg/closer`; idempotency makes re-running safe, per ADR-0001/0002).
- Postgres is the single store for all four databases (taper_db,
  reservation_db, order_db, channelsync_db) on a PVC. Before any production
  rollout, read `docs/runbooks/backup-restore.md` and schedule the
  backup/PITR tooling — the PVC is not a backup.
- Kafka runs KRaft (no ZooKeeper), single-node as in compose; for HA, run 3
  brokers and raise the replication factors in `kafka.yaml`.
- Probes: `/healthz` is static liveness, `/readyz` is dependency-aware
  readiness (Postgres pings, downstream gRPC reach, Kafka dials). Drain
  behavior is governed by `TAPER_DRAIN` (default 15s).
- TLS: set `GRPC_TLS_CERT`/`GRPC_TLS_KEY` (and `GRPC_TLS_CA` on clients) via
  Secrets for the in-cluster TLS posture; `GATEWAY_TLS_CERT`/`KEY` terminate
  HTTPS at the gateway. See `.env.example` for the full surface.
