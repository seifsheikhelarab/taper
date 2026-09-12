# Backup & Restore Runbook — shared Postgres (spec #52, US24)

Single Postgres instance holds **every** service database: `taper_db`
(stock), `reservation_db`, `order_db`, `channelsync_db`. A storage-layer
loss touches all of them at once; this is the recoverability story.

> **This runbook is the difference between "we have a PVC" and "we can
> recover."** The PVC in `deploy/postgres.yaml` is not a backup.

## Targets (defaults until an owner revises — flag `ready-for-human`)

| Target | Value | Rationale |
|---|---|---|
| RPO | 5 min | WAL archiving cadence; stock mutations are high-value |
| RTO | 30 min | restore + verify + repoint services |
| Backup cadence | nightly base + continuous WAL | PITR to any point in the last N days |
| Retention | 14 days of PITR, 4 monthly base backups | balances storage vs. forensic depth |

## Nightly base backup

```bash
# Per-database granularity: each service DB restores independently.
for db in taper_db reservation_db order_db channelsync_db; do
  docker compose exec -T postgres pg_dump -Fc -d "$db" \
    > "backups/${db}-$(date +%F).dump"
done
```

On Kubernetes (statefulset pod name from `kubectl -n taper get pods -l app=postgres`):

```bash
kubectl -n taper exec postgres-0 -- \
  pg_dump -Fc -d order_db > backups/order_db-$(date +%F).dump
```

## WAL archiving (continuous)

Compose/dev: enable archiving in `postgres`'s command:

```
-c archive_mode=on -c archive_command='test ! -f /wal_archive/%f && cp %p /wal_archive/%f'
```

with a mounted `/wal_archive` volume, then ship the archive directory
off-host (rsync/S3) on a timer. Production: run the same flags in
`deploy/postgres.yaml` and ship from the mount, or use a sidecar
(wal-g/pgBackRest) — the flag surface is identical.

## Point-in-time recovery (PITR)

1. Stop all service binaries (drain: `TAPER_DRAIN` already bounds it).
2. Restore the nearest base backup **before** the target time into a fresh
   data directory; create `recovery.signal` with `restore_command` pulling
   WAL up to the target (`recovery_target_time`).
3. Start Postgres; verify recovery reached the target:
   `SELECT pg_last_wal_replay_lsn();`
4. Before repointing services, run the verification suite below.

## Restore verification (never skip)

```bash
# Invariant audit (CONTEXT.md Unit Accounting): available + allocated ==
# seeded, reserved == 0 after quiescence.
psql -d taper_db -f scripts/audit-oversell.sql

# Row counts per DB vs. the pre-incident snapshot counts.
psql -d order_db -c "SELECT count(*) FROM orders;"
psql -d reservation_db -c "SELECT count(*) FROM reservations;"
```

Then repoint services (or restart them) and watch `/readyz` flip to 200
before re-admitting traffic.
