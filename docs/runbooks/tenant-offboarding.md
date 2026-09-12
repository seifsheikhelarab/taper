# Tenant Offboarding Runbook (spec #52, US28)

Purges a tenant's rows from every service database. All queries run as the
**taper_sweeper** role (BYPASSRLS): the app role's RLS shows only
`app.current_tenant_id`, and the point of offboarding is to run without
impersonating the departing tenant.

> Cross-reference: `data-retention.md` decides *what must go*; this
> document is the *how*.

## What survives, and why

| Table | Purged? | Why |
|---|---|---|
| `orders` / `saga_instances` / `order_lines` | **anonymized, not deleted** | financial/transactional retention (`ready-for-human` policy pending); tenant FK-like reference replaced by a tombstone UUID |
| `reservations` | purged (terminal-state rows) or anonymized per policy above | operational only |
| `stock_levels` | n/a — SKU-scoped, not tenant-private rows | shared inventory rows keyed by tenant_id UUID; delete only if the tenant owned dedicated stock |
| `stock_events` | **retained** | immutable audit log; reconciliation depends on full derivation; contains no personal data |
| `outbox` | purged by normal retention (72h) | no action needed |
| `processed_idempotency_keys` | purged by normal retention (7d) | no action needed |
| `availability_projection` / `alerts` (channelsync) | purged | derived state, rebuildable |

## Procedure

For `<TENANT>` substitute the departing tenant's UUID. Run per database.

### 1. Freeze the tenant (stop new writes)

The gateway rejects tokens of removed tenants only if issuing stops; first
stop minting tokens for `<TENANT>` at the issuer, then confirm drain:

```sql
-- order_db: confirm no non-terminal sagas remain (must return 0 rows)
SELECT order_id, state FROM saga_instances
WHERE tenant_id = '<TENANT>'
  AND state IN ('PENDING_PAYMENT', 'RESERVED');
```

If rows remain: compensate or await TTL/saga-resume before continuing.

### 2. Purge derived state

```sql
-- channelsync_db (as taper_sweeper)
DELETE FROM availability_projection WHERE tenant_id = '<TENANT>';
DELETE FROM alerts               WHERE tenant_id = '<TENANT>';
DELETE FROM processed_idempotency_keys WHERE tenant_id = '<TENANT>';
```

### 3. Purge operational state

```sql
-- reservation_db (as taper_sweeper)
DELETE FROM reservations WHERE tenant_id = '<TENANT>';
DELETE FROM processed_idempotency_keys WHERE tenant_id = '<TENANT>';

-- taper_db (as taper_sweeper): reservations of dedicated stock, if any
DELETE FROM stock_levels WHERE tenant_id = '<TENANT>';  -- only dedicated rows
```

### 4. Anonymize the transactional record

```sql
-- order_db (as taper_sweeper): tombstone the tenant reference, keep the
-- transactional skeleton for financial retention.
UPDATE orders
SET tenant_id = gen_random_uuid()  -- break the link to the former identity
WHERE tenant_id = '<TENANT>';
-- Repeat for saga_instances and order_lines in the same transaction.
```

Wrap steps 2–4 per database in one transaction each (`BEGIN; ... COMMIT;`)
so a partial offboard cannot hide.

### 5. Verify

```sql
-- Must return 0 everywhere (as taper_sweeper):
SELECT count(*) FROM reservation_db.reservations          WHERE tenant_id = '<TENANT>';
SELECT count(*) FROM channelsync_db.availability_projection WHERE tenant_id = '<TENANT>';
SELECT count(*) FROM order_db.saga_instances              WHERE tenant_id = '<TENANT>';
-- order_db.orders must show only tombstoned rows:
SELECT count(*) FROM order_db.orders WHERE tenant_id = '<TENANT>';
```

### 6. Kafka

Events already published retain the tenant UUID in keys/payloads until
topic retention (7 days) expires. For immediate erasure, offboard **after**
retention lapses or re-create affected topics (consumers rebuild
projection state from sources; DLQs replay nothing tenant-specific unless
envelopes remain — inspect with `cmd/dlqreplay inspect` first).
