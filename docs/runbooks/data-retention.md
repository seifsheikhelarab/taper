# Data Retention Matrix (spec #52, US27)

Per table/topic: owner, window, mechanism, and notes. Windows marked
`ready-for-human` are policy decisions pending compliance sign-off — the
mechanism exists; the number needs a human.

## Databases

### All four DBs: `processed_idempotency_keys`

| Field | Value |
|---|---|
| Owner | platform on-call |
| Window | **7 days** (default; `IDEMPOTENCY_PRUNE_RETENTION`) |
| Mechanism | `pkg/idemprune` batched sweep, advisory-lock guarded, env-gated |
| Notes | must outlive client retry horizon + saga resume window (ADR-0002 analog) |

### All four DBs: `outbox`

| Field | Value |
|---|---|
| Owner | platform on-call |
| Window | 72h (default; `OUTBOX_PRUNE_RETENTION`) |
| Mechanism | `pkg/outboxprune` batched sweep (ADR-0002) |
| Notes | events are already delivered before pruning; Debezium has its own offset |

### taper_db: `stock_events`

| Field | Value |
|---|---|
| Owner | TBD (`ready-for-human`) |
| Window | TBD — currently **retained indefinitely** (audit trail) |
| Mechanism | none yet; candidate: partition by month, drop old partitions |
| Notes | immutable audit log; feeds reconciliation; contains no personal data (tenant UUID + SKU + actor id) |

### taper_db: `stock_levels`

| Field | Value |
|---|---|
| Owner | n/a — current business state |
| Window | never pruned |
| Mechanism | reconciliation corrects; rows live while the SKU exists |
| Notes | drift lock (`is_locked_for_audit`) blocks writes until cleared |

### order_db: `orders`, `saga_instances`, `order_lines`

| Field | Value |
|---|---|
| Owner | TBD (`ready-for-human`) |
| Window | TBD — order history is a business record; propose 7y (tax/commercial) |
| Mechanism | none yet; `ready-for-human` policy decision |
| Notes | **GDPR-relevant:** tenant_id links to customer data; order payloads contain no free-text personal fields today — keep it that way |

### channelsync_db: `availability_projection`, `alerts`

| Field | Value |
|---|---|
| Owner | platform on-call |
| Window | projection is derivable state (rebuild from events); alerts follow stock_events policy |
| Mechanism | projection: truncate-and-rebuild is always safe; alerts: none yet |
| Notes | no personal data |

## Kafka topics

| Topic | Window | Mechanism |
|---|---|---|
| `stock.events`, `reservation.events`, `order.events` | 7 days (`retention.ms`) | broker-level retention; consumers commit offsets independently |
| `*.dlq` | 30 days, compacted key | DLQ Envelopes kept for replay forensics (`cmd/dlqreplay`) |
| `connect_configs/offsets/statuses` | compacted, never pruned | Connect internals |

## GDPR notes (`ready-for-human`)

- Personal data today: none directly — tenant UUIDs are pseudonymous
  identifiers. Any future column holding names/emails/addresses must be
  added to this matrix with an erasure path.
- Erasure of a tenant's *facts* (orders, reservations) conflicts with the
  financial-retention requirement; the standard resolution (revoke +
  anonymize the tenant row, keep the transactional skeleton) is a policy
  decision — see `tenant-offboarding.md`.
