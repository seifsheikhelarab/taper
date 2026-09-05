# 0002 Outbox Pruning Strategy

**Status**: Accepted

Per ADR-0001, each service owns its outbox retention: `pg_partman` daily range partitioning for high-throughput services, or a scheduled background worker executing batch deletions of records older than 3 days for lower-volume services. This ADR records which option each service uses and why.

## Decision

| Service | Throughput class | Pruning mechanism |
|---------|-----------------|-------------------|
| Stock (`taper_db`) | High (every reservation, release, allocation, external adjustment appends events) | `pkg/outboxprune` scheduled worker, batched deletes, 3-day retention |
| Reservation (`reservation_db`) | Medium (create/release/expire per order) | `pkg/outboxprune` scheduled worker, batched deletes, 3-day retention |
| Order (`order_db`) | Medium (one saga emits ~4-6 events per order) | `pkg/outboxprune` scheduled worker, batched deletes, 3-day retention |

## Rationale

- **Worker over `pg_partman` for now**: partitioning requires converting `outbox` to a partitioned table (migration + Debezium snapshot coordination). Current volumes are far below the threshold where batched deletes hurt; the worker keeps the schema simple and CDC stable. Revisit `pg_partman` for the stock service when outbox inserts exceed roughly 10k rows/day sustained.
- **3-day retention** matches ADR-0001 and gives downstream consumers a generous replay window before rows disappear.
- **Never concurrently**: the worker takes an advisory lock per database (`pg_try_advisory_lock`) so two workers (e.g. a service restart overlapping the old process) cannot prune at the same time.
- **Batched deletes** (`LIMIT` per pass in a short transaction) keep row locks small so Debezium's WAL reader never stalls behind a long pruning transaction.

## Consequences

- Services embed the worker as a background goroutine behind an env toggle (`OUTBOX_PRUNE_ENABLED`), so local dev can disable it.
- If Debezium lags more than 3 days, events can be lost before streaming — monitoring should alert on `Debeziumconnector` millisecond lag well before retention expires.
