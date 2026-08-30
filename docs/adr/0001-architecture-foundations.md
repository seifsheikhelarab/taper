# 0001 Architecture Foundations

**Status**: Accepted

We are building a multi-tenant distributed inventory management system. To guarantee stock correctness, low latency, and operational simplicity, we have made three foundational architecture decisions:

1. **Shared Tables with PostgreSQL Row-Level Security (RLS)**:
   - *Context & Decision*: We evaluated Database-per-Tenant, Schema-per-Tenant, and Shared Tables with RLS. We selected Shared Tables with PostgreSQL RLS for multi-tenancy isolation within each service.
   - *Why*: DB/Schema-per-tenant introduces massive schema migration overhead and connection pool exhaustion at scale. Postgres RLS enforces security at the engine layer without sacrificing query simplicity or CDC pipeline setup.

2. **Transactional Outbox Pattern & Debezium CDC for Kafka Events**:
   - *Context & Decision*: We evaluated Direct Dual-Writes (writing to DB then publishing to Kafka directly in application code) vs. Transactional Outbox + Debezium CDC. We selected Transactional Outbox tables per service watched by Debezium CDC.
   - *Why*: Dual-writes risk dual-write inconsistencies if the service crashes between DB commit and Kafka publish. Transactional Outbox guarantees that event records are committed atomically within the same DB transaction as business logic state mutations and emitted reliably to Kafka.
   - *Pruning*: Outbox retention is managed per service via either `pg_partman` 7-day range partitioning (for high-throughput services) OR a scheduled background worker executing batch deletions of records older than 3 days (for lower-volume services).

3. **Row-Locked Write Model (`stock_levels`) with Synchronous Audit Event Log (`stock_events`)**:
   - *Context & Decision*: We evaluated Full Event Sourcing (rebuilding state on-the-fly from events) vs. Row-locking on a primary write model + audit log. We selected row-level locking (`SELECT ... FOR UPDATE`) on `stock_levels` combined with a synchronous insert into `stock_events` within the same transaction.
   - *Why*: Full event sourcing adds latency and complexity to hot path stock reservation checks under high concurrency. Synchronous row locks on `stock_levels` provide sub-100ms p99 latency and strict no-oversell guarantees, while `stock_events` provides a complete audit trail.
   - *Audit Drift & Clearance*: If nightly reconciliation detects drift between `stock_levels` and `stock_events`, `stock_levels.is_locked_for_audit` is set to `true`, blocking new reservations. A warehouse manager clears the lock via the `UnlockStockForAudit` admin endpoint after reviewing the discrepancy and appending a `RECONCILIATION_CORRECTION` event.
