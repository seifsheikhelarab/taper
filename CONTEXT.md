# Distributed Inventory Management Context

Core domain terminology and concepts for the multi-tenant distributed inventory management system.

## Inventory States

**Available Stock**:
Inventory in a warehouse ready for new reservations.
_Avoid_: Free stock, unreserved stock

**Reserved Stock**:
Inventory held temporarily for an order line item during the payment window, subject to TTL expiry.
_Avoid_: Soft reservation, held stock

**Allocated Stock**:
Inventory assigned to an order pick ticket following confirmed payment; TTL expiry can no longer release it.
_Avoid_: Hard reservation, committed stock

**Fulfilled Stock**:
Inventory that has physically departed the warehouse upon order dispatch, clearing allocated count and total physical stock.
_Avoid_: Shipped stock, deducted stock

## Mutability & Events

**Stock Level**:
The per-SKU, per-warehouse count acting as the fast write model for lock checks and stock validation.
_Avoid_: Inventory balance, stock projection

**Stock Event**:
An immutable audit log record created synchronously alongside every stock level update in the same database transaction.
_Avoid_: Inventory delta, stock transaction log

**Outbox Event**:
A domain event record written to a service's transactional outbox table within the local database transaction, streamed asynchronously to Kafka by Debezium. Pruning is managed per service via either `pg_partman` daily partitioning or a scheduled cleanup worker (never concurrently).
_Avoid_: CDC raw log, direct dual-write event

**Audit Lock**:
An explicit flag (`is_locked_for_audit`) placed on a SKU's stock level row when reconciliation drift is detected, blocking new reservations until cleared via the `UnlockStockForAudit` admin endpoint.
_Avoid_: SKU freeze, inventory block

**DLQ Envelope**:
The standardized wrapper containing failed Kafka message payload, retry metadata, and stack trace for dead-letter queues.
_Avoid_: Error log record, message dead-letter wrapper

## Reliability & Orchestration

**Order Saga**:
The orchestrated workflow across Order, Reservation, and Payment services with explicit compensating transactions.
_Avoid_: Distributed transaction, two-phase commit

**Stock Allocation**:
The synchronous gRPC operation in the order saga transitioning stock from Reserved to Allocated post-payment before confirming the order.
_Avoid_: Stock claim, fulfillment reservation

**Fail-Fast Policy**:
The circuit breaker behavior that immediately returns HTTP 503 error responses when downstream dependencies are unreachable, avoiding request queuing.
_Avoid_: Request buffering, offline queueing

**External Sync Adjustment**:
A stock mutation initiated by external sales channels (Shopify/WooCommerce), clamped to 0 if negative with an urgent deficit alert.
_Avoid_: Direct override, uncapped negative balance

**Idempotency Record**:
An in-database record (`processed_idempotency_keys`) checked and inserted within local transactions to guarantee exact-once execution.
_Avoid_: Cache token, Redis deduplication key

**Composite Partition Key**:
A Kafka message key (`tenant_id:entity_id`) ensuring message ordering per SKU or per Order while balancing traffic across partition workers.
_Avoid_: Tenant-only partition key

**Tenant**:
A distinct client entity whose data and requests are isolated using PostgreSQL Row-Level Security.
_Avoid_: Account, client, customer workspace
