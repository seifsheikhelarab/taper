# Distributed Inventory Management System — Technical Specification

## 1. Overview

Multi-tenant inventory management platform handling stock tracking, reservations, and order fulfillment across multiple warehouses per tenant. Built as a set of independently deployable services communicating via gRPC (synchronous) and Kafka (asynchronous), with strong consistency guarantees on stock mutations and eventual consistency on reporting/analytics.

Target scale: single-digit thousands of tenants, low-hundreds of concurrent reservation requests per second at peak, sub-100ms p99 latency on reservation checks.

## 2. Goals / Non-Goals

**Goals**
- Correct stock counts under concurrent writes (no overselling)
- Multi-warehouse stock visibility with per-warehouse reservation
- Auditable history of every stock mutation (event sourced)
- Async integrations (Shopify, WooCommerce) without blocking core writes
- Horizontal scalability of read paths and worker pools

**Non-Goals (v1)**
- Multi-region active-active deployment (single-region, single primary DB per shard)
- Real-time analytics dashboards (batch reporting is acceptable)
- Automated demand forecasting / reorder ML

## 3. Service Decomposition

| Service | Responsibility | Owns |
|---|---|---|
| Product Service | Product/SKU catalog, variants, pricing | `products`, `skus` |
| Stock Service | Per-warehouse stock levels, adjustments | `stock_levels`, `stock_events` |
| Reservation Service | Reservation lifecycle, locking, expiry | `reservations` |
| Order Service | Order lifecycle, orchestrates reservation → fulfillment | `orders`, `order_lines` |
| Warehouse Service | Warehouse metadata, transfer requests | `warehouses`, `transfers` |
| Sync Service | Inbound/outbound integrations (Shopify, WooCommerce) | `sync_jobs`, `webhook_log` |
| Notification Service | Low-stock alerts, reorder reminders | `alert_rules`, `alert_log` |
| Reporting Service | Read-optimized aggregates (turnover, aging) | materialized views, CQRS read store |

Each service owns its schema. No service reads another's tables directly; cross-service reads go through gRPC calls or subscribe to Kafka topics and maintain a local read replica of the data they need.

## 4. Data Ownership & Consistency Model

**Strong consistency required:**
- Stock reservation (no overselling) — this is the one place where correctness trumps availability
- Order line item pricing at time of order

**Eventual consistency acceptable:**
- Reporting aggregates (turnover, low-stock dashboards)
- Search/catalog read replicas
- Notification delivery

### 4.1 Reservation correctness

Two approaches, pick one and document the tradeoff in the spec review:

**Option A — Row-level locking (simpler, recommended for v1)**
- `stock_levels` table has `available_qty`, `reserved_qty` columns per (sku_id, warehouse_id)
- Reservation Service issues `SELECT ... FOR UPDATE` inside a transaction, checks `available_qty >= requested_qty`, decrements available, increments reserved, commits
- Works within Stock Service's own database; no distributed lock needed since it's a single-service, single-DB operation
- Contention bottleneck: hot SKUs during flash sales serialize on the row lock. Mitigate with `SELECT ... FOR UPDATE SKIP LOCKED` on batched requests or by sharding hot SKUs across synthetic sub-rows (e.g., splitting a 1000-unit stock row into 10x100-unit shards, routing requests round-robin)

**Option B — Distributed lock via Redis (only if Stock Service itself becomes multi-node with local caches)**
- Redlock or a single Redis instance with `SET NX PX` for lock acquisition on `lock:stock:{sku_id}:{warehouse_id}`
- Adds a network hop and a failure mode (lock service down = reservations blocked). Only justified if Option A's DB becomes the bottleneck after sharding

Default to Option A. It's simpler, has fewer failure modes, and Postgres row locks scale further than people assume at this traffic tier.

### 4.2 Saga: Order → Reservation → Fulfillment

Order placement spans three services and must not use a distributed transaction (2PC is operationally expensive and doesn't fit this stack). Use an orchestrated saga:

```
Order Service (orchestrator)
  1. Create order (status: PENDING)
  2. Call Reservation Service: reserve(order_id, lines[])
     - success -> order.status = RESERVED
     - failure -> order.status = FAILED, emit order.failed event, stop
  3. Call Payment (external/mocked): charge(order_id)
     - success -> order.status = PAID
     - failure -> compensate: release reservation, order.status = FAILED
  4. Emit order.confirmed event
  5. Async: Warehouse Service picks up order.confirmed, creates pick/pack task
```

Each step has an explicit compensating action. Reservation Service exposes both `reserve()` and `release()`. Order Service tracks saga state in an `order_saga_state` column (`PENDING → RESERVED → PAID → CONFIRMED`, or `FAILED` at any step with a `failure_reason`).

Reservations have a TTL (default 15 min, configurable per tenant). A background job in Reservation Service sweeps expired reservations and releases stock, emitting `reservation.expired` so Order Service can transition orphaned orders to `FAILED`.

### 4.3 Event sourcing for stock mutations

`stock_events` is append-only: every adjustment (sale, reservation, release, manual correction, transfer in/out, sync from Shopify) is a row: `(event_id, sku_id, warehouse_id, delta, reason, source, created_at, actor_id)`. `stock_levels.available_qty` is a materialized projection, recomputed by folding events, and reconciled nightly (sum of events should equal current level; alert on drift).

This gives full audit trail and makes stock corrections trivial to reason about, at the cost of an extra write per mutation. Acceptable given this isn't a >10k writes/sec system.

## 5. Communication Patterns

**Synchronous (gRPC):**
- Order Service → Reservation Service (reserve/release) — needs immediate success/failure
- Order Service → Product Service (price lookup at order time)
- Any service → Warehouse Service (metadata lookups)

**Asynchronous (Kafka, one topic per domain event):**
- `stock.adjusted`, `stock.low` — Stock Service publishes, Notification Service + Reporting Service subscribe
- `order.confirmed`, `order.failed` — Order Service publishes, Warehouse Service + Reporting Service subscribe
- `reservation.expired` — Reservation Service publishes, Order Service subscribes
- `sync.product_updated` — Sync Service publishes after pulling from Shopify, Product Service subscribes

Topic partitioning: partition by `tenant_id` for all topics to preserve per-tenant ordering while allowing parallelism across tenants. Consumer groups per service, one group per topic.

**Idempotency:** every Kafka consumer and every gRPC write endpoint accepts/derives an idempotency key (`event_id` for Kafka, client-supplied `Idempotency-Key` header for gRPC/REST). Consumers store processed keys in a dedup table with a retention window (e.g., 7 days) and short-circuit on replay.

## 6. Data Layer

| Store | Used by | Purpose |
|---|---|---|
| PostgreSQL (per-service DB) | all services | primary transactional store |
| Read replicas (Postgres streaming replication) | Reporting Service, Product Service catalog reads | offload read traffic |
| Redis | Reservation Service (optional lock), Sync Service (rate-limit tokens for Shopify API), API gateway (session/rate limiting) | caching, ephemeral coordination |
| Kafka | all services | event bus, CDC-style propagation |
| Elasticsearch (optional, phase 2) | Product Service catalog search | full-text/faceted search |

CDC note: rather than dual-writing to Postgres and Kafka, use Debezium on each service's Postgres instance (via logical replication / WAL) to publish row changes to Kafka. This guarantees the event stream reflects committed DB state and avoids the dual-write consistency problem where a service crashes after writing to Postgres but before publishing to Kafka.

## 7. API Design

External API: REST (via API Gateway, e.g., Express/Bun gateway service) fronting internal gRPC services. Gateway handles auth (JWT, tenant resolution), rate limiting, request routing.

Internal APIs: gRPC with protobuf schemas versioned in a shared `proto/` repo, imported by each service. Breaking changes require a new proto package version; services must support N-1 compatibility during rollout.

Example reservation RPC:

```protobuf
service ReservationService {
  rpc Reserve(ReserveRequest) returns (ReserveResponse);
  rpc Release(ReleaseRequest) returns (ReleaseResponse);
}

message ReserveRequest {
  string tenant_id = 1;
  string order_id = 2;
  string idempotency_key = 3;
  repeated ReservationLine lines = 4;
  int32 ttl_seconds = 5;
}

message ReservationLine {
  string sku_id = 1;
  string warehouse_id = 2;
  int32 quantity = 3;
}

message ReserveResponse {
  bool success = 1;
  string reservation_id = 2;
  repeated string failed_sku_ids = 3; // populated on partial failure
}
```

## 8. Tech Stack

- **Services:** TypeScript/Bun + Express for Product, Order, Notification, Sync, Reporting. Go (chi) for Stock and Reservation Service — the two services under the tightest latency/correctness constraints benefit from Go's simpler concurrency model and lower GC pause variance under lock contention.
- **RPC:** gRPC (protobuf) between services
- **ORM/DB access:** Prisma (TypeScript services), sqlc (Go services)
- **Migrations:** Prisma Migrate / goose
- **Message bus:** Kafka (Redpanda acceptable as a lighter-weight drop-in for local dev/smaller deployments)
- **CDC:** Debezium
- **Cache/locks:** Redis
- **Containerization:** Docker, docker-compose for local dev
- **Orchestration:** Kubernetes (k3s acceptable for a portfolio-scale cluster)
- **Observability:** OpenTelemetry SDK in every service → Jaeger (tracing), Prometheus (metrics), Loki or ELK (logs)
- **API Gateway:** Hono/Bun with JWT auth middleware, or Kong/Envoy if you want to demonstrate gateway-as-infra rather than hand-rolled

## 9. Deployment Topology

- One Postgres instance per service (can colocate on shared hardware for portfolio scale, logically separated by database/schema, physically separated in the writeup)
- One Kafka cluster (3 brokers minimum for replication factor 3 in a "production-shaped" setup, 1 broker acceptable for local/demo)
- Each service: 2+ replicas behind a k8s Service, HPA on CPU/request latency
- API Gateway: 2+ replicas, public-facing LoadBalancer/Ingress
- Debezium Connect cluster: 1-2 workers pulling from each service's Postgres WAL

## 10. Observability & Failure Handling

- **Tracing:** every request gets a trace ID at the gateway, propagated through gRPC metadata and Kafka message headers, so a single order's full path (gateway → order service → reservation service → stock service → kafka → warehouse service) is visible in Jaeger as one trace.
- **Dead-letter queues:** every Kafka consumer has a DLQ topic (`{topic}.dlq`); after N retries with exponential backoff, message goes to DLQ with the error and is not silently dropped. Manual replay tooling for DLQ messages.
- **Circuit breakers:** gRPC calls between services wrapped with a circuit breaker (fail fast after threshold, half-open retry) to avoid cascading failure when Stock Service is degraded.
- **Reconciliation job:** nightly job diffs `stock_events` sum against `stock_levels.available_qty` per SKU/warehouse, alerts on drift beyond a threshold.

## 11. Phased Build Plan

**Phase 1 (weeks 1-3): Core transactional path**
- Product Service, Stock Service, Reservation Service (Option A locking)
- Single Postgres per service, synchronous gRPC only, no Kafka yet
- Goal: correct reservation under concurrent load, verified with a load test script hammering the same SKU

**Phase 2 (weeks 4-6): Order saga + async backbone**
- Order Service with saga orchestration and compensation
- Kafka + Debezium wired in, stock.adjusted / order.confirmed events flowing
- Warehouse Service consuming order.confirmed

**Phase 3 (weeks 7-9): Integrations + reporting**
- Sync Service (Shopify inbound webhook + outbound push)
- Reporting Service with materialized views fed by Kafka consumers
- Notification Service for low-stock alerts

**Phase 4 (weeks 10-12): Hardening**
- DLQs, circuit breakers, tracing end-to-end
- Load testing (k6 or similar) against reservation path, document p50/p95/p99 latencies
- Chaos testing: kill Stock Service mid-reservation, verify saga compensation fires correctly

## 12. What This Demonstrates

- Correct handling of concurrent writes under contention (the hard part interviewers actually probe)
- Saga pattern with real compensating transactions, not just a diagram
- Event sourcing + CDC instead of dual-writes
- Service boundaries drawn around data ownership, not just "microservices for the sake of it"
- A documented, justified choice between two consistency strategies (Option A vs B) rather than defaulting to the trendier distributed-lock approach without reason