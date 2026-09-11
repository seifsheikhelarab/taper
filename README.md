# Taper — Multi-Tenant Distributed Inventory Management System

**Taper** is a high-performance, multi-tenant distributed inventory management system designed for enterprise-grade stock tracking, reservations, order fulfillment, and multi-channel synchronization across multiple warehouses per tenant.

Built in **Go**, Taper balances strong consistency for stock reservation paths (preventing overselling under high concurrency) with eventual consistency for asynchronous downstream workflows using gRPC, PostgreSQL Row-Level Security (RLS), the Transactional Outbox pattern, Debezium CDC, and Kafka.

---

## 📋 Table of Contents

- [Overview & Core Architecture](#overview--core-architecture)
- [Key Features & Design Principles](#key-features--design-principles)
- [Domain Model & Inventory States](#domain-model--inventory-states)
- [System Architecture & Services](#system-architecture--services)
- [Data Consistency & Reliability Patterns](#data-consistency--reliability-patterns)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Documentation & Architecture Decisions](#documentation--architecture-decisions)

---

## 💡 Overview & Core Architecture

In multi-tenant e-commerce and retail ecosystems, stock management faces two primary challenges:
1. **Preventing Overselling**: High-concurrency events (e.g., flash sales) can cause race conditions if stock deduction is not strongly consistent.
2. **System Resilience & Scalability**: Synchronous dual-writes to databases and message queues lead to partial failure states. Microservices must communicate cleanly without tight coupling or two-phase commits (2PC).

Taper addresses these challenges through:
- **Fast-Write Row-Level Locking**: Per-SKU per-warehouse stock updates utilize PostgreSQL `SELECT ... FOR UPDATE` row locks combined with synchronous audit event appending (`stock_events`).
- **Orchestrated Order Saga**: An explicit Saga manager coordinates `Order Service` $\rightarrow$ `Reservation Service` $\rightarrow$ `Payment Gateway` $\rightarrow$ `Stock Service` with compensating transactions.
- **Transactional Outbox + Debezium CDC**: State mutations and outbox events are committed atomically within local transactions, avoiding dual-write bugs. Debezium streams events from Postgres WAL directly into Kafka.
- **Multi-Tenant Isolation**: Tenant data is isolated at the database level using PostgreSQL Row-Level Security (RLS) and multi-tenant connection parameters.

---

## ✨ Key Features & Design Principles

- 🔐 **Multi-Tenant Isolation via RLS**: Shared database tables with engine-level row isolation by `tenant_id` ([ADR-0001](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/docs/adr/0001-architecture-foundations.md)).
- ⚡ **Zero-Oversell Stock Reservation**: Sub-100ms p99 reservation checks with row-level locks on stock rows.
- 🔄 **Order Saga Orchestration**: Compensation logic for failed payments or stock reservations with explicit state tracking (`PENDING` $\rightarrow$ `RESERVED` $\rightarrow$ `PAID` $\rightarrow$ `CONFIRMED` or `FAILED`).
- ⏱️ **TTL Reservation Sweeper**: Automatic release of expired stock reservations via a background sweeper goroutine.
- 📦 **Transactional Outbox & CDC**: Zero dual-write inconsistencies; outbox records are streamed to Kafka via Debezium Connect connectors.
- 🛡️ **Outbox Retention & Pruning**: Safe background worker ([`pkg/outboxprune`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/pkg/outboxprune)) with Postgres advisory locks and batched deletions ([ADR-0002](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/docs/adr/0002-outbox-pruning.md)).
- 🚨 **Audit Locking & Reconciliation**: Detection of stock count discrepancies automatically locks SKU rows (`is_locked_for_audit = true`) until manual audit clearance via gRPC endpoints.
- 🔁 **Dead-Letter Queue (DLQ) & Replay**: Standardized DLQ message encapsulation ([`pkg/streaming`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/pkg/streaming)) and CLI tool ([`cmd/dlqreplay`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/dlqreplay/main.go)) for message recovery.
- 🔌 **External Channel Sync**: Clamped stock sync from third-party channels (Shopify, WooCommerce) with deficit reporting.

---

## 🏷️ Domain Model & Inventory States

Taper defines explicit inventory state transitions to enforce strict domain boundaries:

```
[ Available Stock ] --(Reserve)--> [ Reserved Stock ] --(Pay & Allocate)--> [ Allocated Stock ] --(Dispatch)--> [ Fulfilled Stock ]
         ^                                |
         |-------(TTL Expire/Release)-----|
```

| Term | Definition |
|---|---|
| **Available Stock** | Inventory ready for new reservations per SKU and warehouse. |
| **Reserved Stock** | Stock held temporarily for an active order during payment processing (subject to TTL expiry). |
| **Allocated Stock** | Stock committed to confirmed, paid orders; TTL expiry can no longer release it. |
| **Fulfilled Stock** | Stock physically shipped, reducing allocated count and physical balance. |
| **Audit Lock** | An explicit flag (`is_locked_for_audit`) blocking reservation attempts on SKUs with reconciliation drift. |

*For complete domain terminology, see [`CONTEXT.md`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/CONTEXT.md).*

---

## 🏗️ System Architecture & Services

Taper follows a microservices architecture where each service owns its database schema:

```
                      +-------------------+
                      |   gRPC Clients    |
                      +---------+---------+
                                |
        +-----------------------+-----------------------+
        |                       |                       |
        v                       v                       v
+---------------+       +---------------+       +---------------+
| Order Service |       |  Reservation  |       | Stock Service |
|  (cmd/order)  |       | (cmd/reserv.) |       |  (cmd/stock)  |
+-------+-------+       +-------+-------+       +-------+-------+
        |                       |                       |
   [order_db]           [reservation_db]            [taper_db]
        |                       |                       |
        | outbox                | outbox                | outbox
        v                       v                       v
+---------------------------------------------------------------+
|                      Debezium CDC Connect                     |
+-------------------------------+-------------------------------+
                                |
                                v
+---------------------------------------------------------------+
|                       Apache Kafka Bus                        |
+--------+----------------------+-----------------------+-------+
         |                      |                       |
         v                      v                       v
+------------------+  +-------------------+   +------------------+
|   Fulfillment    |  |    Channel Sync   |   |   DLQ Replay     |
|(cmd/fulfillment) |  |(cmd/channelsync)  |   | (cmd/dlqreplay)  |
+------------------+  +-------------------+   +------------------+
```

### Services Summary

- 🛒 **Order Service** ([`cmd/order`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/order/main.go), [`internal/orderservice`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/internal/orderservice)): Saga orchestrator managing order state transitions, invoking Reservation and Stock services via gRPC with circuit breaking ([`pkg/circuitbreaker`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/pkg/circuitbreaker)).
- ⏳ **Reservation Service** ([`cmd/reservation`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/reservation/main.go), [`internal/reservationservice`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/internal/reservationservice)): Manages order line item reservations, holds, releases, and background TTL expiration ([`sweeper.go`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/internal/reservationservice/sweeper.go)).
- 📊 **Stock Service** ([`cmd/stock`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/stock/main.go), [`internal/stockservice`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/internal/stockservice)): Controls warehouse stock counts (`stock_levels`), logs audit entries (`stock_events`), executes allocations, and locks SKUs for audit reconciliation.
- 📦 **Fulfillment Service** ([`cmd/fulfillment`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/fulfillment/main.go), [`internal/fulfillment`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/internal/fulfillment)): Consumes `order.confirmed` Kafka events and manages order pick tickets and dispatching.
- 🔄 **Channel Sync Service** ([`cmd/channelsync`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/channelsync/main.go), [`internal/channelsync`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/internal/channelsync)): Syncs inventory levels with external channels (e.g. Shopify), clamping negative numbers and emitting deficit alerts.
- 🛠️ **DLQ Replay Utility** ([`cmd/dlqreplay`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/cmd/dlqreplay/main.go)): CLI utility to read failed payloads from Dead Letter Queue Kafka topics and re-inject them into main event topics.

---

## 🔒 Data Consistency & Reliability Patterns

### 1. No-Oversell Fast Write Model
Stock updates execute within a single PostgreSQL transaction:
```sql
SELECT available_qty, reserved_qty 
FROM stock_levels 
WHERE tenant_id = $1 AND sku_id = $2 AND warehouse_id = $3 
FOR UPDATE;
```
If `available_qty >= requested_qty`, the quantities are adjusted, an immutable event is inserted into `stock_events`, and outbox events are appended—all committed in one transaction.

### 2. Transactional Outbox + Debezium CDC
Services write domain events to an `outbox` table during business operations. Debezium watches Postgres WAL logs via logical replication and pushes events directly to Kafka:
- Config files: [`db/connect/stock-outbox.json`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/db/connect/stock-outbox.json), [`db/connect/order-outbox.json`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/db/connect/order-outbox.json), [`db/connect/reservation-outbox.json`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/db/connect/reservation-outbox.json).

### 3. Outbox Retention Worker
The [`pkg/outboxprune`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/pkg/outboxprune) worker runs as an in-process background goroutine per service. It acquires a PostgreSQL advisory lock (`pg_try_advisory_lock`) to safely execute batched deletions (`DELETE ... LIMIT`) on processed outbox rows older than 3 days without locking Debezium CDC streams.

---

## 🛠️ Tech Stack

- **Language**: Go 1.26+
- **RPC & Serialization**: gRPC, Protocol Buffers (`buf`)
- **Database**: PostgreSQL 16+ with Row-Level Security (RLS)
- **Database Drivers & Code Generation**: `pgx/v5`, `sqlc`
- **Messaging & Event Streaming**: Apache Kafka ([`segmentio/kafka-go`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/go.mod)), Debezium CDC Connect
- **Containerization**: Docker & Docker Compose

---

## 📁 Repository Structure

```
.
├── cmd/                        # Application entry points
│   ├── stock/                  # Stock service daemon
│   ├── reservation/            # Reservation service daemon
│   ├── order/                  # Order saga orchestrator daemon
│   ├── fulfillment/            # Fulfillment consumer daemon
│   ├── channelsync/            # Channel sync daemon
│   └── dlqreplay/              # DLQ replay CLI utility
├── internal/                   # Service-private domain logic & gRPC implementations
│   ├── stockservice/           # Stock levels, adjustments & audit locking
│   ├── reservationservice/     # Reservation lifecycle & TTL sweeper
│   ├── orderservice/           # Order saga state machine & handlers
│   ├── fulfillment/            # Fulfillment processing
│   └── channelsync/            # External channel synchronization logic
├── pkg/                        # Reusable system packages & infrastructure drivers
│   ├── database/               # PostgreSQL connection & RLS helpers
│   ├── outboxprune/            # Advisory-locked outbox pruning worker
│   ├── circuitbreaker/         # gRPC client circuit breaker
│   ├── payment/                # Payment gateway integration interface
│   ├── streaming/              # Kafka consumer loops, DLQ wrappers & envelopes
│   └── channel/                # Sales channel sync abstractions
├── proto/                      # Protobuf schema definitions
├── db/                         # Database scripts, migrations, queries, and CDC configs
│   ├── init/                   # Multi-database init SQL & role creation
│   ├── migrations/             # sql-migrate / golang-migrate schema files
│   ├── queries/                # sqlc raw SQL query definitions
│   └── connect/                # Debezium Kafka Connect JSON definitions
├── docs/                       # Architecture decisions & domain guidelines
│   ├── adr/                    # Architecture Decision Records (ADRs)
│   └── agents/                 # Contributor and agent documentation
├── docker-compose.yml          # Local environment setup (Postgres, Kafka, Debezium)
├── CONTEXT.md                  # Complete domain glossary & context specifications
└── idea.md                     # Initial technical specification draft
```

---

## 🚀 Getting Started

### Prerequisites

- **Go**: 1.26 or higher
- **Docker & Docker Compose**
- **Buf CLI** (optional, for regenerating protobuf code)

### 1. Launch Infrastructure Stack

Start PostgreSQL databases, Kafka, Zookeeper, and Debezium Connect:

```bash
docker-compose up -d
```

This starts:
- **Postgres** (ports `5432-5436` mapped for `taper_db`, `reservation_db`, `order_db`, `fulfillment_db`, `channelsync_db`)
- **Kafka** (port `9092`)
- **Debezium Connect** (port `8383`)
- **pgAdmin** (port `5050`)

### 2. Run Database Migrations

Apply database schemas per service:

```bash
# Example applying migrations using your preferred migration runner (e.g. migrate / goose)
migrate -path db/migrations/stock -database "postgres://stock_user:stock_pass@localhost:5432/taper_db?sslmode=disable" up
migrate -path db/migrations/reservation -database "postgres://res_user:res_pass@localhost:5433/reservation_db?sslmode=disable" up
migrate -path db/migrations/order -database "postgres://order_user:order_pass@localhost:5434/order_db?sslmode=disable" up
```

### 3. Run Microservices

Start individual services locally:

```bash
# Start Stock Service
go run ./cmd/stock

# Start Reservation Service
go run ./cmd/reservation

# Start Order Service
go run ./cmd/order
```

---

## 📚 Documentation & Architecture Decisions

For detailed architectural rationale and design choices, refer to:
- 📖 [`CONTEXT.md`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/CONTEXT.md): Domain vocabulary, state definitions, and invariant rules.
- 📐 [`docs/adr/0001-architecture-foundations.md`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/docs/adr/0001-architecture-foundations.md): RLS multi-tenancy, outbox CDC, and row-locking model.
- 📐 [`docs/adr/0002-outbox-pruning.md`](file:///C:/Users/niteinheaven/Desktop/Data/Code/taper/docs/adr/0002-outbox-pruning.md): Advisory-locked outbox retention worker design.
