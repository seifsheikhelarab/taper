-- name: UpsertOrder :one
INSERT INTO orders (
    tenant_id,
    order_id,
    status,
    total_amount,
    currency
) VALUES (
    $1, $2, $3, $4, $5
)
ON CONFLICT (tenant_id, order_id) DO NOTHING
RETURNING *;

-- name: GetOrderById :one
SELECT * FROM orders
WHERE order_id = $1;

-- name: UpdateOrderStatus :one
UPDATE orders
SET status = $2,
    transaction_id = $3,
    updated_at = NOW()
WHERE order_id = $1
RETURNING *;

-- name: InsertOrderLine :one
INSERT INTO order_lines (
    tenant_id,
    order_id,
    sku_id,
    warehouse_id,
    quantity,
    unit_price
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING *;

-- name: GetOrderLines :many
SELECT * FROM order_lines
WHERE order_id = $1
ORDER BY sku_id, warehouse_id;

-- name: UpsertSagaInstance :one
INSERT INTO saga_instances (
    tenant_id,
    order_id,
    state,
    step,
    idempotency_key
) VALUES (
    $1, $2, $3, $4, $5
)
ON CONFLICT (tenant_id, order_id) DO NOTHING
RETURNING *;

-- name: GetSagaInstance :one
SELECT * FROM saga_instances
WHERE order_id = $1;

-- name: UpdateSagaState :one
UPDATE saga_instances
SET state = $2,
    step = $3,
    error = $4,
    updated_at = NOW()
WHERE order_id = $1
RETURNING *;

-- name: GetResumableSagas :many
SELECT * FROM saga_instances
WHERE state IN ('PENDING_PAYMENT', 'RESERVED')
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: InsertOutboxEvent :one
INSERT INTO outbox (
    tenant_id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING *;

-- name: CheckAndInsertIdempotencyKey :one
INSERT INTO processed_idempotency_keys (
    tenant_id,
    idempotency_key,
    payload_hash
) VALUES (
    $1, $2, $3
)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
RETURNING idempotency_key;
