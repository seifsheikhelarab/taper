-- name: GetStockLevelForUpdate :one
SELECT * FROM stock_levels
WHERE tenant_id = $1 AND sku_id = $2 AND warehouse_id = $3
FOR UPDATE;

-- name: UpdateStockLevel :one
UPDATE stock_levels
SET available_qty = $4,
    reserved_qty = $5,
    allocated_qty = $6,
    is_locked_for_audit = $7,
    updated_at = $8
WHERE tenant_id = $1 AND sku_id = $2 AND warehouse_id = $3
RETURNING *;

-- name: InsertStockEvent :one
INSERT INTO stock_events (
    tenant_id,
    sku_id,
    warehouse_id,
    delta,
    reason,
    source,
    actor_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING *;

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

-- name: UpsertStockLevel :one
INSERT INTO stock_levels (
    tenant_id,
    sku_id,
    warehouse_id,
    available_qty
) VALUES (
    $1, $2, $3, $4
)
ON CONFLICT (tenant_id, sku_id, warehouse_id) DO UPDATE
SET available_qty = stock_levels.available_qty + EXCLUDED.available_qty,
    updated_at = NOW()
RETURNING *;

-- name: SetAuditLock :exec
UPDATE stock_levels
SET is_locked_for_audit = $4,
    updated_at = NOW()
WHERE tenant_id = $1 AND sku_id = $2 AND warehouse_id = $3;
