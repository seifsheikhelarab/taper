-- name: UpsertAvailability :one
INSERT INTO availability_projection (
    tenant_id, sku_id, warehouse_id, available_qty
) VALUES (
    $1, $2, $3, $4
)
ON CONFLICT (tenant_id, sku_id, warehouse_id) DO UPDATE
SET available_qty = EXCLUDED.available_qty,
    updated_at = NOW()
RETURNING *;

-- name: GetAvailability :one
SELECT * FROM availability_projection
WHERE tenant_id = $1 AND sku_id = $2 AND warehouse_id = $3;

-- name: InsertAlert :one
INSERT INTO alerts (
    tenant_id, sku_id, warehouse_id, delta, reason, source
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING *;

-- name: CheckAndInsertIdempotencyKey :one
INSERT INTO processed_idempotency_keys (
    tenant_id, idempotency_key, payload_hash
) VALUES (
    $1, $2, $3
)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
RETURNING idempotency_key;

-- name: ListAlerts :many
SELECT * FROM alerts
WHERE tenant_id = $1
ORDER BY created_at DESC
LIMIT $2;
