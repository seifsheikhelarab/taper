-- name: InsertReservation :one
INSERT INTO reservations (
    tenant_id,
    order_id,
    sku_id,
    warehouse_id,
    quantity,
    status,
    expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING *;

-- name: UpdateReservationStatus :one
UPDATE reservations
SET status = $2
WHERE id = $1
RETURNING *;

-- name: GetExpiredReservations :many
SELECT * FROM reservations
WHERE expires_at < $1 AND status = 'ACTIVE'
LIMIT $2
FOR UPDATE SKIP LOCKED;

-- name: GetActiveReservationsByOrder :many
SELECT * FROM reservations
WHERE order_id = $1 AND status = 'ACTIVE'
ORDER BY id
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

-- name: GetReservation :one
SELECT * FROM reservations
WHERE id = $1;
