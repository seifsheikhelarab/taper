package reservationservice

import "github.com/jackc/pgx/v5/pgtype"

// Reservation status constants (US5: centralized instead of scattered literals).
const (
	StatusActive    = "ACTIVE"
	StatusAllocated = "ALLOCATED"
	StatusReleased  = "RELEASED"
	StatusExpired   = "EXPIRED"
)

// compensationKey generates a distinct idempotency key for compensation-release
// calls so they are not treated as duplicates by the stock service.
func compensationKey(tenantID, orderID string) string {
	return "comp:" + tenantID + ":" + orderID
}

// partitionKey builds the Composite Partition Key (CONTEXT.md) used as the
// outbox aggregate id: tenant_id:entity_id. Debezium keys Kafka messages by
// this value, guaranteeing per-entity ordering while balancing partitions.
func partitionKey(tenantUUID pgtype.UUID, entityID string) string {
	return tenantUUID.String() + ":" + entityID
}
