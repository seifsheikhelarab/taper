package reservationservice

import "github.com/jackc/pgx/v5/pgtype"

// Reservation status constants (US5: centralized instead of scattered literals).
const (
	StatusActive    = "ACTIVE"
	StatusAllocated = "ALLOCATED"
	StatusReleased  = "RELEASED"
	StatusExpired   = "EXPIRED"
)

// partitionKey builds the Composite Partition Key (CONTEXT.md) used as the
// outbox aggregate id: tenant_id:entity_id. Debezium keys Kafka messages by
// this value, guaranteeing per-entity ordering while balancing partitions.
func partitionKey(tenantUUID pgtype.UUID, entityID string) string {
	return tenantUUID.String() + ":" + entityID
}
