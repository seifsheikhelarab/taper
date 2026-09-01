package reservationservice

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
