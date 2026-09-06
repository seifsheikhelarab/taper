package orderservice

// Saga state constants — the durable state machine persisted in saga_instances.
const (
	SagaPendingPayment = "PENDING_PAYMENT" // created, reservation held, awaiting payment
	SagaReserved       = "RESERVED"        // stock reserved, payment authorized next
	SagaAllocated      = "ALLOCATED"       // payment ok, stock allocated, confirming
	SagaConfirmed      = "CONFIRMED"       // terminal success
	SagaCompensated    = "COMPENSATED"     // terminal: stock released after failure/cancel
	SagaFailed         = "FAILED"          // terminal: unrecoverable error
)

// Order status constants mirrored on the orders row.
const (
	OrderPending     = "PENDING"
	OrderConfirmed   = "CONFIRMED"
	OrderFulfilled   = "FULFILLED"
	OrderCompensated = "COMPENSATED"
	OrderFailed      = "FAILED"
)

// compensationKey matches the Phase 1 convention (internal/reservationservice/domain.go)
// so compensation releases are not suppressed as duplicate idempotency keys.
func compensationKey(tenantID, orderID string) string {
	return "comp:" + tenantID + ":" + orderID
}
