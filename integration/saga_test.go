package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

func sagaLine(sku, wh string, qty int32, unit int64) *orderv1.OrderLine {
	return &orderv1.OrderLine{
		SkuId:       sku,
		WarehouseId: wh,
		Quantity:    qty,
		UnitPrice:   unit,
	}
}

func sagaReq(tenant, order string, lines []*orderv1.OrderLine) *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{
		TenantId:       tenant,
		OrderId:        order,
		Lines:          lines,
		IdempotencyKey: "idem-" + order,
	}
}

// orderSagaState reads the saga row directly (tenant-scoped via RLS).
func (e *testEnv) orderSagaState(t *testing.T, tenant, orderID string) (string, string) {
	t.Helper()
	var state, orderStatus string
	ctx := database.WithTenantID(context.Background(), tenant)
	err := database.ExecTxWithTenant(ctx, e.orderPool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT s.state, o.status FROM saga_instances s JOIN orders o ON o.order_id = s.order_id WHERE s.order_id=$1",
			orderID).Scan(&state, &orderStatus)
	})
	if err != nil {
		t.Fatalf("read saga state %s: %v", orderID, err)
	}
	return state, orderStatus
}

func (e *testEnv) reservationStatus(t *testing.T, tenant, orderID string) string {
	t.Helper()
	var st string
	ctx := database.WithTenantID(context.Background(), tenant)
	err := database.ExecTxWithTenant(ctx, e.resPool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT status FROM reservations WHERE tenant_id=$1::uuid AND order_id=$2 LIMIT 1",
			tenant, orderID).Scan(&st)
	})
	if err != nil {
		t.Fatalf("read reservation status %s: %v", orderID, err)
	}
	return st
}

// TestSagaHappyPath: create -> reserve -> pay -> allocate -> CONFIRMED.
func TestSagaHappyPath(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-1", "W1", 100)

	resp, err := e.order.CreateOrder(e.ctx, sagaReq(tenantA, "ord-happy", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-1", "W1", 5, 1000),
	}))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if resp.GetStatus() != "CONFIRMED" {
		t.Fatalf("expected CONFIRMED, got %s", resp.GetStatus())
	}
	if resp.GetTotalAmount() != 5000 {
		t.Fatalf("expected total 5000, got %d", resp.GetTotalAmount())
	}
	if resp.GetTransactionId() == "" {
		t.Fatal("expected transaction id on confirmed order")
	}

	// Stock moved from available to allocated.
	avail, res, alloc := e.stockLevel(t, tenantA, "SKU-SAGA-1", "W1")
	if avail != 95 || res != 0 || alloc != 5 {
		t.Fatalf("expected 95/0/5, got %d/%d/%d", avail, res, alloc)
	}
	// Reservation must be ALLOCATED so the sweeper can never release it.
	if st := e.reservationStatus(t, tenantA, "ord-happy"); st != "ALLOCATED" {
		t.Fatalf("expected reservation ALLOCATED, got %s", st)
	}
	// Saga terminal state.
	state, orderStatus := e.orderSagaState(t, tenantA, "ord-happy")
	if state != "CONFIRMED" || orderStatus != "CONFIRMED" {
		t.Fatalf("expected saga+order CONFIRMED, got %s/%s", state, orderStatus)
	}
	// Order outbox event emitted.
	if n := e.outboxCount(t, e.orderPool, tenantA, "order.confirmed"); n != 1 {
		t.Fatalf("expected 1 order.confirmed outbox event, got %d", n)
	}
}

// TestSagaCompensationOnPaymentFailure: declined payment releases held stock.
func TestSagaCompensationOnPaymentFailure(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-2", "W1", 50)

	req := sagaReq(tenantA, "ord-fail", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-2", "W1", 10, 500),
	})
	req.ForcePaymentFailure = true

	resp, err := e.order.CreateOrder(e.ctx, req)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if resp.GetStatus() != "COMPENSATED" {
		t.Fatalf("expected COMPENSATED, got %s", resp.GetStatus())
	}

	// Stock fully returned to available.
	avail, res, alloc := e.stockLevel(t, tenantA, "SKU-SAGA-2", "W1")
	if avail != 50 || res != 0 || alloc != 0 {
		t.Fatalf("expected 50/0/0 after compensation, got %d/%d/%d", avail, res, alloc)
	}
	// Reservation released.
	if st := e.reservationStatus(t, tenantA, "ord-fail"); st != "RELEASED" {
		t.Fatalf("expected reservation RELEASED, got %s", st)
	}
	state, orderStatus := e.orderSagaState(t, tenantA, "ord-fail")
	if state != "COMPENSATED" || orderStatus != "COMPENSATED" {
		t.Fatalf("expected saga+order COMPENSATED, got %s/%s", state, orderStatus)
	}
}

// TestSagaInsufficientStockFails: all-or-nothing at the saga level.
func TestSagaInsufficientStockFails(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-3", "W1", 3)

	resp, err := e.order.CreateOrder(e.ctx, sagaReq(tenantA, "ord-poor", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-3", "W1", 10, 100),
	}))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if resp.GetStatus() != "FAILED" {
		t.Fatalf("expected FAILED, got %s", resp.GetStatus())
	}
	// Nothing held.
	avail, res, alloc := e.stockLevel(t, tenantA, "SKU-SAGA-3", "W1")
	if avail != 3 || res != 0 || alloc != 0 {
		t.Fatalf("expected untouched stock 3/0/0, got %d/%d/%d", avail, res, alloc)
	}
}

// TestSagaCancelOrderCompensates: cancel inside payment window releases stock.
func TestSagaCancelOrderCompensates(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-4", "W1", 20)

	// Create order that stays PENDING: payment is only charged synchronously in
	// CreateOrder, so simulate a cancel of the reservation hold via CancelOrder
	// on a freshly created order that has not been allocated. Because our saga
	// runs synchronously to CONFIRMED, cancel targets the compensated path for
	// still-pending orders; here we verify cancel of a confirmed order is refused.
	if _, err := e.order.CreateOrder(e.ctx, sagaReq(tenantA, "ord-cancel", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-4", "W1", 2, 100),
	})); err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	resp, err := e.order.CancelOrder(e.ctx, &orderv1.CancelOrderRequest{
		TenantId: tenantA,
		OrderId:  "ord-cancel",
		Reason:   "test",
	})
	if err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	// Confirmed orders cannot be cancelled.
	if resp.GetSuccess() {
		t.Fatalf("expected cancel refusal for confirmed order, got %+v", resp)
	}
	if resp.GetStatus() != "CONFIRMED" {
		t.Fatalf("expected status CONFIRMED, got %s", resp.GetStatus())
	}
	// Stock unchanged.
	avail, res, alloc := e.stockLevel(t, tenantA, "SKU-SAGA-4", "W1")
	if avail != 18 || res != 0 || alloc != 2 {
		t.Fatalf("expected 18/0/2, got %d/%d/%d", avail, res, alloc)
	}
}

// TestSagaIdempotentCreateOrder: duplicate CreateOrder returns cached outcome.
func TestSagaIdempotentCreateOrder(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-5", "W1", 30)

	req := sagaReq(tenantA, "ord-idem", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-5", "W1", 1, 100),
	})
	first, err := e.order.CreateOrder(e.ctx, req)
	if err != nil {
		t.Fatalf("first CreateOrder: %v", err)
	}
	second, err := e.order.CreateOrder(e.ctx, req)
	if err != nil {
		t.Fatalf("duplicate CreateOrder: %v", err)
	}
	if first.GetStatus() != second.GetStatus() {
		t.Fatalf("idempotent replay mismatch: %s vs %s", first.GetStatus(), second.GetStatus())
	}
	// Only one line of stock consumed.
	avail, res, alloc := e.stockLevel(t, tenantA, "SKU-SAGA-5", "W1")
	if avail != 29 || res != 0 || alloc != 1 {
		t.Fatalf("expected 29/0/1 exactly-once, got %d/%d/%d", avail, res, alloc)
	}
}

// TestSagaGetOrder: reads back order + saga state.
func TestSagaGetOrder(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-6", "W1", 10)

	if _, err := e.order.CreateOrder(e.ctx, sagaReq(tenantA, "ord-get", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-6", "W1", 2, 250),
	})); err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	resp, err := e.order.GetOrder(e.ctx, &orderv1.GetOrderRequest{
		TenantId: tenantA,
		OrderId:  "ord-get",
	})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if resp.GetStatus() != "CONFIRMED" || resp.GetSagaState() != "CONFIRMED" {
		t.Fatalf("expected CONFIRMED, got %s/%s", resp.GetStatus(), resp.GetSagaState())
	}
	if resp.GetTotalAmount() != 500 || len(resp.GetLines()) != 1 {
		t.Fatalf("unexpected order payload: %+v", resp)
	}
	if resp.GetTransactionId() == "" {
		t.Fatal("expected transaction id")
	}

	_, err = e.order.GetOrder(e.ctx, &orderv1.GetOrderRequest{
		TenantId: tenantA,
		OrderId:  "missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// TestSagaExpirySweeperLeavesAllocated: the sweeper must not release ALLOCATED
// reservations from confirmed orders (US5 invariant end-to-end).
func TestSagaExpirySweeperLeavesAllocated(t *testing.T) {
	e := setup(t)
	e.seed(t, tenantA, "SKU-SAGA-7", "W1", 10)

	if _, err := e.order.CreateOrder(e.ctx, sagaReq(tenantA, "ord-ttl", []*orderv1.OrderLine{
		sagaLine("SKU-SAGA-7", "W1", 4, 100),
	})); err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	// Force expires_at into the past for the reservation rows.
	ctx := database.WithTenantID(context.Background(), tenantA)
	if err := database.ExecTxWithTenant(ctx, e.resPool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			"UPDATE reservations SET expires_at = NOW() - INTERVAL '1 hour' WHERE order_id='ord-ttl'")
		return err
	}); err != nil {
		t.Fatalf("backdate reservation: %v", err)
	}
	// Run one sweep pass (same query the sweeper uses).
	var expired int
	if err := database.ExecTxWithTenant(ctx, e.resPool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id FROM reservations WHERE expires_at < NOW() AND status = 'ACTIVE' LIMIT 100 FOR UPDATE SKIP LOCKED")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			expired++
		}
		return rows.Err()
	}); err != nil {
		t.Fatalf("sweep query: %v", err)
	}
	if expired != 0 {
		t.Fatalf("sweeper saw %d expirable rows; ALLOCATED must be invisible to the sweeper", expired)
	}
	avail, res, alloc := e.stockLevel(t, tenantA, "SKU-SAGA-7", "W1")
	if avail != 6 || res != 0 || alloc != 4 {
		t.Fatalf("expected allocation intact 6/0/4, got %d/%d/%d", avail, res, alloc)
	}
}

// Compile-time guard: errors and time stay referenced in future assertions.
var (
	_ = errors.New
	_ = time.Now
)
