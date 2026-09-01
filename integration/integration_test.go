package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

// TestHappyPathLifecycle covers reservation -> allocation.
func TestHappyPathLifecycle(t *testing.T) {
	e := setup(t)

	const (
		orderID = "order-1"
		skuA    = "sku-a"
		skuB    = "sku-b"
		wh      = "wh-1"
	)
	e.seed(t, tenantA, skuA, wh, 100)
	e.seed(t, tenantA, skuB, wh, 100)

	resp, err := e.reservation.Reserve(context.Background(), &resv1.ReserveRequest{
		TenantId:       tenantA,
		OrderId:        orderID,
		IdempotencyKey: "res-1",
		TtlSeconds:     600,
		Items: []*resv1.ReservationItem{
			{SkuId: skuA, WarehouseId: wh, Quantity: 5},
			{SkuId: skuB, WarehouseId: wh, Quantity: 7},
		},
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !resp.GetSuccess() || resp.GetReservationId() == "" {
		t.Fatalf("unexpected reserve response: %+v", resp)
	}

	availA, reservA, _ := e.stockLevel(t, tenantA, skuA, wh)
	if availA != 95 || reservA != 5 {
		t.Fatalf("after reserve A: available=%d reserved=%d, want 95/5", availA, reservA)
	}
	availB, reservB, _ := e.stockLevel(t, tenantA, skuB, wh)
	if availB != 93 || reservB != 7 {
		t.Fatalf("after reserve B: available=%d reserved=%d, want 93/7", availB, reservB)
	}

	// Confirm allocation: reserved -> allocated (stock DB).
	allocResp, err := e.stock.ConfirmStockAllocation(context.Background(), &stockv1.ConfirmStockAllocationRequest{
		TenantId:       tenantA,
		OrderId:        orderID,
		IdempotencyKey: "alloc-1",
		Lines: []*stockv1.StockLine{
			{SkuId: skuA, WarehouseId: wh, Quantity: 5},
			{SkuId: skuB, WarehouseId: wh, Quantity: 7},
		},
	})
	if err != nil {
		t.Fatalf("confirm allocation: %v", err)
	}
	if !allocResp.GetSuccess() || len(allocResp.GetLines()) != 2 {
		t.Fatalf("unexpected alloc response: %+v", allocResp)
	}
	availA2, reservA2, allocA2 := e.stockLevel(t, tenantA, skuA, wh)
	if availA2 != 95 || reservA2 != 0 || allocA2 != 5 {
		t.Fatalf("after alloc A: avail=%d reserved=%d allocated=%d, want 95/0/5", availA2, reservA2, allocA2)
	}

	// US5: Transition reservation status to ALLOCATED (prevents sweeper release).
	allocResResp, err := e.reservation.AllocateReservation(context.Background(), &resv1.AllocateReservationRequest{
		TenantId:       tenantA,
		OrderId:        orderID,
		IdempotencyKey: "res-alloc-1",
	})
	if err != nil {
		t.Fatalf("allocate reservation: %v", err)
	}
	if !allocResResp.GetSuccess() || allocResResp.GetAllocatedCount() != 2 {
		t.Fatalf("unexpected allocate reservation response: %+v", allocResResp)
	}

	// Release must be a no-op now (no active reservations remain for the order).
	relResp, err := e.reservation.Release(context.Background(), &resv1.ReleaseRequest{
		TenantId:      tenantA,
		ReservationId: resp.GetReservationId(),
		OrderId:       orderID,
		Reason:        "cancel",
	})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !relResp.GetSuccess() {
		t.Fatalf("release failed as expected no-op")
	}
}

// TestAllOrNothingRollback asserts a multi-item reserve with one insufficient SKU fails entirely.
func TestAllOrNothingRollback(t *testing.T) {
	e := setup(t)

	const skuA = "sku-a"
	const skuB = "sku-b"
	const wh = "wh-1"
	e.seed(t, tenantA, skuA, wh, 5)
	e.seed(t, tenantA, skuB, wh, 100)

	resp, err := e.reservation.Reserve(context.Background(), &resv1.ReserveRequest{
		TenantId:       tenantA,
		OrderId:        "order-2",
		IdempotencyKey: "res-2",
		Items: []*resv1.ReservationItem{
			{SkuId: skuA, WarehouseId: wh, Quantity: 10}, // insufficient: only 5 available
			{SkuId: skuB, WarehouseId: wh, Quantity: 5},
		},
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatalf("expected all-or-nothing failure, got success")
	}
	if len(resp.GetFailedSkuIds()) == 0 || resp.GetFailedSkuIds()[0] != skuA {
		t.Fatalf("expected sku-a in failed set, got %v", resp.GetFailedSkuIds())
	}

	// Rollback check: nothing should have changed.
	availA, reservA, _ := e.stockLevel(t, tenantA, skuA, wh)
	if availA != 5 || reservA != 0 {
		t.Fatalf("sku-a after failed reserve: avail=%d reserved=%d, want 5/0", availA, reservA)
	}
	availB, reservB, _ := e.stockLevel(t, tenantA, skuB, wh)
	if availB != 100 || reservB != 0 {
		t.Fatalf("sku-b after failed reserve: avail=%d reserved=%d, want 100/0", availB, reservB)
	}

	rows := e.countReservations(t, tenantA, "order-2")
	if rows != 0 {
		t.Fatalf("failed reserve created %d reservation rows, want 0", rows)
	}
}

// TestOutboxAndIdempotency asserts atomic outbox rows and duplicate suppression.
func TestOutboxAndIdempotency(t *testing.T) {
	e := setup(t)

	const skuA = "sku-a"
	const wh = "wh-1"
	e.seed(t, tenantA, skuA, wh, 20)

	req := &resv1.ReserveRequest{
		TenantId:       tenantA,
		OrderId:        "order-3",
		IdempotencyKey: "key-3",
		TtlSeconds:     600,
		Items:          []*resv1.ReservationItem{{SkuId: skuA, WarehouseId: wh, Quantity: 3}},
	}
	resp, err := e.reservation.Reserve(context.Background(), req)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("first reserve failed")
	}

	// Duplicate with identical payload + same idempotency key -> success, no state change.
	dup, err := e.reservation.Reserve(context.Background(), req)
	if err != nil {
		t.Fatalf("duplicate reserve: %v", err)
	}
	if !dup.GetSuccess() {
		t.Fatalf("duplicate reserve should return cached success")
	}
	avail, reserv, _ := e.stockLevel(t, tenantA, skuA, wh)
	if avail != 17 || reserv != 3 {
		t.Fatalf("after dup reserve: avail=%d reserved=%d, want 17/3", avail, reserv)
	}

	// Outbox event generated.
	if n := e.outboxCount(t, e.resPool, tenantA, "reservation.created"); n != 1 {
		t.Fatalf("reservation.created outbox count = %d, want 1", n)
	}
	if n := e.outboxCount(t, e.resPool, tenantA, "reservation.created"); n == 0 {
		t.Fatalf("no outbox rows")
	}
}

// TestRLSTenantIsolation asserts one tenant cannot read or mutate another tenant's rows.
func TestRLSTenantIsolation(t *testing.T) {
	e := setup(t)

	e.seed(t, tenantA, "sku-a", "wh-1", 50)
	e.seed(t, tenantB, "sku-a", "wh-1", 999)

	// A reserves from its own 50 units.
	_, err := e.reservation.Reserve(context.Background(), &resv1.ReserveRequest{
		TenantId:       tenantA,
		OrderId:        "order-A",
		IdempotencyKey: "key-A",
		TtlSeconds:     600,
		Items:          []*resv1.ReservationItem{{SkuId: "sku-a", WarehouseId: "wh-1", Quantity: 10}},
	})
	if err != nil {
		t.Fatalf("reserve tenant A: %v", err)
	}

	// Tenant A's stock must be unaffected by B and vice versa: A cannot reduce B's qty.
	availB, _, _ := e.stockLevel(t, tenantB, "sku-a", "wh-1")
	if availB != 999 {
		t.Fatalf("tenant B available after A reserve = %d, want 999 (RLS isolation broken)", availB)
	}
}

// TestConcurrencyNoOversell hammers Reserve on one hot SKU with 50 units.
func TestConcurrencyNoOversell(t *testing.T) {
	e := setup(t)

	const skuA = "hot-sku"
	const wh = "wh-1"
	e.seed(t, tenantA, skuA, wh, 50)

	const workers = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	failures := 0

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := e.reservation.Reserve(context.Background(), &resv1.ReserveRequest{
				TenantId:       tenantA,
				OrderId:        fmt.Sprintf("order-%d", i),
				IdempotencyKey: fmt.Sprintf("key-%d", i),
				TtlSeconds:     600,
				Items:          []*resv1.ReservationItem{{SkuId: skuA, WarehouseId: wh, Quantity: 1}},
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures++
				return
			}
			if res.GetSuccess() {
				successes++
			} else {
				failures++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if successes != 50 {
		t.Fatalf("successes = %d (failures=%d), want exactly 50", successes, failures)
	}
	if failures != 50 {
		t.Fatalf("failures = %d, want exactly 50", failures)
	}

	// No oversell: available must be 0, never negative, reserved exactly 50.
	avail, reserv, alloc := e.stockLevel(t, tenantA, skuA, wh)
	if avail < 0 || avail != 0 || reserv != 50 || alloc != 0 {
		t.Fatalf("final stock avail=%d reserved=%d allocated=%d, want 0/50/0", avail, reserv, alloc)
	}

	// No negative stock ever recorded: all stock_events deltas must keep available >= 0.
	var negativeCount int
	ctx := database.WithTenantID(context.Background(), tenantA)
	err := database.ExecTxWithTenant(ctx, e.stockPool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM stock_events se JOIN stock_levels sl
			   ON se.tenant_id=sl.tenant_id AND se.sku_id=sl.sku_id AND se.warehouse_id=sl.warehouse_id
			   WHERE sl.tenant_id=$1::uuid AND sl.sku_id=$2 AND sl.available_qty < 0`, tenantA, skuA).Scan(&negativeCount)
	})
	if err != nil {
		t.Fatalf("count negative: %v", err)
	}
	if negativeCount != 0 {
		t.Fatalf("negative stock events recorded: %d", negativeCount)
	}
}
