package orderservice

import (
	"encoding/json"
	"testing"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
)

func lines() []*orderv1.OrderLine {
	return []*orderv1.OrderLine{
		{SkuId: "SKU-1", WarehouseId: "W1", Quantity: 2, UnitPrice: 300},
		{SkuId: "SKU-2", WarehouseId: "W2", Quantity: 1, UnitPrice: 700},
	}
}

// orderTotal drives the payment charge amount: a wrong total means wrong
// money movement.
func TestOrderTotal(t *testing.T) {
	if got := orderTotal(lines()); got != 1300 {
		t.Fatalf("total = %d, want 1300", got)
	}
	if got := orderTotal(nil); got != 0 {
		t.Fatalf("empty total = %d, want 0", got)
	}
}

// Step conversions must not reorder, drop, or mutate lines: the reserve
// call (all-or-nothing) and the allocation confirm must cover exactly the
// ordered lines.
func TestReserveAndStockLineConversion(t *testing.T) {
	in := lines()
	items := reserveItems(in)
	if len(items) != 2 || items[0].SkuId != "SKU-1" || items[1].Quantity != 1 {
		t.Fatalf("reserve items = %+v", items)
	}
	out := stockLines(in)
	if len(out) != 2 || out[1].WarehouseId != "W2" || out[0].Quantity != 2 {
		t.Fatalf("stock lines = %+v", out)
	}
}

// The outbox payload shape feeds the fulfillment consumer: event_type is
// the lowercased state, lines ride along for per-line fan-out.
func TestMarshalOrderEvent(t *testing.T) {
	b := marshalOrderEvent(SagaConfirmed, "o-1", 1300, lines())
	var ev struct {
		EventType   string `json:"event_type"`
		OrderID     string `json:"order_id"`
		TotalAmount int64  `json:"total_amount"`
		Lines       []struct {
			SkuID string `json:"sku_id"`
		} `json:"lines"`
	}
	if err := json.Unmarshal(b, &ev); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if ev.EventType != "order.confirmed" || ev.OrderID != "o-1" || ev.TotalAmount != 1300 {
		t.Fatalf("event = %+v", ev)
	}
	if len(ev.Lines) != 2 || ev.Lines[0].SkuID != "SKU-1" {
		t.Fatalf("lines = %+v", ev.Lines)
	}
}

// Cancel guards (US4), pinned against the constants the DB-backed handler
// switches on: allocation and confirmation are too late to cancel; terminal
// states return success as idempotent no-ops; only the payment window is
// cancellable. If someone renames or adds a state, the explicit set here
// forces a conscious update.
func TestCancelGuards(t *testing.T) {
	tooLate := map[string]bool{SagaAllocated: true, SagaConfirmed: true}
	terminal := map[string]bool{SagaCompensated: true, SagaFailed: true}
	cancellable := map[string]bool{SagaPendingPayment: true, SagaReserved: true}

	for _, state := range []string{
		SagaPendingPayment, SagaReserved, SagaAllocated, SagaConfirmed, SagaCompensated, SagaFailed,
	} {
		// Exactly one bucket applies to every saga state.
		hits := 0
		for _, m := range []map[string]bool{tooLate, terminal, cancellable} {
			if m[state] {
				hits++
			}
		}
		if hits != 1 {
			t.Errorf("state %s hits %d guard buckets, want exactly 1", state, hits)
		}
	}
	if !cancellable[SagaPendingPayment] || !cancellable[SagaReserved] {
		t.Error("the payment window (PENDING_PAYMENT/RESERVED) must stay cancellable")
	}
	if tooLate[SagaReserved] || cancellable[SagaAllocated] {
		t.Error("allocation is the point of no return")
	}
}

func TestReasonOrDefault(t *testing.T) {
	if got := reasonOrDefault("", "customer_cancel"); got != "customer_cancel" {
		t.Fatalf("got %q", got)
	}
	if got := reasonOrDefault("warehouse_fire", "customer_cancel"); got != "warehouse_fire" {
		t.Fatalf("got %q", got)
	}
}
