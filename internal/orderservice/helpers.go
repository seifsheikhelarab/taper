package orderservice

import (
	"encoding/json"
	"strings"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
)

// orderTotal sums quantity * unit_price across lines.
func orderTotal(lines []*orderv1.OrderLine) int64 {
	var total int64
	for _, l := range lines {
		total += int64(l.GetQuantity()) * l.GetUnitPrice()
	}
	return total
}

// marshalOrderEvent builds an outbox payload for an order transition. Lines
// are included when the downstream consumer needs them (fulfillment fans out
// per line to the stock service).
func marshalOrderEvent(state, orderID string, total int64, lines []*orderv1.OrderLine) []byte {
	type line struct {
		SkuID       string `json:"sku_id"`
		WarehouseID string `json:"warehouse_id"`
		Quantity    int32  `json:"quantity"`
	}
	ev := struct {
		EventType   string `json:"event_type"`
		OrderID     string `json:"order_id"`
		TotalAmount int64  `json:"total_amount"`
		Lines       []line `json:"lines,omitempty"`
	}{
		EventType:   "order." + strings.ToLower(state),
		OrderID:     orderID,
		TotalAmount: total,
	}
	for _, l := range lines {
		ev.Lines = append(ev.Lines, line{SkuID: l.GetSkuId(), WarehouseID: l.GetWarehouseId(), Quantity: l.GetQuantity()})
	}
	b, _ := json.Marshal(ev)
	return b
}
