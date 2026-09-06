package orderservice

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
)

func hashPayload(msg proto.Message) string {
	b, _ := proto.Marshal(msg)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

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
	var b strings.Builder
	b.WriteString(`{"event_type":"`)
	b.WriteString("order." + strings.ToLower(state))
	b.WriteString(`","order_id":"`)
	b.WriteString(orderID)
	b.WriteString(`","total_amount":`)
	b.WriteString(strconv.FormatInt(total, 10))
	if len(lines) > 0 {
		b.WriteString(`,"lines":[`)
		for i, l := range lines {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`{"sku_id":"`)
			b.WriteString(l.GetSkuId())
			b.WriteString(`","warehouse_id":"`)
			b.WriteString(l.GetWarehouseId())
			b.WriteString(`","quantity":`)
			b.WriteString(strconv.FormatInt(int64(l.GetQuantity()), 10))
			b.WriteByte('}')
		}
		b.WriteByte(']')
	}
	b.WriteByte('}')
	return []byte(b.String())
}
