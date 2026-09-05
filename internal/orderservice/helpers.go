package orderservice

import (
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/protobuf/proto"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
)

func hashPayload(msg proto.Message) string {
	b, _ := proto.Marshal(msg)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// marshalSagaEvent builds the outbox payload for a saga/order transition.
func marshalSagaEvent(eventType, orderID string, total int64) []byte {
	return []byte(`{"event_type":"` + eventType + `","order_id":"` + orderID + `","total_amount":` + itoa(total) + `}`)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// orderTotal sums quantity * unit_price across lines.
func orderTotal(lines []*orderv1.OrderLine) int64 {
	var total int64
	for _, l := range lines {
		total += int64(l.GetQuantity()) * l.GetUnitPrice()
	}
	return total
}
