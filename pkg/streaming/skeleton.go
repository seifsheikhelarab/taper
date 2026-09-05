package streaming

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// StockEvent is the decoded shape of an outbox event streamed from the stock
// service. Debezium's outbox event router flattens the outbox row: the value
// carries payload plus the extracted aggregate/event-type fields.
type StockEvent struct {
	EventType   string          `json:"event_type"`
	AggregateID string          `json:"aggregate_id"`
	Payload     json.RawMessage `json:"payload"`
}

// DecodeStockEvent decodes a stock.events message value.
func DecodeStockEvent(value []byte) (StockEvent, error) {
	var ev StockEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return StockEvent{}, fmt.Errorf("decode stock event: %w", err)
	}
	return ev, nil
}

// LogHandler returns a skeleton Handler that decodes stock events and logs
// them — the verification consumer described by spec #9. Real business
// consumers are out of scope for this phase.
func LogHandler(log func(format string, args ...any)) Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		ev, err := DecodeStockEvent(msg.Value)
		if err != nil {
			return err
		}
		log("stock event type=%s aggregate=%s", ev.EventType, ev.AggregateID)
		return nil
	}
}
