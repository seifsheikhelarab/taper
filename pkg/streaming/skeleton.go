package streaming

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// OutboxEvent is the decoded shape of an outbox event streamed by Debezium.
// (Named OutboxEvent, not StockEvent, to avoid colliding with the sqlc
// generated db.StockEvent row type.) The outbox event router flattens the
// outbox row: the value carries payload plus the extracted aggregate and
// event-type fields.
type OutboxEvent struct {
	EventType   string          `json:"event_type"`
	AggregateID string          `json:"aggregate_id"`
	Payload     json.RawMessage `json:"payload"`
}

// DecodeOutboxEvent decodes an outbox event message value.
func DecodeOutboxEvent(value []byte) (OutboxEvent, error) {
	var ev OutboxEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return OutboxEvent{}, fmt.Errorf("decode outbox event: %w", err)
	}
	return ev, nil
}

// LogHandler returns a skeleton Handler that decodes stock events and logs
// them — the verification consumer described by spec #9. Real business
// consumers are out of scope for this phase.
func LogHandler(log func(format string, args ...any)) Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		ev, err := DecodeOutboxEvent(msg.Value)
		if err != nil {
			return err
		}
		log("outbox event type=%s aggregate=%s", ev.EventType, ev.AggregateID)
		return nil
	}
}
