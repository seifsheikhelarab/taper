package streaming

import (
	"encoding/json"
	"fmt"
)

// ParseEnvelope decodes a DLQ Envelope from a dead-letter message value.
func ParseEnvelope(b []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return Envelope{}, fmt.Errorf("decode dlq envelope: %w", err)
	}
	if env.OriginalTopic == "" || env.Payload == nil {
		return Envelope{}, fmt.Errorf("dlq envelope missing original_topic or payload")
	}
	return env, nil
}
