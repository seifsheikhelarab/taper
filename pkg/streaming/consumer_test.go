package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestProcessSuccessOnFirstAttempt(t *testing.T) {
	c := &Consumer{cfg: Config{Topic: "t", GroupID: "g"}.withDefaults()}
	calls := 0
	err := c.process(context.Background(), kafka.Message{Key: []byte("k"), Value: []byte("v")}, func(ctx context.Context, m kafka.Message) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 handler call, got %d", calls)
	}
}

func TestProcessRetriesThenSucceeds(t *testing.T) {
	c := &Consumer{cfg: Config{Topic: "t", GroupID: "g", MaxRetries: 3, RetryBackoff: 0}.withDefaults()}
	calls := 0
	err := c.process(context.Background(), kafka.Message{}, func(ctx context.Context, m kafka.Message) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success on 3rd attempt, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}
