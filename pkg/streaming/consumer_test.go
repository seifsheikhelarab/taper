package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestProcessSuccessOnFirstAttempt(t *testing.T) {
	c := &Consumer{cfg: Config{Topic: "t", GroupID: "g"}.withDefaults(), dlqSink: func(ctx context.Context, m kafka.Message) error { return nil }}
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
	c.dlqSink = func(ctx context.Context, m kafka.Message) error { return nil }
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

func TestProcessDeadLettersAfterExhaustion(t *testing.T) {
	var dlqMsgs []kafka.Message
	c := &Consumer{cfg: Config{Topic: "stock.events", GroupID: "g", MaxRetries: 1, RetryBackoff: 0}.withDefaults()}
	c.dlqSink = func(ctx context.Context, m kafka.Message) error {
		dlqMsgs = append(dlqMsgs, m)
		return nil
	}
	calls := 0
	err := c.process(context.Background(), kafka.Message{
		Topic: "stock.events", Key: []byte("tenant:sku"), Value: []byte(`{"x":1}`),
	}, func(ctx context.Context, m kafka.Message) error {
		calls++
		return errors.New("poison")
	})
	if err == nil {
		t.Fatal("expected DLQ marker error to surface")
	}
	if calls != 2 { // 1 initial + 1 retry (MaxRetries=1)
		t.Fatalf("expected 2 attempts, got %d", calls)
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlqMsgs))
	}
	env, err := ParseEnvelope(dlqMsgs[0].Value)
	if err != nil {
		t.Fatalf("DLQ value is not a valid envelope: %v", err)
	}
	if env.OriginalTopic != "stock.events" || string(env.OriginalKey) != "tenant:sku" {
		t.Fatalf("envelope mismatch: %+v", env)
	}
	if env.Error != "poison" || env.Attempts != 2 || env.StackTrace == "" {
		t.Fatalf("envelope metadata missing: %+v", env)
	}
	if string(env.Payload) != `{"x":1}` {
		t.Fatalf("envelope payload mismatch: %s", env.Payload)
	}
}
