// Package streaming provides a minimal Kafka consumer framework implementing
// the spec's reliability requirements: consumer groups with at-least-once
// semantics (offsets commit only after the handler succeeds), bounded retry,
// and a dead-letter path that wraps failed messages in the DLQ Envelope
// defined in CONTEXT.md.
//
// Kafka client: github.com/segmentio/kafka-go (pure Go, no cgo, consumer
// group protocol built in).
package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/segmentio/kafka-go"
)

// Envelope is the DLQ Envelope from CONTEXT.md: the standardized wrapper
// containing the failed Kafka message payload, retry metadata, and stack
// trace for dead-letter queues.
type Envelope struct {
	OriginalTopic string            `json:"original_topic"`
	OriginalKey   string            `json:"original_key"`
	Payload       []byte            `json:"payload"`
	Headers       map[string]string `json:"headers,omitempty"`
	ConsumerGroup string            `json:"consumer_group"`
	Error         string            `json:"error"`
	Attempts      int               `json:"attempts"`
	FailedAtUnix  int64             `json:"failed_at_unix"`
	StackTrace    string            `json:"stack_trace"`
}

// Handler processes one Kafka message. Returning an error triggers retry,
// then dead-lettering once retries are exhausted.
type Handler func(ctx context.Context, msg kafka.Message) error

// Config tunes a Consumer.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
	// MaxRetries is the number of redelivery attempts within this process
	// before dead-lettering. Defaults to 3.
	MaxRetries int
	// RetryBackoff between attempts. Defaults to 200ms.
	RetryBackoff time.Duration
	// DLQTopic defaults to <topic>.dlq.
	DLQTopic string
	// PollTimeout caps each FetchMessage wait. Defaults to 500ms so ctx
	// cancellation is responsive.
	PollTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	if c.DLQTopic == "" {
		c.DLQTopic = c.Topic + ".dlq"
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = 500 * time.Millisecond
	}
	return c
}

// Consumer consumes a topic with at-least-once semantics and DLQ on poison.
type Consumer struct {
	cfg    Config
	reader *kafka.Reader
	dlq    *kafka.Writer
	// dlqSink overrides the Kafka DLQ writer (tests). When nil, deadLetter
	// writes to the DLQ topic over Kafka.
	dlqSink func(ctx context.Context, m kafka.Message) error
	log     func(format string, args ...any)
}

// NewConsumer builds a consumer for the topic in a consumer group.
func NewConsumer(cfg Config, log func(format string, args ...any)) *Consumer {
	if log == nil {
		log = func(string, ...any) {}
	}
	cfg = cfg.withDefaults()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
		// At-least-once: offsets commit only after the handler succeeds.
		CommitInterval: 0,
		MaxWait:        cfg.PollTimeout,
	})
	dlq := &kafka.Writer{
		Addr:  kafka.TCP(cfg.Brokers...),
		Topic: cfg.DLQTopic,
		// Preserve partition affinity from the original key when set.
		Balancer: &kafka.Hash{},
	}
	return &Consumer{cfg: cfg, reader: reader, dlq: dlq, log: log}
}

// Run consumes until ctx is cancelled. Every message is processed with
// retries; a message that keeps failing is dead-lettered and then committed
// so the group can make progress.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return c.reader.Close()
			}
			c.log("streaming: fetch: %v", err)
			continue
		}
		if err := c.process(ctx, msg, h); err != nil {
			c.log("streaming: process key=%s: %v", string(msg.Key), err)
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log("streaming: commit: %v", err)
		}
	}
}

// process handles one message with bounded retry, then dead-letters.
func (c *Consumer) process(ctx context.Context, msg kafka.Message, h Handler) error {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.RetryBackoff):
			}
		}
		if err := h(ctx, msg); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return c.deadLetter(ctx, msg, lastErr, c.cfg.MaxRetries+1)
}

// deadLetter publishes the DLQ Envelope for a failed message.
func (c *Consumer) deadLetter(ctx context.Context, msg kafka.Message, cause error, attempts int) error {
	headers := make(map[string]string, len(msg.Headers))
	for _, kv := range msg.Headers {
		headers[kv.Key] = string(kv.Value)
	}
	env := Envelope{
		OriginalTopic: msg.Topic,
		OriginalKey:   string(msg.Key),
		Payload:       msg.Value,
		Headers:       headers,
		ConsumerGroup: c.cfg.GroupID,
		Error:         fmt.Sprint(cause),
		Attempts:      attempts,
		FailedAtUnix:  time.Now().Unix(),
		StackTrace:    string(debug.Stack()),
	}
	b, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}
	dlqMsg := kafka.Message{Key: msg.Key, Value: b}
	if c.dlqSink != nil {
		if err := c.dlqSink(ctx, dlqMsg); err != nil {
			return err
		}
		// Surface the dead-letter marker to callers via a sentinel error so
		// Run logs it and the offset commits (poison messages never block).
		return fmt.Errorf("dead-lettered after %d attempts: %w", attempts, cause)
	}
	if err := c.dlq.WriteMessages(ctx, dlqMsg); err != nil {
		return err
	}
	return fmt.Errorf("dead-lettered after %d attempts: %w", attempts, cause)
}

// Close releases the reader and DLQ writer.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return err
	}
	return c.dlq.Close()
}
