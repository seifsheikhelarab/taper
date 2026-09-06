// Command dlqreplay inspects and replays dead-lettered Kafka messages.
//
// Usage:
//
//	dlqreplay inspect  -brokers localhost:29092 -topic stock.events.dlq [-limit 20]
//	dlqreplay replay   -brokers localhost:29092 -topic stock.events.dlq [-key SKU-1] [-dry-run]
//
// DLQ messages are DLQ Envelopes (pkg/streaming): replay republishes the
// original key, headers, and payload to the envelope's original topic.
// Kafka cannot delete individual messages, so replayed records remain on
// the DLQ topic; replays are at-least-once and consumers must tolerate
// duplicates (they already do by design).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:29092", "comma-separated Kafka brokers")
	topic := fs.String("topic", "", "DLQ topic (e.g. stock.events.dlq)")
	group := fs.String("group", "", "consumer group for reading (default: ephemeral per run)")
	key := fs.String("key", "", "only replay messages whose original key matches (replay)")
	limit := fs.Int("limit", 20, "max messages to show (inspect)")
	dryRun := fs.Bool("dry-run", false, "replay without producing (replay)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		usage()
	}
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "-topic is required")
		usage()
	}
	brokerList := strings.Split(*brokers, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	switch cmd {
	case "inspect":
		inspect(ctx, brokerList, *topic, ephemeralGroup(*group), *limit)
	case "replay":
		replay(ctx, brokerList, *topic, ephemeralGroup(*group), *key, *dryRun)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: dlqreplay <inspect|replay> -brokers <list> -topic <dlq-topic> [-key k] [-limit n] [-dry-run]")
	os.Exit(2)
}

func ephemeralGroup(g string) string {
	if g != "" {
		return g
	}
	return fmt.Sprintf("dlqreplay-%d", time.Now().UnixNano())
}

// readAll drains the DLQ topic from the earliest offset via an ephemeral
// consumer group (group-less readers receive no data on this broker).
func readAll(ctx context.Context, brokers []string, topic, group string) []kafka.Message {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	defer r.Close()
	var msgs []kafka.Message
	var lastErr error
	idle := 0
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		m, err := r.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				break // parent cancelled/expired
			}
			lastErr = err
			idle++
			if idle >= 3 {
				fmt.Fprintf(os.Stderr, "stopping after repeated fetch errors: %v\n", lastErr)
				break
			}
			continue
		}
		idle = 0
		msgs = append(msgs, m)
		// Commit so a drained DLQ is not re-read by the same run.
		_ = r.CommitMessages(context.Background(), m)
	}
	return msgs
}

func inspect(ctx context.Context, brokers []string, topic, group string, limit int) {
	msgs := readAll(ctx, brokers, topic, group)
	if len(msgs) == 0 {
		fmt.Println("no messages on", topic)
		return
	}
	for i, m := range msgs {
		if i >= limit {
			fmt.Printf("... and %d more\n", len(msgs)-limit)
			break
		}
		env, err := streaming.ParseEnvelope(m.Value)
		if err != nil {
			fmt.Printf("[%d] key=%s (not a DLQ envelope: %v)\n", i, string(m.Key), err)
			continue
		}
		fmt.Printf("[%d] key=%s topic=%s attempts=%d error=%q failed_at=%s payload=%.120s\n",
			i, env.OriginalKey, env.OriginalTopic, env.Attempts, env.Error,
			time.Unix(env.FailedAtUnix, 0).Format(time.RFC3339), string(env.Payload))
	}
}

func replay(ctx context.Context, brokers []string, topic, group, key string, dryRun bool) {
	msgs := readAll(ctx, brokers, topic, group)
	var w *kafka.Writer
	if !dryRun {
		w = &kafka.Writer{Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{}}
		defer w.Close()
	}
	replayed, skipped := 0, 0
	for _, m := range msgs {
		env, err := streaming.ParseEnvelope(m.Value)
		if err != nil {
			fmt.Printf("skip key=%s: not a DLQ envelope: %v\n", string(m.Key), err)
			skipped++
			continue
		}
		if key != "" && env.OriginalKey != key {
			skipped++
			continue
		}
		headers := make([]kafka.Header, 0, len(env.Headers))
		for k, v := range env.Headers {
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}
		if dryRun {
			fmt.Printf("dry-run: key=%s -> %s (%d bytes)\n", env.OriginalKey, env.OriginalTopic, len(env.Payload))
		} else {
			if err := w.WriteMessages(ctx, kafka.Message{
				Topic:   env.OriginalTopic,
				Key:     []byte(env.OriginalKey),
				Value:   env.Payload,
				Headers: headers,
			}); err != nil {
				fmt.Fprintf(os.Stderr, "produce key=%s: %v\n", env.OriginalKey, err)
				os.Exit(1)
			}
			fmt.Printf("replayed: key=%s -> %s\n", env.OriginalKey, env.OriginalTopic)
		}
		replayed++
	}
	fmt.Printf("done: %d replayed, %d skipped%s\n", replayed, skipped, dryRunSuffix(dryRun))
}

func dryRunSuffix(dryRun bool) string {
	if dryRun {
		return " (dry-run, nothing produced)"
	}
	return ""
}
