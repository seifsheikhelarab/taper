package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

// stockv1Adjust builds an AdjustStockRequest.
func stockv1Adjust(tenant, sku, wh string, delta int32) *stockv1.AdjustStockRequest {
	return &stockv1.AdjustStockRequest{
		TenantId: tenant, SkuId: sku, WarehouseId: wh,
		QuantityDelta: delta, Reason: "kafka_test", Source: "test",
	}
}

// resv1ReserveRequest builds a ReserveRequest with a single item.
func resv1ReserveRequest(tenant, order, sku, wh string, qty int32) *resv1.ReserveRequest {
	return &resv1.ReserveRequest{
		TenantId: tenant, OrderId: order,
		Items: []*resv1.ReservationItem{{SkuId: sku, WarehouseId: wh, Quantity: qty}},
	}
}

// kafkaBrokers returns configured brokers or "" when Kafka is not available,
// in which case Kafka-gated tests skip so plain CI stays green.
func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		t.Skip("KAFKA_BROKERS not set; skipping Kafka-gated test")
	}
	return strings.Split(b, ",")
}

// kafkaReachable pings the Debezium Connect REST API; connectors must be
// registered for events to flow.
func kafkaConnectReady(t *testing.T) {
	t.Helper()
	url := os.Getenv("KAFKA_CONNECT_URL")
	if url == "" {
		url = "http://localhost:8083"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url + "/connectors")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Debezium Connect not reachable at %s; is docker-compose up?", url)
}

// waitForMessage polls a topic for the first message whose value contains
// substr, failing after the timeout. Reads all partitions.
func waitForMessage(t *testing.T, brokers []string, topic, contains string, timeout time.Duration) kafka.Message {
	t.Helper()
	// Group-mode reader: each call gets a unique ephemeral group starting at
	// the earliest offset. (Topic-level group-less readers fail to receive
	// data against this broker, while group readers work reliably.)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     fmt.Sprintf("wait-%d", time.Now().UnixNano()),
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		m, err := r.ReadMessage(ctx)
		cancel()
		if err != nil {
			continue // deadline on this read; loop until overall deadline
		}
		if contains == "" || strings.Contains(string(m.Value), contains) {
			return m
		}
	}
	t.Fatalf("no message containing %q arrived on %s within %v", contains, topic, timeout)
	return kafka.Message{}
}

// TestKafkaStockEventStreaming: AdjustStock via gRPC -> message on
// stock.events keyed by the composite tenant:sku key (spec US1, US2).
func TestKafkaStockEventStreaming(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConnectReady(t)
	e := setup(t)

	tenant := tenantA
	// Unique per run: topic retains messages from earlier runs, so a shared
	// id would let waitForMessage match a stale event and fail the key check.
	sku := fmt.Sprintf("SKU-KAFKA-1-%d", time.Now().UnixNano())
	e.seed(t, tenant, sku, "W1", 100)

	m := waitForMessage(t, brokers, "stock.events", sku, 90*time.Second)
	key := string(m.Key)
	if key != tenant+":"+sku {
		t.Fatalf("expected composite partition key %s:%s, got %s", tenant, sku, key)
	}
	var ev streaming.OutboxEvent
	if err := json.Unmarshal(m.Value, &ev); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if ev.EventType == "" {
		// EventRouter may deliver event_type as a header instead of value.
		for _, kv := range m.Headers {
			if kv.Key == "eventType" || kv.Key == "event_type" {
				ev.EventType = string(kv.Value)
			}
		}
	}
	if ev.EventType != "stock.adjusted" && ev.EventType != "stock.reserved" {
		t.Fatalf("unexpected event_type %q (value=%s headers=%v)", ev.EventType, m.Value, m.Headers)
	}
}

// TestKafkaReservationEventStreaming: Reserve -> message on reservation.events
// keyed tenant:order (spec US4, depends on T7 composite keys).
func TestKafkaReservationEventStreaming(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConnectReady(t)
	e := setup(t)

	tenant := tenantA
	sku := "SKU-KAFKA-2"
	// Unique per run for the same staleness reason as above.
	order := fmt.Sprintf("ord-kafka-1-%d", time.Now().UnixNano())
	e.seed(t, tenant, sku, "W1", 50)

	if _, err := e.reservation.Reserve(context.Background(), resv1ReserveRequest(tenant, order, sku, "W1", 2)); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	m := waitForMessage(t, brokers, "reservation.events", order, 90*time.Second)
	if key := string(m.Key); key != tenant+":"+order {
		t.Fatalf("expected composite partition key %s:%s, got %s", tenant, order, key)
	}
}

// TestKafkaPerKeyOrdering: interleaved mutations on one SKU across two
// tenants keep per-tenant:sku ordering inside partitions (spec US2).
func TestKafkaPerKeyOrdering(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConnectReady(t)
	e := setup(t)

	// Unique per run so older runs' messages (same topic, keys collide) can
	// never be attributed to this run's per-key sequences.
	sku := fmt.Sprintf("SKU-KAFKA-SHARED-%d", time.Now().UnixNano())
	// Two tenants share the same sku id; keys differ by tenant prefix.
	for i := int32(1); i <= 5; i++ {
		if _, err := e.stock.AdjustStock(context.Background(), stockv1Adjust(tenantA, sku, "W1", i)); err != nil {
			t.Fatalf("adjust A %d: %v", i, err)
		}
		if _, err := e.stock.AdjustStock(context.Background(), stockv1Adjust(tenantB, sku, "W1", i)); err != nil {
			t.Fatalf("adjust B %d: %v", i, err)
		}
	}

	// Collect all messages for both keys, then verify per-key delta ordering.
	// Group-mode reader (see waitForMessage) with a unique ephemeral group.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "stock.events",
		GroupID:     fmt.Sprintf("ordering-%d", time.Now().UnixNano()),
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	deadline := time.Now().Add(60 * time.Second)
	keyA, keyB := tenantA+":"+sku, tenantB+":"+sku
	var deltasA, deltasB []float64
	for time.Now().Before(deadline) && (len(deltasA) < 5 || len(deltasB) < 5) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		m, err := r.ReadMessage(ctx)
		cancel()
		if err != nil {
			break
		}
		// Debezium flattens the JSONB payload into the top-level value.
		var inner struct {
			QuantityDelta float64 `json:"quantity_delta"`
		}
		if err := json.Unmarshal(m.Value, &inner); err != nil {
			continue
		}
		switch string(m.Key) {
		case keyA:
			deltasA = append(deltasA, inner.QuantityDelta)
		case keyB:
			deltasB = append(deltasB, inner.QuantityDelta)
		}
	}
	r.Close()
	if len(deltasA) != 5 || len(deltasB) != 5 {
		t.Fatalf("expected 5 events per key, got A=%d B=%d", len(deltasA), len(deltasB))
	}
	for i, d := range deltasA {
		if d != float64(i+1) {
			t.Fatalf("tenant A ordering broken at %d: %v", i, deltasA)
		}
	}
	for i, d := range deltasB {
		if d != float64(i+1) {
			t.Fatalf("tenant B ordering broken at %d: %v", i, deltasB)
		}
	}
}

// TestKafkaDLQEnvelope: a poison handler dead-letters into <topic>.dlq with a
// populated DLQ Envelope (spec US3), driven through pkg/streaming.
func TestKafkaDLQEnvelope(t *testing.T) {
	brokers := kafkaBrokers(t)
	topic := "stock.events"
	dlq := topic + ".dlq"

	// Publish a probe message directly so the test is self-contained. Key and
	// error are unique per run: the DLQ retains earlier runs' envelopes and a
	// shared needle would match those instead of this run's message.
	runID := time.Now().UnixNano()
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}}
	probeKey := []byte(fmt.Sprintf("dlq-probe-%d", runID))
	probeVal := []byte(`{"poison":true}`)
	if err := w.WriteMessages(context.Background(), kafka.Message{Key: probeKey, Value: probeVal}); err != nil {
		t.Fatalf("seed probe: %v", err)
	}
	w.Close()

	// Consumer with a handler that always fails; MaxRetries=0 for speed.
	c := streaming.NewConsumer(streaming.Config{
		Brokers: brokers, Topic: topic, GroupID: fmt.Sprintf("dlq-test-%d", time.Now().UnixNano()),
		MaxRetries: 0, RetryBackoff: 10 * time.Millisecond,
	}, nil)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = c.Run(runCtx, func(ctx context.Context, m kafka.Message) error {
			// Only poison the probe key; anything else succeeds so offsets move.
			if string(m.Key) == string(probeKey) {
				return fmt.Errorf("poison-%d", runID)
			}
			return nil
		})
		close(done)
	}()

	// Wait for this run's DLQ message.
	envMsg := waitForMessage(t, brokers, dlq, fmt.Sprintf("poison-%d", runID), 90*time.Second)
	cancel()
	<-done
	_ = c.Close()

	env, err := streaming.ParseEnvelope(envMsg.Value)
	if err != nil {
		t.Fatalf("invalid DLQ envelope: %v", err)
	}
	if env.OriginalTopic != topic || string(env.OriginalKey) != string(probeKey) {
		t.Fatalf("envelope topic/key mismatch: %+v", env)
	}
	if env.Error == "" || env.StackTrace == "" || env.Attempts < 1 {
		t.Fatalf("envelope metadata incomplete: %+v", env)
	}
	if string(env.Payload) != string(probeVal) {
		t.Fatalf("envelope payload mismatch: %s", env.Payload)
	}
}
