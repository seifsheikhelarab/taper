package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/channelsync"
	"github.com/seifsheikhelarab/taper/internal/fulfillment"
	"github.com/seifsheikhelarab/taper/internal/stockservice"
	"github.com/seifsheikhelarab/taper/pkg/channel"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

// channelsyncDSN points at the channelsync projection database.
const channelsyncDSN = "postgres://taper_app:taperapp@localhost:5432/channelsync_db"

// stockSweeperDSN is the BYPASSRLS role against stock_db for the
// reconciliation worker (cross-tenant maintenance).
const stockSweeperDSN = "postgres://taper_sweeper:tapersweeper@localhost:5432/taper_db"

// channelsyncSweeperDSN is the BYPASSRLS role against channelsync_db.
const channelsyncSweeperDSN = "postgres://taper_sweeper:tapersweeper@localhost:5432/channelsync_db"

// newGroupReader builds an ephemeral consumer-group reader starting at the
// earliest offset (group-less readers receive no data on this broker).
func newGroupReader(brokers []string, topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     group,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
}

// waitFulfilledEvent fetches messages until one decodes to this run's
// order.fulfilled event. (jsonb normalizes key order, so substring needles
// on payload adjacency are unreliable.)
func waitFulfilledEvent(t *testing.T, brokers []string, orderID string, timeout time.Duration) kafka.Message {
	t.Helper()
	r := newGroupReader(brokers, "order.events", fmt.Sprintf("fulfill-%d", time.Now().UnixNano()))
	defer r.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		m, err := r.FetchMessage(ctx)
		cancel()
		if err != nil {
			break
		}
		var ev struct {
			EventType string `json:"event_type"`
			OrderID   string `json:"order_id"`
		}
		if json.Unmarshal(m.Value, &ev) == nil && ev.EventType == "order.fulfilled" && ev.OrderID == orderID {
			return m
		}
	}
	t.Fatalf("no order.fulfilled for %s arrived on order.events within %v", orderID, timeout)
	return kafka.Message{}
}

// mustChannelsyncPool connects to channelsync_db and truncates between tests.
func mustChannelsyncPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), channelsyncDSN)
	if err != nil {
		t.Fatalf("connect channelsync_db: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := truncate(t, pool, "availability_projection", "alerts", "processed_idempotency_keys"); err != nil {
		t.Fatalf("truncate channelsync: %v", err)
	}
	return pool
}

// fulfillOrder asserts CONFIRMED -> FULFILLED via the saga's new RPC; a
// replay of an already-FULFILLED order is also a success.
func fulfillOrder(t *testing.T, e *testEnv, tenant, order string) {
	t.Helper()
	resp, err := e.order.FulfillOrder(context.Background(), &orderv1.FulfillOrderRequest{
		TenantId: tenant, OrderId: order, IdempotencyKey: "test-fulfill-" + order,
	})
	if err != nil {
		t.Fatalf("FulfillOrder: %v", err)
	}
	if !resp.GetSuccess() || resp.GetStatus() != "FULFILLED" {
		t.Fatalf("want FULFILLED, got success=%v status=%s", resp.GetSuccess(), resp.GetStatus())
	}
}

// TestPhase3AvailabilityProjection: AdjustStock -> stock.adjusted on the wire
// -> channelsync pushes the absolute and records the projection (US1).
func TestPhase3AvailabilityProjection(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConnectReady(t)
	e := setup(t)
	csPool := mustChannelsyncPool(t)

	tenant, sku, wh := tenantA, fmt.Sprintf("SKU-P3-AVAIL-%d", time.Now().UnixNano()), "W1"
	e.seed(t, tenant, sku, wh, 42)

	svc := channelsync.New(csPool, channel.NewSandbox(nil), channel.NewSandbox(nil), nil)
	m := waitForMessage(t, brokers, "stock.events", sku, 90*time.Second)
	if err := svc.HandleStockEvent(context.Background(), m); err != nil {
		t.Fatalf("HandleStockEvent: %v", err)
	}

	// Read via a BYPASSRLS pool: the app pool has no tenant in context and
	// RLS would hide the row from the test's own query.
	var qty int32
	if err := mustPool(t, channelsyncSweeperDSN).QueryRow(context.Background(),
		"SELECT available_qty FROM availability_projection WHERE sku_id=$1 AND warehouse_id=$2", sku, wh).Scan(&qty); err != nil {
		t.Fatalf("projection row: %v", err)
	}
	if qty != 42 {
		t.Fatalf("want projected availability 42, got %d", qty)
	}
}

// TestPhase3FulfillmentFlow: full event-driven fulfillment (US3): CONFIRMED
// order -> FulfillOrder -> order.fulfilled on the wire -> consumer calls
// FulfillStock -> allocated decremented, available untouched. A replay of
// the same event is a no-op (at-least-once safety).
func TestPhase3FulfillmentFlow(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConnectReady(t)
	e := setup(t)

	tenant, sku, wh := tenantA, fmt.Sprintf("SKU-P3-FULFILL-%d", time.Now().UnixNano()), "W1"
	// Unique order id per run: the topic retains earlier runs' events and a
	// reused id would let waitForMessage match a stale fulfilled event whose
	// lines reference SKUs this run never created.
	orderID := fmt.Sprintf("ord-p3-fulfill-%d", time.Now().UnixNano())
	e.seed(t, tenant, sku, wh, 50)
	cr, err := e.order.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
		TenantId: tenant, OrderId: orderID, IdempotencyKey: "p3-key-" + orderID,
		Lines: []*orderv1.OrderLine{{SkuId: sku, WarehouseId: wh, Quantity: 5, UnitPrice: 100}},
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if cr.GetStatus() != "CONFIRMED" {
		t.Fatalf("want CONFIRMED, got %s", cr.GetStatus())
	}
	fulfillOrder(t, e, tenant, orderID)

	svc := fulfillment.New(e.stock, t.Logf)
	// The order topic carries every saga event (this run and previous runs);
	// jsonb re-sorts payload keys alphabetically on the wire, so substring
	// needles on adjacency are unreliable - decode until this run's
	// order.fulfilled arrives.
	m := waitFulfilledEvent(t, brokers, orderID, 90*time.Second)
	if err := svc.HandleOrderEvent(context.Background(), m); err != nil {
		t.Fatalf("HandleOrderEvent: %v", err)
	}
	avail, reserved, allocated := e.stockLevel(t, tenant, sku, wh)
	if allocated != 0 || reserved != 0 || avail != 45 {
		t.Fatalf("want avail=45 reserved=0 allocated=0, got %d/%d/%d", avail, reserved, allocated)
	}
	if err := svc.HandleOrderEvent(context.Background(), m); err != nil {
		t.Fatalf("replay HandleOrderEvent: %v", err)
	}
}

// TestPhase3FulfillOrderPreconditions: state machine rules (US3).
func TestPhase3FulfillOrderPreconditions(t *testing.T) {
	e := setup(t)
	tenant, sku, wh := tenantA, "SKU-P3-PRE", "W1"
	e.seed(t, tenant, sku, wh, 10)

	if _, err := e.order.FulfillOrder(context.Background(), &orderv1.FulfillOrderRequest{
		TenantId: tenant, OrderId: "no-such-order",
	}); err == nil {
		t.Fatal("want error fulfilling unknown order")
	}

	if _, err := e.order.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
		TenantId: tenant, OrderId: "ord-p3-2", IdempotencyKey: "p3-key-2",
		Lines: []*orderv1.OrderLine{{SkuId: sku, WarehouseId: wh, Quantity: 5, UnitPrice: 100}},
	}); err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	// Re-FulfillOrder after FULFILLED (original key) is a successful no-op.
	fulfillOrder(t, e, tenant, "ord-p3-2")
	// A different (non-original) request on a FULFILLED order is rejected:
	// only genuine replays pass.
	if _, err := e.order.FulfillOrder(context.Background(), &orderv1.FulfillOrderRequest{
		TenantId: tenant, OrderId: "ord-p3-2", IdempotencyKey: "different-key",
	}); err == nil {
		t.Fatal("want rejection for non-original request on FULFILLED order")
	}
	// No idempotency key on a FULFILLED order is likewise rejected.
	if _, err := e.order.FulfillOrder(context.Background(), &orderv1.FulfillOrderRequest{
		TenantId: tenant, OrderId: "ord-p3-2",
	}); err == nil {
		t.Fatal("want rejection for keyless request on FULFILLED order")
	}
}

// TestPhase3ReconciliationDrift: inject drift via SQL, reconcile, expect the
// audit lock; then clear it via UnlockStockForAudit (US4).
func TestPhase3ReconciliationDrift(t *testing.T) {
	e := setup(t)
	tenant, sku, wh := tenantA, "SKU-P3-DRIFT", "W1"
	e.seed(t, tenant, sku, wh, 10)

	w := stockservice.New(mustPool(t, stockSweeperDSN), nil)
	drifts, err := w.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(drifts) != 0 {
		t.Fatalf("want no drift, got %+v", drifts)
	}

	// Inject drift directly in the DB (bypassing the event log). The write
	// goes through the BYPASSRLS pool: the app pool has no tenant in context
	// and RLS would silently update zero rows.
	if _, err := mustPool(t, stockSweeperDSN).Exec(context.Background(),
		"UPDATE stock_levels SET available_qty = available_qty + 7 WHERE sku_id=$1 AND warehouse_id=$2", sku, wh); err != nil {
		t.Fatalf("inject drift: %v", err)
	}
	drifts, err = w.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile after drift: %v", err)
	}
	if len(drifts) == 0 {
		t.Fatal("want drift to be detected")
	}
	var locked bool
	// Read via the BYPASSRLS pool: the app pool has no tenant in context and
	// RLS would hide the row entirely.
	if err := mustPool(t, stockSweeperDSN).QueryRow(context.Background(),
		"SELECT is_locked_for_audit FROM stock_levels WHERE sku_id=$1 AND warehouse_id=$2", sku, wh).Scan(&locked); err != nil {
		t.Fatalf("read lock: %v", err)
	}
	if !locked {
		t.Fatal("want is_locked_for_audit after drift")
	}

	ur, err := e.stock.UnlockStockForAudit(context.Background(), &stockv1.UnlockStockForAuditRequest{
		TenantId: tenant, SkuId: sku, WarehouseId: wh,
		Reason: "reconciled", ActorId: "test-manager",
	})
	if err != nil {
		t.Fatalf("UnlockStockForAudit: %v", err)
	}
	if !ur.GetSuccess() || !ur.GetIsUnlocked() {
		t.Fatalf("want unlocked, got %+v", ur)
	}
}

// TestPhase3DLQReplay: a poison message dead-letters; cmd/dlqreplay replays
// it back onto the original topic (US5), verified end-to-end via the CLI
// subprocess against the real broker.
func TestPhase3DLQReplay(t *testing.T) {
	brokers := kafkaBrokers(t)
	topic := "stock.events"
	dlq := topic + ".dlq"

	// Unique key and error per run: the DLQ retains earlier runs' envelopes,
	// so fixed needles would match stale messages instead of this run's.
	runID := time.Now().UnixNano()
	probeKey := []byte(fmt.Sprintf("p3-dlq-probe-%d", runID))
	probeVal := []byte(`{"poison":true,"phase":3}`)
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}}
	if err := w.WriteMessages(context.Background(), kafka.Message{Key: probeKey, Value: probeVal}); err != nil {
		t.Fatalf("seed probe: %v", err)
	}
	w.Close()

	c := streaming.NewConsumer(streaming.Config{
		Brokers: brokers, Topic: topic, GroupID: fmt.Sprintf("p3-dlq-%d", runID),
		MaxRetries: 0, RetryBackoff: 10 * time.Millisecond,
	}, nil)
	runCtx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = c.Run(runCtx, func(ctx context.Context, m kafka.Message) error {
			if string(m.Key) == string(probeKey) {
				return fmt.Errorf("poison-%d", runID)
			}
			return nil
		})
	}()
	waitForMessage(t, brokers, dlq, fmt.Sprintf("poison-%d", runID), 60*time.Second)
	cancel()

	out := runDLQReplay(t, "replay", "-topic", dlq, "-key", string(probeKey))
	if !strings.Contains(out, "replayed") {
		t.Fatalf("want replayed output, got: %s", out)
	}
	// The probe key is unique per run, so key-matching cannot hit stale
	// messages from earlier runs (value needles can).
	got := waitKey(t, brokers, topic, string(probeKey), 60*time.Second)
	if !strings.Contains(string(got.Value), "phase\":3") {
		t.Fatalf("want original payload replayed, got %s", got.Value)
	}
}

// waitKey fetches messages until one with the given key arrives.
func waitKey(t *testing.T, brokers []string, topic, key string, timeout time.Duration) kafka.Message {
	t.Helper()
	r := newGroupReader(brokers, topic, fmt.Sprintf("keywait-%d", time.Now().UnixNano()))
	defer r.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		m, err := r.FetchMessage(ctx)
		cancel()
		if err != nil {
			break
		}
		if string(m.Key) == key {
			return m
		}
	}
	t.Fatalf("no message with key %q arrived on %s within %v", key, topic, timeout)
	return kafka.Message{}
}

// runDLQReplay builds and runs the dlqreplay CLI against the live brokers.
func runDLQReplay(t *testing.T, args ...string) string {
	t.Helper()
	bin := exec.Command("go", append([]string{"run", "./cmd/dlqreplay"}, args...)...)
	bin.Dir = ".." // run from the module root so ./cmd/dlqreplay resolves
	out, err := bin.CombinedOutput()
	if err != nil {
		t.Fatalf("dlqreplay %v: %v\n%s", args, err, out)
	}
	return string(out)
}
