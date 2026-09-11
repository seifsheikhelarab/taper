package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
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
var channelsyncDSN = envOrDSN("TEST_CHANNELSYNC_DSN", "postgres://taper_app:taperapp@localhost:5432/channelsync_db")

// stockSweeperDSN is the BYPASSRLS role against stock_db for the
// reconciliation worker (cross-tenant maintenance).
var stockSweeperDSN = envOrDSN("TEST_STOCK_SWEEPER_DSN", "postgres://taper_sweeper:tapersweeper@localhost:5432/taper_db")

// channelsyncSweeperDSN is the BYPASSRLS role against channelsync_db.
var channelsyncSweeperDSN = envOrDSN("TEST_CHANNELSYNC_SWEEPER_DSN", "postgres://taper_sweeper:tapersweeper@localhost:5432/channelsync_db")

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

// TestStressReservationsAndFulfillment hammers one hot SKU with concurrent
// saga reservations, concurrent fulfillments (plus duplicate replays racing
// the originals), and concurrent reserve attempts on drained stock, then
// asserts the conservation invariants and that reconciliation still derives
// every bucket exactly (no drift) after the mixed concurrent traffic.
func TestStressReservationsAndFulfillment(t *testing.T) {
	e := setup(t)

	const (
		sku        = "stress-hot-sku"
		wh         = "wh-stress"
		initial    = 100
		orders     = 100
		fulfilN    = 60
		replays    = 30
		drainedTry = 40
	)
	e.seed(t, tenantA, sku, wh, initial)

	start := make(chan struct{})
	start2 := make(chan struct{})
	var wg sync.WaitGroup

	// Unique order prefix per run: the topic retains earlier runs' fulfilled
	// events and the consumer filter must only match this run's orders.
	runID := time.Now().UnixNano()
	orderPrefix := fmt.Sprintf("stress-ord-%d-", runID)

	// Phase 1: orders concurrent saga CreateOrders, each reserving 1 unit.
	confirmed := make([]bool, orders)
	for i := 0; i < orders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			cr, err := e.order.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
				TenantId: tenantA, OrderId: fmt.Sprintf("%s%d", orderPrefix, i),
				IdempotencyKey: fmt.Sprintf("stress-key-%d-%d", runID, i),
				Lines:          []*orderv1.OrderLine{{SkuId: sku, WarehouseId: wh, Quantity: 1, UnitPrice: 100}},
			})
			if err == nil && cr.GetStatus() == "CONFIRMED" {
				confirmed[i] = true
			}
		}(i)
	}
	close(start)
	wg.Wait()
	confirmedCount := 0
	for _, c := range confirmed {
		if c {
			confirmedCount++
		}
	}
	if confirmedCount != orders {
		t.Fatalf("confirmed %d/%d orders", confirmedCount, orders)
	}
	// Confirmed orders hold stock in the ALLOCATED bucket (the saga's
	// allocate step moves reserved -> allocated; fulfillment clears it).
	avail, reserved, allocated := e.stockLevel(t, tenantA, sku, wh)
	if avail != 0 || reserved != 0 || allocated != orders {
		t.Fatalf("after reserve phase: avail=%d reserved=%d allocated=%d, want 0/0/%d", avail, reserved, allocated, orders)
	}

	// Phase 2: fulfil orders concurrently; replays of the same orders race
	// them; concurrent reserve attempts on the now-drained stock must all
	// fail (fulfilled stock never returns to available). The real fulfillment
	// consumer runs against the live topic: FulfillStock happens through the
	// event stream, not in-process.
	var fulfillOK int64
	var drainedConfirmed int64
	var consumerFulfilled int64
	brokers := kafkaBrokers(t)
	stressSvc := fulfillment.New(e.stock, nil)
	fc := streaming.NewConsumer(streaming.Config{
		Brokers: brokers, Topic: "order.events", GroupID: fmt.Sprintf("stress-fulfill-%d", time.Now().UnixNano()),
	}, nil)
	fctx, fcancel := context.WithCancel(context.Background())
	fdone := make(chan struct{})
	go func() {
		defer close(fdone)
		_ = fc.Run(fctx, func(ctx context.Context, m kafka.Message) error {
			var ev struct {
				EventType string `json:"event_type"`
				OrderID   string `json:"order_id"`
			}
			if json.Unmarshal(m.Value, &ev) != nil || ev.EventType != "order.fulfilled" || !strings.HasPrefix(ev.OrderID, orderPrefix) {
				return nil // not a stress fulfillment: ignore (offsets still move)
			}
			if err := stressSvc.HandleOrderEvent(ctx, m); err != nil {
				return err
			}
			atomic.AddInt64(&consumerFulfilled, 1)
			return nil
		})
	}()
	for i := 0; i < fulfilN+replays; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start2
			orderIdx := i % fulfilN // replays reuse the first fulfilN orders
			resp, err := e.order.FulfillOrder(context.Background(), &orderv1.FulfillOrderRequest{
				TenantId: tenantA, OrderId: fmt.Sprintf("%s%d", orderPrefix, orderIdx),
				IdempotencyKey: fmt.Sprintf("stress-fulfill-%d-%d", runID, orderIdx),
			})
			if err == nil && resp.GetSuccess() {
				atomic.AddInt64(&fulfillOK, 1)
			}
		}(i)
	}
	for i := 0; i < drainedTry; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start2
			cr, err := e.order.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
				TenantId: tenantA, OrderId: fmt.Sprintf("stress-drained-%d", i),
				IdempotencyKey: fmt.Sprintf("stress-drained-key-%d", i),
				Lines:          []*orderv1.OrderLine{{SkuId: sku, WarehouseId: wh, Quantity: 1, UnitPrice: 100}},
			})
			if err == nil && cr.GetStatus() == "CONFIRMED" {
				atomic.AddInt64(&drainedConfirmed, 1)
			}
		}(i)
	}
	close(start2)
	wg.Wait()

	if drainedConfirmed != 0 {
		fcancel()
		<-fdone
		t.Fatalf("%d reserve attempts on drained stock were confirmed (oversell!)", drainedConfirmed)
	}
	if fulfillOK < fulfilN {
		fcancel()
		<-fdone
		t.Fatalf("only %d fulfillment calls succeeded, want >= %d (one per order incl. replays)", fulfillOK, fulfilN)
	}

	// Wait for the consumer to drive FulfillStock for every stress order
	// (each order yields exactly one fulfilled event; replays are no-ops at
	// the stock service).
	waitDeadline := time.Now().Add(90 * time.Second)
	for consumerFulfilled < int64(fulfilN) && time.Now().Before(waitDeadline) {
		time.Sleep(200 * time.Millisecond)
	}
	fcancel()
	<-fdone
	if consumerFulfilled != int64(fulfilN) {
		t.Fatalf("consumer fulfilled %d/%d stress orders", consumerFulfilled, fulfilN)
	}

	// Conservation: fulfilled stock left the warehouse entirely; unfulfilled
	// confirmed orders remain allocated.
	avail, reserved, allocated = e.stockLevel(t, tenantA, sku, wh)
	if allocated != orders-fulfilN {
		t.Fatalf("allocated=%d, want %d (unfulfilled confirmed orders)", allocated, orders-fulfilN)
	}
	if reserved != 0 || avail != 0 {
		t.Fatalf("reserved=%d avail=%d, want 0/0", reserved, avail)
	}
	if avail+reserved+allocated != initial-fulfilN {
		t.Fatalf("conservation broken: avail+reserved+allocated=%d, want %d", avail+reserved+allocated, initial-fulfilN)
	}

	// Reconciliation must derive every bucket exactly after the concurrent
	// mixed traffic - the signed marker events must line up under contention.
	w := stockservice.New(mustPool(t, stockSweeperDSN), nil)
	drifts, err := w.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for _, d := range drifts {
		if d.SKUID == sku {
			t.Fatalf("reconciliation drift on stressed sku: %+v", d)
		}
	}
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

// TestCDCRoleCanReadOutbox is the regression test for the CI streaming
// outage: Debezium snapshots public.outbox with plain SELECTs as taper_cdc,
// so that role needs SELECT on outbox (granted in-migration) and must be
// exempt from outbox tenant RLS (the policy is scoped to taper_app). When
// either invariant breaks, the snapshot fails or comes back empty and the
// connectors report RUNNING while streaming nothing.
func TestCDCRoleCanReadOutbox(t *testing.T) {
	tenant := "22222222-2222-2222-2222-222222222222"
	cases := []struct {
		dbname string
		appDSN string
	}{
		{"taper_db", stockDSN},
		{"reservation_db", reservationDSN},
		{"order_db", orderDSN},
	}
	for _, tc := range cases {
		t.Run(tc.dbname, func(t *testing.T) {
			app := mustPool(t, tc.appDSN)
			defer app.Close()
			cdc := mustPool(t, fmt.Sprintf("postgres://taper_cdc:tapercdc@%s/%s", envOrDSN("TEST_PG_ADDR", "localhost:5432"), tc.dbname))
			defer cdc.Close()

			ctx := context.Background()
			// Write a row through the app role's normal RLS-gated path.
			tx, err := app.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "SELECT set_config('app.current_tenant_id', $1, false)", tenant); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO outbox (id, tenant_id, aggregate_type, aggregate_id, event_type, payload)
				VALUES (gen_random_uuid(), $1, 'Test', 'cdc-grant-probe', 'test.probe', '{}')`, tenant); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			// The CDC role must read it back without any tenant context set.
			var n int
			if err := cdc.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&n); err != nil {
				t.Fatalf("taper_cdc cannot read outbox in %s (missing GRANT SELECT?): %v", tc.dbname, err)
			}
			if n < 1 {
				t.Fatalf("taper_cdc sees 0 outbox rows in %s although rows exist (outbox RLS is hiding them)", tc.dbname)
			}
		})
	}
}
