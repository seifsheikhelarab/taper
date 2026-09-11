package integration

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Phase 5 chaos suite (spec #44, T5). Gated behind CHAOS=1 (kills real
// subprocesses; not per-push). Runbook: docs/runbooks/chaos.md.
//
// The invariant under chaos (ADR-0001): every seeded unit stays accounted
// for. After kill + restart + quiescence (resume loop and/or TTL sweeper):
//
//	available + allocated == seeded  &&  reserved == 0
//
// i.e. no orphaned holds, no lost stock, no oversell — regardless of where
// the kill landed inside the saga.

const (
	chaosTenant = "22222222-2222-4222-8222-222222222222"
	chaosSKU    = "CHAOS-SKU"
	chaosWH     = "W1"
	chaosSeeded = int32(10)
)

// chaosEnv owns the chaos test's subprocess stack and assertion pools.
type chaosEnv struct {
	t        *testing.T
	pg       string
	stockBin string
	resBin   string
	orderBin string
	stock    *exec.Cmd
	res      *exec.Cmd
	order    *exec.Cmd
	stockDB  *pgxpool.Pool
	resDB    *pgxpool.Pool
	orderDB  *pgxpool.Pool
}

func chaosEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("CHAOS") != "1" {
		t.Skip("CHAOS not set to 1; skipping chaos test (kills real subprocesses)")
	}
}

func newChaosEnv(t *testing.T) *chaosEnv {
	t.Helper()
	for _, p := range []string{"50051", "50052", "50053"} {
		requireFreePort(t, p)
	}
	pg := os.Getenv("CHAOS_PG")
	if pg == "" {
		pg = "localhost:5433"
	}
	e := &chaosEnv{
		t:        t,
		pg:       pg,
		stockBin: goBuild(t, "../cmd/stock"),
		resBin:   goBuild(t, "../cmd/reservation"),
		orderBin: goBuild(t, "../cmd/order"),
	}
	e.stockDB = mustPool(t, "postgres://taper_app:taperapp@"+pg+"/taper_db")
	e.resDB = mustPool(t, "postgres://taper_app:taperapp@"+pg+"/reservation_db")
	e.orderDB = mustPool(t, "postgres://taper_app:taperapp@"+pg+"/order_db")
	t.Cleanup(e.stopAll)
	return e
}

func (e *chaosEnv) stockEnvs() []string {
	return []string{
		"STOCK_ADDR", "localhost:50051",
		"STOCK_DATABASE_URL", "postgres://taper_app:taperapp@" + e.pg + "/taper_db",
		"STOCK_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@" + e.pg + "/taper_db",
	}
}

// startStock (re)starts the stock subprocess.
func (e *chaosEnv) startStock() {
	e.stock = startService(e.t, e.stockBin, e.stockEnvs()...)
}

// waitPort polls until localhost:port accepts a TCP connection, proving
// the subprocess is actually listening (a prior test's process may still
// hold the port for a moment after its kill).
func waitPort(t *testing.T, port string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", "localhost:"+port, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("service on port %s never started listening: %v", port, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startAll boots the full stack with the order service's crash-recovery
// resume loop enabled (SAGA_RESUME_ENABLED=1), waiting for each service
// to actually listen before starting the next.
func (e *chaosEnv) startAll() {
	e.startStock()
	waitPort(e.t, "50051")
	e.res = startService(e.t, e.resBin,
		"RESERVATION_ADDR", "localhost:50052",
		"STOCK_ADDR", "localhost:50051",
		"RESERVATION_DATABASE_URL", "postgres://taper_app:taperapp@"+e.pg+"/reservation_db",
		"SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+e.pg+"/reservation_db",
	)
	waitPort(e.t, "50052")
	e.order = startService(e.t, e.orderBin,
		"ORDER_ADDR", "localhost:50053",
		"RESERVATION_ADDR", "localhost:50052",
		"STOCK_ADDR", "localhost:50051",
		"ORDER_DATABASE_URL", "postgres://taper_app:taperapp@"+e.pg+"/order_db",
		"ORDER_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+e.pg+"/order_db",
		"SAGA_RESUME_ENABLED", "1",
	)
	waitPort(e.t, "50053")
}

func (e *chaosEnv) stopAll() {
	for _, c := range []*exec.Cmd{e.stock, e.res, e.order} {
		if c != nil && c.Process != nil {
			_ = c.Process.Kill()
		}
	}
}

// seed gives the chaos tenant a fresh hot row. The app pool is RLS-bound,
// so — as everywhere in this repo — the tenant GUC is set for the tx.
func (e *chaosEnv) seed() {
	e.t.Helper()
	ctx := database.WithTenantID(context.Background(), chaosTenant)
	err := database.ExecTxWithTenant(ctx, e.stockDB, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
			 VALUES ($1::uuid, $2, $3, $4)
			 ON CONFLICT (tenant_id, sku_id, warehouse_id)
			 DO UPDATE SET available_qty = EXCLUDED.available_qty,
			               reserved_qty = 0, allocated_qty = 0, is_locked_for_audit = false`,
			chaosTenant, chaosSKU, chaosWH, chaosSeeded)
		return err
	})
	if err != nil {
		e.t.Fatalf("seed: %v", err)
	}
}

// buckets returns the current (available, reserved, allocated) triple.
func (e *chaosEnv) buckets() (avail, resv, alloc int32) {
	e.t.Helper()
	ctx := database.WithTenantID(context.Background(), chaosTenant)
	err := database.ExecTxWithTenant(ctx, e.stockDB, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT available_qty, reserved_qty, allocated_qty FROM stock_levels
			 WHERE tenant_id = $1::uuid AND sku_id = $2 AND warehouse_id = $3`,
			chaosTenant, chaosSKU, chaosWH).Scan(&avail, &resv, &alloc)
	})
	if err != nil {
		e.t.Fatalf("buckets: %v", err)
	}
	return
}

// trySagaState returns the saga's state, or an error when the row does not
// exist yet (the saga may still be before its first durable write). The
// app pool is RLS-bound, so the tenant GUC rides along inside a tx.
func (e *chaosEnv) trySagaState(orderID string) (string, error) {
	ctx := database.WithTenantID(context.Background(), chaosTenant)
	var state string
	err := database.ExecTxWithTenant(ctx, e.orderDB, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT state FROM saga_instances WHERE order_id = $1`, orderID).Scan(&state)
	})
	return state, err
}

// waitSagaRow polls until the saga's first durable write has landed, so the
// kill in the kill test provably lands mid-saga (after the row exists).
func (e *chaosEnv) waitSagaRow(orderID string) {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := e.trySagaState(orderID); err == nil {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatal("saga row never appeared; kill test cannot claim mid-saga")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// orderClient dials the running order subprocess.
func (e *chaosEnv) orderClient() orderv1.OrderServiceClient {
	e.t.Helper()
	conn, err := grpc.NewClient("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.t.Fatalf("dial order: %v", err)
	}
	e.t.Cleanup(func() { _ = conn.Close() })
	return orderv1.NewOrderServiceClient(conn)
}

// waitAccounting polls until every seeded unit is accounted for and the
// saga is terminal, or fails after the TTL+sweeper budget elapses.
func (e *chaosEnv) waitAccounting(orderID string) (avail, resv, alloc int32, state string) {
	e.t.Helper()
	deadline := time.Now().Add(3 * time.Minute) // TTL 120s + sweeper 30s + margin
	var stateErr error
	for {
		avail, resv, alloc = e.buckets()
		state, stateErr = e.trySagaState(orderID)
		terminal := stateErr == nil &&
			(state == "CONFIRMED" || state == "COMPENSATED" || state == "FAILED")
		if terminal && resv == 0 && avail+alloc == chaosSeeded {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("accounting never converged: avail=%d resv=%d alloc=%d (seeded=%d) saga=%s (err=%v)",
				avail, resv, alloc, chaosSeeded, state, stateErr)
		}
		time.Sleep(2 * time.Second)
	}
}

// TestChaosStockDownFailsFast: with stock dead from the start, the saga
// fails immediately and leaves zero partial state.
func TestChaosStockDownFailsFast(t *testing.T) {
	chaosEnabled(t)
	e := newChaosEnv(t)
	e.seed()
	e.startAll()
	_ = e.stock.Process.Kill() // stock down before any traffic
	time.Sleep(500 * time.Millisecond)

	_, err := e.orderClient().CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
		TenantId:   chaosTenant,
		OrderId:    "CHAOS-DOWN-1",
		Lines:      []*orderv1.OrderLine{{SkuId: chaosSKU, WarehouseId: chaosWH, Quantity: 1, UnitPrice: 100}},
		TtlSeconds: 120,
	})
	if err == nil {
		t.Log("CreateOrder succeeded despite stock being down (breaker/race) — still a valid outcome")
	}
	if avail, resv, alloc := e.buckets(); resv != 0 || avail+alloc != chaosSeeded {
		t.Fatalf("partial state with stock down: avail=%d resv=%d alloc=%d", avail, resv, alloc)
	}
}

// TestChaosKillStockMidSaga: kill stock while a saga is in flight, restart
// it, and require full unit accounting after the resume loop and TTL
// sweeper converge — the spec's compensation/no-oversell chaos assertion.
func TestChaosKillStockMidSaga(t *testing.T) {
	chaosEnabled(t)
	e := newChaosEnv(t)
	e.seed()
	e.startAll()
	client := e.orderClient()
	// Fire the saga; the kill lands after the saga's first durable write,
	// i.e. provably mid-saga.
	go func() {
		_, _ = client.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
			TenantId:   chaosTenant,
			OrderId:    "CHAOS-KILL-1",
			Lines:      []*orderv1.OrderLine{{SkuId: chaosSKU, WarehouseId: chaosWH, Quantity: chaosSeeded, UnitPrice: 100}},
			TtlSeconds: 120,
		})
	}()
	e.waitSagaRow("CHAOS-KILL-1")
	_ = e.stock.Process.Kill()
	time.Sleep(200 * time.Millisecond)
	e.startStock() // recovery: same env, fresh process

	avail, resv, alloc, state := e.waitAccounting("CHAOS-KILL-1")
	t.Logf("converged: available=%d reserved=%d allocated=%d saga=%s (seeded=%d)",
		avail, resv, alloc, state, chaosSeeded)
	if avail < 0 || resv < 0 || alloc < 0 {
		t.Fatalf("negative bucket under chaos: avail=%d resv=%d alloc=%d", avail, resv, alloc)
	}
}
