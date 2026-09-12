package integration

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/seifsheikhelarab/taper/pkg/database"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Phase 6 integration tests (spec #52, Testing Decisions a+b):
//
// (a) graceful-drain: SIGTERM to a service under live traffic stops the
// listener, finishes in-flight work, and exits 0 with the saga invariant
// intact — an in-flight hold is either completed or compensated, never
// dropped (CONTEXT.md Unit Accounting).
//
// (b) readiness: /readyz flips 503 while /healthz (liveness) stays 200 when
// a dependency dies, and back to 200 when it returns.
//
// Both gate behind DRAIN=1 (they build and kill real subprocesses; the
// chaos suite's precedent). The drain test additionally requires POSIX
// signal delivery (os.Interrupt is a no-op kill signal on windows), so it
// skips there; CI runs Linux.

func stackEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("DRAIN") != "1" {
		t.Skip("DRAIN not set to 1; skipping (builds and kills real subprocesses)")
	}
}

func drainEnabled(t *testing.T) {
	t.Helper()
	stackEnabled(t)
	if runtime.GOOS == "windows" {
		t.Skip("POSIX SIGTERM delivery unavailable on windows; skipping drain test")
	}
}

// drainEnv is a minimal stack: stock + reservation + order binaries against
// the compose Postgres (host port 5433 default, CHAOS_PG-style override).
type drainEnv struct {
	t        *testing.T
	pg       string
	stockBin string
	resBin   string
	orderBin string
	stock    *exec.Cmd
	res      *exec.Cmd
	order    *exec.Cmd
}

func newDrainEnv(t *testing.T) *drainEnv {
	t.Helper()
	for _, p := range []string{"50051", "50052", "50053", "9151"} {
		requireFreePort(t, p)
	}
	pg := os.Getenv("CHAOS_PG")
	if pg == "" {
		pg = "localhost:5433"
	}
	e := &drainEnv{
		t:        t,
		pg:       pg,
		stockBin: goBuild(t, "../cmd/stock"),
		resBin:   goBuild(t, "../cmd/reservation"),
		orderBin: goBuild(t, "../cmd/order"),
	}
	t.Cleanup(e.killAll)
	return e
}

func (e *drainEnv) startAll() {
	e.stock = startService(e.t, e.stockBin,
		"STOCK_ADDR", "localhost:50051",
		"STOCK_DATABASE_URL", "postgres://taper_app:taperapp@"+e.pg+"/taper_db",
		"STOCK_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+e.pg+"/taper_db",
		"METRICS_ADDR", "localhost:9151",
	)
	waitPort(e.t, "50051")
	e.res = startService(e.t, e.resBin,
		"RESERVATION_ADDR", "localhost:50052",
		"STOCK_ADDR", "localhost:50051",
		"RESERVATION_DATABASE_URL", "postgres://taper_app:taperapp@"+e.pg+"/reservation_db",
		"SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+e.pg+"/reservation_db",
		"METRICS_ADDR", "localhost:9152",
	)
	waitPort(e.t, "50052")
	e.startOrder()
	waitPort(e.t, "50053")
}

// startOrder (re)starts the order subprocess with crash-recovery resume
// enabled, so an in-flight saga interrupted by the drain is settled on boot.
func (e *drainEnv) startOrder() {
	e.order = startService(e.t, e.orderBin,
		"ORDER_ADDR", "localhost:50053",
		"RESERVATION_ADDR", "localhost:50052",
		"STOCK_ADDR", "localhost:50051",
		"ORDER_DATABASE_URL", "postgres://taper_app:taperapp@"+e.pg+"/order_db",
		"ORDER_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+e.pg+"/order_db",
		"METRICS_ADDR", "localhost:9153",
		"SAGA_RESUME_ENABLED", "1",
	)
}

func (e *drainEnv) killAll() {
	for _, c := range []*exec.Cmd{e.stock, e.res, e.order} {
		if c != nil && c.Process != nil {
			_ = c.Process.Kill()
		}
	}
}

// orderClient dials the running order service.
func (e *drainEnv) orderClient(t *testing.T) orderv1.OrderServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial order: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return orderv1.NewOrderServiceClient(conn)
}

// sigtermAndWait sends SIGINT (handled identically to SIGTERM by pkg/closer)
// to a subprocess and waits for it to exit, returning the exit code.
func (e *drainEnv) sigtermAndWait(c *exec.Cmd) int {
	t := e.t
	if err := c.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal order process: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case <-done:
		return c.ProcessState.ExitCode()
	case <-time.After(30 * time.Second):
		t.Fatalf("process did not exit within 30s of SIGTERM")
		return -1
	}
}

// TestGracefulDrainUnderTraffic: start the stack, seed, fire one saga,
// SIGTERM the order service while it is in flight, then verify
// (1) the process exits cleanly (0) inside the drain window and (2) the
// saga invariant holds after resume — the in-flight hold was either
// completed or compensated, never dropped (available + allocated == seeded
// and reserved == 0 after quiescence).
func TestGracefulDrainUnderTraffic(t *testing.T) {
	drainEnabled(t)
	e := newDrainEnv(t)
	e.startAll()

	// Seed via the stock service DB (RLS-safe, chaos-suite convention).
	const seed = int32(10)
	seedTenant, seedSKU := chaosTenant, "DRAIN-SKU"
	seedStockRow(t, e.pg, seedTenant, seedSKU, chaosWH, seed)

	client := e.orderClient(t)

	// Fire a saga in a goroutine; SIGTERM the order service 200ms in, while
	// it is inside reserve/pay/allocate.
	type sagaResult struct {
		err    error
		status string
	}
	sagaDone := make(chan sagaResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
			TenantId: seedTenant,
			OrderId:  "drain-ord-1",
			Lines: []*orderv1.OrderLine{{
				SkuId: seedSKU, WarehouseId: chaosWH, Quantity: 4, UnitPrice: 100,
			}},
			IdempotencyKey: "drain-ord-1",
		})
		sagaDone <- sagaResult{err: err, status: resp.GetStatus()}
	}()
	time.Sleep(200 * time.Millisecond)
	code := e.sigtermAndWait(e.order)

	// (1) Clean exit within the drain window (TAPER_DRAIN default 15s).
	if code != 0 {
		t.Fatalf("order process exit code = %d, want 0 after graceful drain", code)
	}

	// The in-flight call may legitimately fail with a transport error (the
	// listener closed mid-call) or complete — the client-side outcome is
	// unconstrained; the durable state is what must stay consistent.
	res := <-sagaDone
	t.Logf("in-flight saga outcome: err=%v status=%q", res.err, res.status)

	// (2) Restart the order service and let resume/quiescence settle, then
	// assert the invariant: no orphaned holds.
	e.startOrder()
	waitPort(e.t, "50053")
	time.Sleep(12 * time.Second) // resume pass + sweeper settle

	assertUnitAccounting(t, e.pg, seedTenant, seedSKU, chaosWH, seed)
}

// TestReadinessFlipsWithDependency: pause Postgres (the one dependency every
// service holds) and watch /readyz fail while /healthz stays 200; unpause
// and watch readiness recover.
func TestReadinessFlipsWithDependency(t *testing.T) {
	stackEnabled(t)
	e := newDrainEnv(t)
	e.startAll()

	probe := func(path string) int {
		resp, err := http.Get("http://localhost:9151" + path) //nolint:gosec // test URL
		if err != nil {
			return -1
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// Healthy baseline.
	if got := probe("/readyz"); got != http.StatusOK {
		t.Fatalf("baseline /readyz = %d, want 200", got)
	}
	if got := probe("/healthz"); got != http.StatusOK {
		t.Fatalf("baseline /healthz = %d, want 200", got)
	}

	pausePostgres(t, true)
	defer pausePostgres(t, false)

	// Readiness must fail while liveness must not.
	deadline := time.Now().Add(30 * time.Second)
	readyFailed := false
	for time.Now().Before(deadline) {
		if probe("/readyz") == http.StatusServiceUnavailable {
			readyFailed = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !readyFailed {
		t.Fatal("/readyz never flipped to 503 after the dependency died")
	}
	if got := probe("/healthz"); got != http.StatusOK {
		t.Fatalf("liveness /healthz = %d while dependency down, want 200 (static)", got)
	}

	// Recovery: bring the dependency back.
	pausePostgres(t, false)
	deadline = time.Now().Add(30 * time.Second)
	readyOK := false
	for time.Now().Before(deadline) {
		if probe("/readyz") == http.StatusOK {
			readyOK = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !readyOK {
		t.Fatal("/readyz never recovered to 200 after the dependency returned")
	}
}

// pausePostgres pauses/unpauses the compose Postgres container. go test runs
// in the package dir, so compose must be pointed at the repo root. Unpause
// is idempotent (a repeated unpause is a no-op, not an error).
func pausePostgres(t *testing.T, pause bool) {
	t.Helper()
	verb, want := "unpause", "unpaused"
	if pause {
		verb, want = "pause", "paused"
	}
	cmd := exec.Command("docker", "compose", verb, "postgres")
	cmd.Dir = ".."
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Unpause is idempotent: a second unpause (mid-test + deferred
		// cleanup) finds the container already running and must not fail
		// the test.
		if verb == "unpause" && strings.Contains(string(out), "is not paused") {
			return
		}
		t.Fatalf("%s postgres: %v (want %s)\n%s", verb, err, want, out)
	}
}

// seedStockRow inserts/overwrites a hot stock row (RLS-bound pool, so the
// tenant GUC rides along inside a tx — chaos-suite convention).
func seedStockRow(t *testing.T, pg, tenant, sku, wh string, qty int32) {
	t.Helper()
	pool := mustPool(t, "postgres://taper_app:taperapp@"+pg+"/taper_db")
	ctx := database.WithTenantID(context.Background(), tenant)
	err := database.ExecTxWithTenant(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO stock_levels (tenant_id, sku_id, warehouse_id, available_qty)
			 VALUES ($1::uuid, $2, $3, $4)
			 ON CONFLICT (tenant_id, sku_id, warehouse_id)
			 DO UPDATE SET available_qty = EXCLUDED.available_qty,
			               reserved_qty = 0, allocated_qty = 0, is_locked_for_audit = false`,
			tenant, sku, wh, qty)
		return err
	})
	if err != nil {
		t.Fatalf("seed stock: %v", err)
	}
}

// assertUnitAccounting polls until the CONTEXT.md Unit Accounting invariant
// holds: available + allocated == seeded and reserved == 0 (the in-flight
// hold was completed or compensated, never dropped).
func assertUnitAccounting(t *testing.T, pg, tenant, sku, wh string, seeded int32) {
	t.Helper()
	pool := mustPool(t, "postgres://taper_app:taperapp@"+pg+"/taper_db")
	ctx := database.WithTenantID(context.Background(), tenant)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var avail, res, alloc int32
		err := database.ExecTxWithTenant(ctx, pool, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx,
				`SELECT available_qty, reserved_qty, allocated_qty FROM stock_levels
				 WHERE tenant_id = $1::uuid AND sku_id = $2 AND warehouse_id = $3`,
				tenant, sku, wh).Scan(&avail, &res, &alloc)
		})
		if err == nil && res == 0 && avail+alloc == seeded {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("unit accounting violated after drain: available=%d reserved=%d allocated=%d, want avail+alloc=%d reserved=0 (last err=%v)",
				avail, res, alloc, seeded, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
