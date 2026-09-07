package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/orderservice"
	"github.com/seifsheikhelarab/taper/internal/reservationservice"
	"github.com/seifsheikhelarab/taper/internal/stockservice"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"github.com/seifsheikhelarab/taper/pkg/payment"
)

const (
	stockDSN        = "postgres://taper_app:taperapp@localhost:5432/taper_db"
	reservationDSN  = "postgres://taper_app:taperapp@localhost:5432/reservation_db"
	orderDSN        = "postgres://taper_app:taperapp@localhost:5432/order_db"
	sweeperDSN      = "postgres://taper_sweeper:tapersweeper@localhost:5432/reservation_db"
	orderSweeperDSN = "postgres://taper_sweeper:tapersweeper@localhost:5432/order_db"
	tenantA         = "11111111-1111-1111-1111-111111111111"
	tenantB         = "22222222-2222-2222-2222-222222222222"
)

type testEnv struct {
	ctx           context.Context
	cancel        context.CancelFunc
	stock         stockv1.StockServiceClient
	reservation   resv1.ReservationServiceClient
	order         orderv1.OrderServiceClient
	stockConn     *grpc.ClientConn
	resConn       *grpc.ClientConn
	orderConn     *grpc.ClientConn
	stockPool     *pgxpool.Pool
	resPool       *pgxpool.Pool
	orderPool     *pgxpool.Pool
	sweeperPool   *pgxpool.Pool
	resSrv        *grpc.Server
	resListener   *bufconn.Listener
	resSrvStopped bool
	saga          *orderservice.Saga
}

// resumeSagas invokes ResumePendingSagas on the in-process order server.
func (e *testEnv) resumeSagas(t *testing.T, limit int32) error {
	t.Helper()
	return e.saga.ResumePendingSagas(e.ctx, limit)
}

// breakReservation stops the in-process reservation server to simulate a
// dependency outage for circuit-breaker tests.
func (e *testEnv) breakReservation(t *testing.T) {
	t.Helper()
	if !e.resSrvStopped {
		e.resSrv.Stop()
		e.resSrvStopped = true
	}
}

// fixReservation restarts the in-process reservation server after an outage.
func (e *testEnv) fixReservation(t *testing.T) {
	t.Helper()
	if e.resSrvStopped {
		go func() { _ = e.resSrv.Serve(e.resListener) }()
		e.resSrvStopped = false
	}
}

func setup(t *testing.T) *testEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	stockPool := mustPool(t, stockDSN)
	resPool := mustPool(t, reservationDSN)
	orderPool := mustPool(t, orderDSN)
	sweeperPool := mustPool(t, sweeperDSN)
	orderSweeperPool := mustPool(t, orderSweeperDSN)
	if err := truncate(t, stockPool, "stock_levels", "stock_events", "outbox", "processed_idempotency_keys"); err != nil {
		t.Fatalf("truncate stock: %v", err)
	}
	if err := truncate(t, resPool, "reservations", "outbox", "processed_idempotency_keys"); err != nil {
		t.Fatalf("truncate reservation: %v", err)
	}
	if err := truncate(t, orderPool, "orders", "saga_instances", "order_lines", "outbox", "processed_idempotency_keys"); err != nil {
		t.Fatalf("truncate order: %v", err)
	}

	stockSrv := grpc.NewServer()
	stockv1.RegisterStockServiceServer(stockSrv, stockservice.NewServer(stockPool))

	stockLis := bufconn.Listen(1024 * 1024)
	go func() { _ = stockSrv.Serve(stockLis) }()
	t.Cleanup(stockSrv.Stop)

	stockConn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return stockLis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial stock: %v", err)
	}
	t.Cleanup(func() { _ = stockConn.Close() })

	orderPool2 := orderPool // keep linters happy about shadowing below
	_ = orderPool2

	resSrv := grpc.NewServer()
	resImpl := reservationservice.NewServer(resPool, stockv1.NewStockServiceClient(stockConn))
	resv1.RegisterReservationServiceServer(resSrv, resImpl)
	resLis := bufconn.Listen(1024 * 1024)
	go func() { _ = resSrv.Serve(resLis) }()
	t.Cleanup(resSrv.Stop)

	resConn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return resLis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial reservation: %v", err)
	}
	t.Cleanup(func() { _ = resConn.Close() })

	// Order service (saga) on top of reservation + stock + sandbox payment.
	// The order sweeper pool uses the BYPASSRLS role for cross-tenant resume scans.
	sagaImpl := orderservice.NewSaga(
		orderPool,
		orderSweeperPool,
		resv1.NewReservationServiceClient(resConn),
		stockv1.NewStockServiceClient(stockConn),
		payment.NewSandbox(),
	)
	orderSrv := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(orderSrv, sagaImpl)
	orderLis := bufconn.Listen(1024 * 1024)
	go func() { _ = orderSrv.Serve(orderLis) }()
	t.Cleanup(orderSrv.Stop)

	orderConn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return orderLis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial order: %v", err)
	}
	t.Cleanup(func() { _ = orderConn.Close() })

	return &testEnv{
		ctx:         ctx,
		cancel:      cancel,
		stock:       stockv1.NewStockServiceClient(stockConn),
		reservation: resv1.NewReservationServiceClient(resConn),
		order:       orderv1.NewOrderServiceClient(orderConn),
		stockConn:   stockConn,
		resConn:     resConn,
		orderConn:   orderConn,
		stockPool:   stockPool,
		resPool:     resPool,
		orderPool:   orderPool,
		sweeperPool: sweeperPool,
		resSrv:      resSrv,
		resListener: resLis,
		saga:        sagaImpl,
	}
}

func (e *testEnv) seed(t *testing.T, tenant, sku, warehouse string, qty int32) {
	t.Helper()
	resp, err := e.stock.AdjustStock(context.Background(), &stockv1.AdjustStockRequest{
		TenantId:      tenant,
		SkuId:         sku,
		WarehouseId:   warehouse,
		QuantityDelta: qty,
		Reason:        "test_seed",
		Source:        "test",
	})
	if err != nil {
		t.Fatalf("seed adjust: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("seed adjust failed")
	}
}

func (e *testEnv) stockLevel(t *testing.T, tenant, sku, warehouse string) (available, reserved, allocated int32) {
	t.Helper()
	ctx := database.WithTenantID(context.Background(), tenant)
	err := database.ExecTxWithTenant(ctx, e.stockPool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT available_qty, reserved_qty, allocated_qty FROM stock_levels WHERE tenant_id=$1::uuid AND sku_id=$2 AND warehouse_id=$3",
			tenant, sku, warehouse).Scan(&available, &reserved, &allocated)
	})
	if err != nil {
		t.Fatalf("read stock level %s/%s: %v", sku, warehouse, err)
	}
	return
}

func (e *testEnv) outboxCount(t *testing.T, pool *pgxpool.Pool, tenant, eventType string) int {
	t.Helper()
	var n int
	ctx := database.WithTenantID(context.Background(), tenant)
	err := database.ExecTxWithTenant(ctx, pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox WHERE tenant_id=$1::uuid AND event_type=$2", tenant, eventType).Scan(&n)
	})
	if err != nil {
		t.Fatalf("count outbox %s: %v", eventType, err)
	}
	return n
}

func (e *testEnv) countReservations(t *testing.T, tenant, orderID string) int {
	t.Helper()
	var rows int
	ctx := database.WithTenantID(context.Background(), tenant)
	err := database.ExecTxWithTenant(ctx, e.resPool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT COUNT(*) FROM reservations WHERE tenant_id=$1::uuid AND order_id=$2", tenant, orderID).Scan(&rows)
	})
	if err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	return rows
}

func mustPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func truncate(t *testing.T, pool *pgxpool.Pool, tables ...string) error {
	t.Helper()
	for _, tbl := range tables {
		if _, err := pool.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s RESTART IDENTITY CASCADE", tbl)); err != nil {
			return err
		}
	}
	return nil
}

var _ = time.Second
