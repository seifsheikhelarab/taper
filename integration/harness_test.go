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

	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/reservationservice"
	"github.com/seifsheikhelarab/taper/internal/stockservice"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

const (
	stockDSN       = "postgres://taper_app:taperapp@localhost:5432/taper_db"
	reservationDSN = "postgres://taper_app:taperapp@localhost:5432/reservation_db"
	sweeperDSN     = "postgres://taper_sweeper:tapersweeper@localhost:5432/reservation_db"
	tenantA        = "11111111-1111-1111-1111-111111111111"
	tenantB        = "22222222-2222-2222-2222-222222222222"
)

type testEnv struct {
	ctx         context.Context
	cancel      context.CancelFunc
	stock       stockv1.StockServiceClient
	reservation resv1.ReservationServiceClient
	stockPool   *pgxpool.Pool
	resPool     *pgxpool.Pool
	sweeperPool *pgxpool.Pool
}

func setup(t *testing.T) *testEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	stockPool := mustPool(t, stockDSN)
	resPool := mustPool(t, reservationDSN)
	sweeperPool := mustPool(t, sweeperDSN)
	if err := truncate(t, stockPool, "stock_levels", "stock_events", "outbox", "processed_idempotency_keys"); err != nil {
		t.Fatalf("truncate stock: %v", err)
	}
	if err := truncate(t, resPool, "reservations", "outbox", "processed_idempotency_keys"); err != nil {
		t.Fatalf("truncate reservation: %v", err)
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

	resSrv := grpc.NewServer()
	resv1.RegisterReservationServiceServer(resSrv, reservationservice.NewServer(resPool, stockv1.NewStockServiceClient(stockConn)))
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

	return &testEnv{
		ctx:         ctx,
		cancel:      cancel,
		stock:       stockv1.NewStockServiceClient(stockConn),
		reservation: resv1.NewReservationServiceClient(resConn),
		stockPool:   stockPool,
		resPool:     resPool,
		sweeperPool: sweeperPool,
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
