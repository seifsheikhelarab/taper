package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/stockservice"
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/database"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/outboxprune"
)

func main() {
	addr := config.EnvOr("STOCK_ADDR", ":50051")
	dsn := config.EnvOr("STOCK_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/taper_db")
	// Maintenance tasks (prune, reconcile) run cross-tenant and need the
	// BYPASSRLS sweeper role; the RLS-bound app pool would see no rows.
	sweeperDSN := config.EnvOr("STOCK_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@localhost:5432/taper_db")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9101")

	c := closer.New(closer.DrainWindow())

	provs, stopObs := obs.MustRun("stock", metricsAddr)
	c.Defer(func(context.Context) error { stopObs(); return nil })

	pool, err := database.OpenPool(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	sweeperPool, err := database.OpenPool(context.Background(), sweeperDSN)
	if err != nil {
		log.Fatalf("connect sweeper db: %v", err)
	}
	c.Defer(func(context.Context) error { sweeperPool.Close(); return nil })
	c.Defer(func(context.Context) error { pool.Close(); return nil })

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// ADR-0002: batched outbox retention via the BYPASSRLS sweeper pool
	// (off unless OUTBOX_PRUNE_ENABLED). Root ctx cancels on shutdown.
	outboxprune.StartFromEnv(c.Context(), sweeperPool, log.Printf)

	// ADR-0001: nightly stock reconciliation with drift locking (off unless
	// RECONCILE_ENABLED). Root ctx gives prompt stop on shutdown.
	go stockservice.New(sweeperPool, log.Printf).Run(c.Context())

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(provs.UnaryServerInterceptor()))
	stockv1.RegisterStockServiceServer(srv, stockservice.NewServer(pool))

	log.Printf("stock service listening on %s", addr)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	// Drain first (stop accepting, finish in-flight RPCs), then pools.
	c.DeferFirst(closer.Drain(closer.DeferGRPC(srv), func() error { return <-serveErr }))

	osExit(c.Wait())
}

// osExit is a test seam over os.Exit: 0 returns normally (process exit 0),
// anything else is fatal.
var osExit = func(code int) {
	if code != 0 {
		log.Fatalf("stock: unclean exit %d", code)
	}
}
