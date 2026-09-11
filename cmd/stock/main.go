package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/stockservice"
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

	provs, stopObs := obs.MustRun("stock", metricsAddr)
	defer stopObs()

	pool, err := database.OpenPool(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()
	sweeperPool, err := database.OpenPool(context.Background(), sweeperDSN)
	if err != nil {
		log.Fatalf("connect sweeper db: %v", err)
	}
	defer sweeperPool.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// ADR-0002: batched outbox retention (off unless OUTBOX_PRUNE_ENABLED).
	outboxprune.StartFromEnv(context.Background(), sweeperPool, log.Printf)

	// ADR-0001: nightly stock reconciliation with drift locking (off unless
	// RECONCILE_ENABLED).
	go stockservice.New(sweeperPool, log.Printf).Run(context.Background())

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(provs.UnaryServerInterceptor()))
	stockv1.RegisterStockServiceServer(srv, stockservice.NewServer(pool))
	log.Printf("stock service listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
