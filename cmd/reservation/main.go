package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/reservationservice"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"github.com/seifsheikhelarab/taper/pkg/grpcx"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/outboxprune"
)

func main() {
	ctx := context.Background()
	addr := config.EnvOr("RESERVATION_ADDR", ":50052")
	dsn := config.EnvOr("RESERVATION_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/reservation_db")
	sweeperDSN := config.EnvOr("SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@localhost:5432/reservation_db")
	stockAddr := config.EnvOr("STOCK_ADDR", ":50051")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9102")

	provs, stopObs := obs.MustRun("reservation", metricsAddr)
	defer stopObs()

	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	sweeperPool, err := database.OpenPool(ctx, sweeperDSN)
	if err != nil {
		log.Fatalf("connect sweeper db: %v", err)
	}
	defer sweeperPool.Close()

	// grpcx.Dial opts into client-side round_robin (docs/research/0002):
	// Reserve calls are idempotency-keyed, so spreading across replicas is safe.
	conn, err := grpcx.Dial(stockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	defer conn.Close()
	stock := stockv1.NewStockServiceClient(conn)

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(provs.UnaryServerInterceptor()))
	resv1.RegisterReservationServiceServer(srv, reservationservice.NewServer(pool, stock))

	sweeper := reservationservice.NewSweeper(sweeperPool, stock)
	go sweeper.Run(ctx)

	// ADR-0002: batched outbox retention via the BYPASSRLS sweeper pool
	// (off unless OUTBOX_PRUNE_ENABLED).
	outboxprune.StartFromEnv(ctx, sweeperPool, log.Printf)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("reservation service listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
