package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/reservationservice"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/database"
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

	provs, err := obs.Setup(ctx, obs.Config{
		ServiceName:  "reservation",
		OTLPEndpoint: config.EnvOr("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("observability setup: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provs.Shutdown(shutdownCtx); err != nil {
			log.Printf("observability shutdown: %v", err)
		}
	}()

	metricsSrv := obs.MetricsServer(provs.Metrics, metricsAddr)
	go func() {
		log.Printf("reservation metrics listening on %s", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics serve: %v", err)
		}
	}()
	defer metricsSrv.Close() //nolint:errcheck // admin endpoint at exit

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

	conn, err := grpc.NewClient(stockAddr,
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
