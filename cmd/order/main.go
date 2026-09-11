package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/orderservice"
	"github.com/seifsheikhelarab/taper/pkg/config"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/outboxprune"
	"github.com/seifsheikhelarab/taper/pkg/payment"
)

func main() {
	ctx := context.Background()
	addr := config.EnvOr("ORDER_ADDR", ":50053")
	dsn := config.EnvOr("ORDER_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/order_db")
	sweeperDSN := config.EnvOr("ORDER_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@localhost:5432/order_db")
	resAddr := config.EnvOr("RESERVATION_ADDR", "localhost:50052")
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9103")

	provs, err := obs.Setup(context.Background(), obs.Config{
		ServiceName:  "order",
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
		log.Printf("order metrics listening on %s", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics serve: %v", err)
		}
	}()
	defer metricsSrv.Close() //nolint:errcheck // admin endpoint at exit

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	sweeperPool, err := pgxpool.New(context.Background(), sweeperDSN)
	if err != nil {
		log.Fatalf("connect sweeper db: %v", err)
	}
	defer sweeperPool.Close()

	resConn, err := grpc.NewClient(resAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial reservation: %v", err)
	}
	defer resConn.Close()

	stockConn, err := grpc.NewClient(stockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	defer stockConn.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// ADR-0002: batched outbox retention via the BYPASSRLS sweeper pool
	// (off unless OUTBOX_PRUNE_ENABLED).
	outboxprune.StartFromEnv(context.Background(), sweeperPool, log.Printf)

	saga := orderservice.NewSaga(
		pool,
		sweeperPool,
		resv1.NewReservationServiceClient(resConn),
		stockv1.NewStockServiceClient(stockConn),
		payment.NewSandbox(),
	)

	// Crash recovery (spec #44, T5): periodically resume non-terminal
	// sagas (PENDING_PAYMENT/RESERVED) so a killed or restarted order
	// process leaves no orphaned stock holds. Every step is idempotently
	// keyed, so replaying after a crash is exactly-once per dependency.
	// Off unless SAGA_RESUME_ENABLED=1 (the reservation TTL sweeper
	// remains the independent backstop).
	if config.EnvOr("SAGA_RESUME_ENABLED", "") == "1" {
		go func() {
			for {
				if err := saga.ResumePendingSagas(ctx, 100); err != nil {
					log.Printf("saga resume: %v", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
				}
			}
		}()
	}

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(provs.UnaryServerInterceptor()))
	orderv1.RegisterOrderServiceServer(srv, saga)
	log.Printf("order service listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
