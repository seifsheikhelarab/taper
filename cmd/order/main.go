package main

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/orderservice"
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"github.com/seifsheikhelarab/taper/pkg/grpcx"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/outboxprune"
	"github.com/seifsheikhelarab/taper/pkg/payment"
)

func main() {
	addr := config.EnvOr("ORDER_ADDR", ":50053")
	dsn := config.EnvOr("ORDER_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/order_db")
	sweeperDSN := config.EnvOr("ORDER_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@localhost:5432/order_db")
	resAddr := config.EnvOr("RESERVATION_ADDR", "localhost:50052")
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9103")

	c := closer.New(closer.DrainWindow())

	provs, stopObs := obs.MustRun("order", metricsAddr)
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

	// grpcx.Dial opts into client-side round_robin (docs/research/0002):
	// saga steps are idempotency-keyed, so spreading across replicas is safe.
	resConn, err := grpcx.Dial(resAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial reservation: %v", err)
	}
	stockConn, err := grpcx.Dial(stockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	c.Defer(func(context.Context) error {
		_ = stockConn.Close()
		_ = resConn.Close()
		return nil
	})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// ADR-0002: batched outbox retention via the BYPASSRLS sweeper pool
	// (off unless OUTBOX_PRUNE_ENABLED).
	outboxprune.StartFromEnv(c.Context(), sweeperPool, log.Printf)

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
	// remains the independent backstop). Root ctx gives prompt stop.
	if config.EnvOr("SAGA_RESUME_ENABLED", "") != "" &&
		config.EnvOr("SAGA_RESUME_ENABLED", "") != "0" {
		go func() {
			for {
				if err := saga.ResumePendingSagas(c.Context(), 100); err != nil {
					log.Printf("saga resume: %v", err)
				}
				select {
				case <-c.Context().Done():
					return
				case <-time.After(10 * time.Second):
				}
			}
		}()
	}

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(provs.UnaryServerInterceptor()))
	orderv1.RegisterOrderServiceServer(srv, saga)

	log.Printf("order service listening on %s", addr)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	c.DeferFirst(closer.Drain(closer.DeferGRPC(srv), func() error { return <-serveErr }))

	osExit(c.Wait())
}

// osExit is a test seam over os.Exit: 0 returns normally (process exit 0),
// anything else is fatal.
var osExit = func(code int) {
	if code != 0 {
		log.Fatalf("order: unclean exit %d", code)
	}
}
