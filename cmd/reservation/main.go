package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/reservationservice"
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"github.com/seifsheikhelarab/taper/pkg/grpcx"
	"github.com/seifsheikhelarab/taper/pkg/idemprune"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/outboxprune"
)

func main() {
	addr := config.EnvOr("RESERVATION_ADDR", ":50052")
	dsn := config.EnvOr("RESERVATION_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/reservation_db")
	sweeperDSN := config.EnvOr("SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@localhost:5432/reservation_db")
	stockAddr := config.EnvOr("STOCK_ADDR", ":50051")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9102")

	c := closer.New(closer.DrainWindow())

	// Structured JSON logs with trace correlation (spec #52, US22): the
	// stdlib logger routes through slog, so existing log.Printf call sites
	// render as correlated JSON.
	obs.SetDefaultLogger("reservation")

	provs, stopObs := obs.MustRun("reservation", metricsAddr)
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

	// grpcx dials opt into client-side round_robin (docs/research/0002):
	// Reserve calls are idempotency-keyed, so spreading across replicas is
	// safe. Transport posture comes from the GRPC_TLS_* / GRPC_INSECURE env
	// surface (spec #52, B2).
	conn, err := grpcx.DialFromEnv(stockAddr,
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	c.Defer(func(context.Context) error { _ = conn.Close(); return nil })
	stock := stockv1.NewStockServiceClient(conn)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	// Listener posture: TLS when GRPC_TLS_CERT/KEY are set, plaintext dev
	// default otherwise. A misconfiguration exits rather than falling back.
	tlsOpt, _, err := grpcx.ServerOptionFromEnv()
	if err != nil {
		log.Fatalf("tls: %v", err)
	}
	opts := []grpc.ServerOption{grpc.ChainUnaryInterceptor(provs.UnaryServerInterceptor())}
	if tlsOpt != nil {
		opts = append(opts, tlsOpt)
	}

	srv := grpc.NewServer(opts...)
	resv1.RegisterReservationServiceServer(srv, reservationservice.NewServer(pool, stock))

	// TTL sweeper: the independent backstop for expired holds. Root ctx
	// gives prompt stop; idempotency makes re-running safe.
	sweeper := reservationservice.NewSweeper(sweeperPool, stock)
	go sweeper.Run(c.Context())

	// ADR-0002: batched outbox retention via the BYPASSRLS sweeper pool
	// (off unless OUTBOX_PRUNE_ENABLED).
	outboxprune.StartFromEnv(c.Context(), sweeperPool, log.Printf)

	// Spec #52 (US18): retention owner for processed_idempotency_keys
	// (off unless IDEMPOTENCY_PRUNE_ENABLED).
	idemprune.StartFromEnv(c.Context(), sweeperPool, log.Printf)

	// Readiness (spec #52, B1): Postgres via both roles, stock downstream.
	provs.RegisterReadiness("postgres", closer.Ping(pool))
	provs.RegisterReadiness("postgres-sweeper", closer.Ping(sweeperPool))
	provs.RegisterReadiness("stock", closer.GRPCReach(conn))

	log.Printf("reservation service listening on %s", addr)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	c.DeferFirst(closer.Drain(closer.DeferGRPC(srv), func() error { return <-serveErr }))

	osExit(c.Wait())
}

// osExit is a test seam over os.Exit: 0 returns normally (process exit 0),
// anything else is fatal.
var osExit = func(code int) {
	if code != 0 {
		log.Fatalf("reservation: unclean exit %d", code)
	}
}
