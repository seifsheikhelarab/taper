// Command gateway is the REST front for the internal gRPC services
// (Phase 4, spec #35): JWT tenant auth, per-tenant rate limiting, and
// Fail-Fast error mapping, per docs/research/0001-gateway-http-layer.md.
// Phase 5 adds tracing (client spans join the inbound HTTP-less trace via
// the gRPC interceptors) and Prometheus RED metrics.
// Phase 6 (spec #52) adds graceful shutdown with a bounded drain and the
// dependency-aware /readyz probe (liveness /healthz stays static 200).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/seifsheikhelarab/taper/internal/gateway"
	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/grpcx"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/ratelimit"
)

func main() {
	addr := config.EnvOr("GATEWAY_ADDR", ":8080")
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	resAddr := config.EnvOr("RESERVATION_ADDR", "localhost:50052")
	orderAddr := config.EnvOr("ORDER_ADDR", "localhost:50053")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9106")
	floor := config.EnvOrInt("GATEWAY_RATE_BUCKET_FLOOR", 0)

	// Spec #52, B2: required and numeric values fail fast at boot — a wrong
	// GATEWAY_RATE_PER_TENANT must not silently apply a default. Everything
	// wrong is reported in one shot.
	secret, errSecret := config.EnvRequired("GATEWAY_JWT_SECRET")
	rate, errRate := config.EnvFloat("GATEWAY_RATE_PER_TENANT")
	burst, errBurst := config.EnvFloat("GATEWAY_BURST_PER_TENANT")
	if err := config.Collect(errSecret, errRate, errBurst); err != nil {
		log.Fatalf("gateway config: %v", err)
	}

	c := closer.New(closer.DrainWindow())

	provs, stopObs := obs.MustRun("gateway", metricsAddr)
	c.Defer(func(context.Context) error { stopObs(); return nil })

	stockConn := dial(stockAddr, provs)
	resConn := dial(resAddr, provs)
	orderConn := dial(orderAddr, provs)
	// LIFO: conns close after the HTTP server has drained.
	c.Defer(func(context.Context) error {
		_ = orderConn.Close()
		_ = resConn.Close()
		_ = stockConn.Close()
		return nil
	})

	core := &gateway.Core{
		Verifier:           auth.NewSandbox([]byte(secret)),
		Limiter:            ratelimit.NewLimited(rate, burst, floor),
		StockBreaker:       circuitbreaker.New(circuitbreaker.Config{}),
		ReservationBreaker: circuitbreaker.New(circuitbreaker.Config{}),
		OrderBreaker:       circuitbreaker.New(circuitbreaker.Config{}),
	}
	// Fail-Fast breaker visibility (spec #44): publish each dependency's
	// breaker state as the taper_breaker_state gauge.
	core.BreakerGauge = provs.Metrics

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Readiness (spec #52, B1): every downstream gRPC dependency must be
	// reachable. /readyz on the API listener and the admin listener share
	// the same registry; /healthz stays dependency-free liveness.
	provs.RegisterReadiness("stock", closer.GRPCReach(stockConn))
	provs.RegisterReadiness("reservation", closer.GRPCReach(resConn))
	provs.RegisterReadiness("order", closer.GRPCReach(orderConn))
	mux.Handle("GET /readyz", provs.ReadyHandler())
	gateway.NewStockRoutes(core, stockConn).Mount(mux)
	gateway.NewReservationRoutes(core, resConn).Mount(mux)
	gateway.NewOrderRoutes(core, orderConn).Mount(mux)

	srv := &http.Server{Addr: addr, Handler: core.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}

	// TLS termination (spec #52, US11): GATEWAY_TLS_CERT/KEY enable
	// ServeTLS; plaintext otherwise. Half-set is a boot failure.
	serve := func() error { return srv.ListenAndServe() }
	if cert, key := os.Getenv("GATEWAY_TLS_CERT"), os.Getenv("GATEWAY_TLS_KEY"); cert != "" || key != "" {
		if cert == "" || key == "" {
			log.Fatalf("GATEWAY_TLS_CERT and GATEWAY_TLS_KEY must be set together")
		}
		serve = func() error { return srv.ListenAndServeTLS(cert, key) }
		log.Printf("gateway TLS enabled")
	}

	log.Printf("gateway listening on %s (stock=%s reservation=%s order=%s)", addr, stockAddr, resAddr, orderAddr)
	serveErr := make(chan error, 1)
	go func() { serveErr <- serve() }()

	// Drain runs after the conn-closing Defer above in LIFO order; the
	// server must stop accepting before its conns close, so register the
	// drain via DeferFirst instead.
	c.DeferFirst(closer.Drain(closer.DeferHTTP(srv), func() error { return <-serveErr }))

	osExit(c.Wait())
}

// osExit is a test seam over os.Exit: 0 returns normally (process exit 0),
// anything else is fatal.
var osExit = func(code int) {
	if code != 0 {
		log.Fatalf("gateway: unclean exit %d", code)
	}
}

func dial(addr string, provs *obs.Providers) *grpc.ClientConn {
	// grpcx.Dial opts into client-side round_robin (docs/research/0002):
	// DNS replicas are addressed per-call, so `docker compose --profile
	// containers up --scale stock=2` (or k8s replicas) is load-bearing
	// without any proxy hop.
	// grpcx dials opt into client-side round_robin (docs/research/0002):
	// DNS replicas are addressed per-call, so `docker compose --profile
	// containers up --scale stock=2` (or k8s replicas) is load-bearing
	// without any proxy hop. Transport posture comes from the GRPC_TLS_* /
	// GRPC_INSECURE env surface (spec #52, B2).
	conn, err := grpcx.DialFromEnv(addr,
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}
