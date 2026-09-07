// Command gateway is the REST front for the internal gRPC services
// (Phase 4, spec #35): JWT tenant auth, per-tenant rate limiting, and
// Fail-Fast error mapping, per docs/research/0001-gateway-http-layer.md.
package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seifsheikhelarab/taper/internal/gateway"
	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/ratelimit"
)

func main() {
	addr := config.EnvOr("GATEWAY_ADDR", ":8080")
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	resAddr := config.EnvOr("RESERVATION_ADDR", "localhost:50052")
	orderAddr := config.EnvOr("ORDER_ADDR", "localhost:50053")
	secret := config.EnvOr("GATEWAY_JWT_SECRET", "taper-sandbox-secret")
	rate := config.EnvOrFloat("GATEWAY_RATE_PER_TENANT", 10)
	burst := config.EnvOrFloat("GATEWAY_BURST_PER_TENANT", 20)

	stockConn := dial(stockAddr)
	defer stockConn.Close()
	resConn := dial(resAddr)
	defer resConn.Close()
	orderConn := dial(orderAddr)
	defer orderConn.Close()

	core := &gateway.Core{
		Verifier:           auth.NewSandbox([]byte(secret)),
		Limiter:            ratelimit.New(rate, burst),
		StockBreaker:       circuitbreaker.New(circuitbreaker.Config{}),
		ReservationBreaker: circuitbreaker.New(circuitbreaker.Config{}),
		OrderBreaker:       circuitbreaker.New(circuitbreaker.Config{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	gateway.NewStockRoutes(core, stockConn).Mount(mux)
	gateway.NewReservationRoutes(core, resConn).Mount(mux)
	gateway.NewOrderRoutes(core, orderConn).Mount(mux)

	log.Printf("gateway listening on %s (stock=%s reservation=%s order=%s)", addr, stockAddr, resAddr, orderAddr)
	if err := http.ListenAndServe(addr, core.Middleware(mux)); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func dial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}
