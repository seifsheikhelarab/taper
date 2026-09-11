package gateway

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	reservationv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
)

// claims returns the middleware-injected claims, writing 401 when absent.
func (c *Core) claims(w http.ResponseWriter, r *http.Request) (auth.Claims, bool) {
	cl, ok := ClaimsFromContext(r.Context())
	if !ok {
		(&httpError{status: http.StatusUnauthorized, code: "unauthenticated", message: "missing claims"}).write(w)
		return auth.Claims{}, false
	}
	return cl, true
}

// route wires one REST endpoint: decode + tenant injection, then one
// breaker-guarded gRPC call, writing protojson or the mapped error.
// The call wrapper must return proto.Message explicitly; grpc-go client
// methods have concrete return types and are not directly assignable.
func route[P proto.Message](c *Core, dep string, br *circuitbreaker.Breaker, mk func() P, call func(context.Context, P) (proto.Message, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := c.claims(w, r)
		if !ok {
			return
		}
		req := mk()
		if he := c.decode(r, claims, req); he != nil {
			he.write(w)
			return
		}
		c.invoke(w, r, dep, br, func(ctx context.Context) (proto.Message, error) {
			return call(ctx, req)
		})
	}
}

// StockRoutes fronts the stock service.
type StockRoutes struct {
	core   *Core
	client stockv1.StockServiceClient
}

// NewStockRoutes builds stock routes against a gRPC connection.
func NewStockRoutes(core *Core, cc *grpc.ClientConn) *StockRoutes {
	return &StockRoutes{core: core, client: stockv1.NewStockServiceClient(cc)}
}

// Mount registers the stock REST surface on mux.
func (s *StockRoutes) Mount(mux *http.ServeMux) {
	c, br := s.core, s.core.StockBreaker
	cl := s.client
	mux.HandleFunc("POST /v1/stock/adjust", route(c, "stock", br,
		func() *stockv1.AdjustStockRequest { return &stockv1.AdjustStockRequest{} },
		func(ctx context.Context, req *stockv1.AdjustStockRequest) (proto.Message, error) {
			return cl.AdjustStock(ctx, req)
		}))
	mux.HandleFunc("POST /v1/stock/reserve", route(c, "stock", br,
		func() *stockv1.ReserveStockRequest { return &stockv1.ReserveStockRequest{} },
		func(ctx context.Context, req *stockv1.ReserveStockRequest) (proto.Message, error) {
			return cl.ReserveStock(ctx, req)
		}))
	mux.HandleFunc("POST /v1/stock/release", route(c, "stock", br,
		func() *stockv1.ReleaseStockRequest { return &stockv1.ReleaseStockRequest{} },
		func(ctx context.Context, req *stockv1.ReleaseStockRequest) (proto.Message, error) {
			return cl.ReleaseStock(ctx, req)
		}))
	mux.HandleFunc("POST /v1/stock/allocate", route(c, "stock", br,
		func() *stockv1.ConfirmStockAllocationRequest { return &stockv1.ConfirmStockAllocationRequest{} },
		func(ctx context.Context, req *stockv1.ConfirmStockAllocationRequest) (proto.Message, error) {
			return cl.ConfirmStockAllocation(ctx, req)
		}))
	mux.HandleFunc("POST /v1/stock/fulfill", route(c, "stock", br,
		func() *stockv1.FulfillStockRequest { return &stockv1.FulfillStockRequest{} },
		func(ctx context.Context, req *stockv1.FulfillStockRequest) (proto.Message, error) {
			return cl.FulfillStock(ctx, req)
		}))
	mux.HandleFunc("POST /v1/stock/unlock", route(c, "stock", br,
		func() *stockv1.UnlockStockForAuditRequest { return &stockv1.UnlockStockForAuditRequest{} },
		func(ctx context.Context, req *stockv1.UnlockStockForAuditRequest) (proto.Message, error) {
			return cl.UnlockStockForAudit(ctx, req)
		}))
}

// ReservationRoutes fronts the reservation service.
type ReservationRoutes struct {
	core   *Core
	client reservationv1.ReservationServiceClient
}

// NewReservationRoutes builds reservation routes against a gRPC connection.
func NewReservationRoutes(core *Core, cc *grpc.ClientConn) *ReservationRoutes {
	return &ReservationRoutes{core: core, client: reservationv1.NewReservationServiceClient(cc)}
}

// Mount registers the reservation REST surface on mux.
func (s *ReservationRoutes) Mount(mux *http.ServeMux) {
	c, br := s.core, s.core.ReservationBreaker
	cl := s.client
	mux.HandleFunc("POST /v1/reservations", route(c, "reservation", br,
		func() *reservationv1.ReserveRequest { return &reservationv1.ReserveRequest{} },
		func(ctx context.Context, req *reservationv1.ReserveRequest) (proto.Message, error) {
			return cl.Reserve(ctx, req)
		}))
	mux.HandleFunc("POST /v1/reservations/release", route(c, "reservation", br,
		func() *reservationv1.ReleaseRequest { return &reservationv1.ReleaseRequest{} },
		func(ctx context.Context, req *reservationv1.ReleaseRequest) (proto.Message, error) {
			return cl.Release(ctx, req)
		}))
	mux.HandleFunc("POST /v1/reservations/allocate", route(c, "reservation", br,
		func() *reservationv1.AllocateReservationRequest { return &reservationv1.AllocateReservationRequest{} },
		func(ctx context.Context, req *reservationv1.AllocateReservationRequest) (proto.Message, error) {
			return cl.AllocateReservation(ctx, req)
		}))
}
