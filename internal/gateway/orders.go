package gateway

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
)

// OrderRoutes fronts the order service (saga surface).
type OrderRoutes struct {
	core   *Core
	client orderv1.OrderServiceClient
}

// NewOrderRoutes builds order routes against a gRPC connection.
func NewOrderRoutes(core *Core, cc *grpc.ClientConn) *OrderRoutes {
	return &OrderRoutes{core: core, client: orderv1.NewOrderServiceClient(cc)}
}

// Mount registers the order REST surface on mux.
func (o *OrderRoutes) Mount(mux *http.ServeMux) {
	c, br := o.core, o.core.OrderBreaker
	cl := o.client
	mux.HandleFunc("POST /v1/orders", route(c, "order", br,
		func() *orderv1.CreateOrderRequest { return &orderv1.CreateOrderRequest{} },
		func(ctx context.Context, req *orderv1.CreateOrderRequest) (proto.Message, error) {
			return cl.CreateOrder(ctx, req)
		}))

	// Cancel and fulfill: order_id from the path, tenant from the token.
	mux.HandleFunc("POST /v1/orders/{id}/cancel", byID(c, "order", br,
		func() *orderv1.CancelOrderRequest { return &orderv1.CancelOrderRequest{} },
		func(ctx context.Context, req *orderv1.CancelOrderRequest) (proto.Message, error) {
			return cl.CancelOrder(ctx, req)
		}))
	mux.HandleFunc("POST /v1/orders/{id}/fulfill", byID(c, "order", br,
		func() *orderv1.FulfillOrderRequest { return &orderv1.FulfillOrderRequest{} },
		func(ctx context.Context, req *orderv1.FulfillOrderRequest) (proto.Message, error) {
			return cl.FulfillOrder(ctx, req)
		}))
	mux.HandleFunc("GET /v1/orders/{id}", o.get)
}

// byID extracts {id} from the path into order_id, then follows the shared
// decode + breaker-guarded call flow. Package-level because Go methods
// cannot have type parameters.
func byID[P proto.Message](c *Core, dep string, br *circuitbreaker.Breaker, mk func() P, call func(context.Context, P) (proto.Message, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			(&httpError{status: http.StatusBadRequest, code: "invalid_argument", message: "missing order id"}).write(w)
			return
		}
		claims, ok := c.claims(w, r)
		if !ok {
			return
		}
		req := mk()
		if he := c.decode(r, claims, req); he != nil {
			he.write(w)
			return
		}
		setStringField(req, "order_id", id)
		c.invoke(w, r, dep, br, func(ctx context.Context) (proto.Message, error) {
			return call(ctx, req)
		})
	}
}

// get handles GET /v1/orders/{id}: tenant injected from the token, order
// id from the path, empty body allowed.
func (o *OrderRoutes) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		(&httpError{status: http.StatusBadRequest, code: "invalid_argument", message: "missing order id"}).write(w)
		return
	}
	claims, ok := o.core.claims(w, r)
	if !ok {
		return
	}
	req := &orderv1.GetOrderRequest{OrderId: id}
	if he := o.core.decode(r, claims, req); he != nil {
		he.write(w)
		return
	}
	o.core.invoke(w, r, "order", o.core.OrderBreaker, func(ctx context.Context) (proto.Message, error) {
		return o.client.GetOrder(ctx, req)
	})
}
