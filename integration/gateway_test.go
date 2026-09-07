package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/seifsheikhelarab/taper/internal/fulfillment"
	"github.com/seifsheikhelarab/taper/internal/gateway"
	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
	"github.com/seifsheikhelarab/taper/pkg/ratelimit"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

const gatewaySecret = "test-secret"

var (
	tenantAUUID = "11111111-1111-1111-1111-111111111111"
	tenantBUUID = "22222222-2222-2222-2222-222222222222"
)

// newGateway spins an httptest server fronting the in-process gRPC stack.
func newGateway(t *testing.T, e *testEnv, rate float64, burst float64) *httptest.Server {
	t.Helper()
	core := &gateway.Core{
		Verifier:           auth.NewSandbox([]byte(gatewaySecret)),
		Limiter:            ratelimit.New(rate, burst),
		StockBreaker:       circuitbreaker.New(circuitbreaker.Config{}),
		ReservationBreaker: circuitbreaker.New(circuitbreaker.Config{}),
		OrderBreaker:       circuitbreaker.New(circuitbreaker.Config{}),
	}
	mux := http.NewServeMux()
	gateway.NewStockRoutes(core, e.stockConn).Mount(mux)
	gateway.NewReservationRoutes(core, e.resConn).Mount(mux)
	gateway.NewOrderRoutes(core, e.orderConn).Mount(mux)
	srv := httptest.NewServer(core.Middleware(mux))
	t.Cleanup(srv.Close)
	return srv
}

func gatewayToken(t *testing.T, tenant string) string {
	t.Helper()
	tok, err := auth.NewSandbox([]byte(gatewaySecret)).Issue(context.Background(), tenant, time.Minute)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return tok
}

func restCall(t *testing.T, srv *httptest.Server, method, path, token, body string, hdr map[string]string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestGatewayAuthRequired(t *testing.T) {
	e := setup(t)
	srv := newGateway(t, e, 0, 0)

	status, _ := restCall(t, srv, "POST", "/v1/orders", "", `{}`, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("missing token: %d, want 401", status)
	}
	status, _ = restCall(t, srv, "POST", "/v1/orders", "garbage", `{}`, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("bad token: %d, want 401", status)
	}
}

func TestGatewayTenantInjectionAndSpoofRejection(t *testing.T) {
	e := setup(t)
	srv := newGateway(t, e, 0, 0)
	tok := gatewayToken(t, tenantAUUID)

	// Spoofed tenant rejected.
	status, _ := restCall(t, srv, "POST", "/v1/stock/adjust", tok,
		fmt.Sprintf(`{"tenant_id":%q,"sku_id":"GW-SKU-1","warehouse_id":"WH-1","quantity_delta":5,"reason":"test","source":"test"}`, tenantBUUID), nil)
	if status != http.StatusForbidden {
		t.Fatalf("spoofed tenant: %d, want 403", status)
	}

	// Correct tenant: stock lands under the JWT tenant, not any body value.
	status, body := restCall(t, srv, "POST", "/v1/stock/adjust", tok,
		`{"sku_id":"GW-SKU-1","warehouse_id":"WH-1","quantity_delta":5,"reason":"test","source":"test"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("adjust: %d (%v)", status, body)
	}
	available, _, _ := e.stockLevel(t, tenantAUUID, "GW-SKU-1", "WH-1")
	if available != 5 {
		t.Fatalf("available = %d, want 5 under JWT tenant", available)
	}
}

func TestGatewaySagaHappyPathAndFulfill(t *testing.T) {
	e := setup(t)
	srv := newGateway(t, e, 0, 0)
	tok := gatewayToken(t, tenantAUUID)
	e.seed(t, tenantAUUID, "GW-SAGA-1", "WH-1", 10)

	lines := `[{"sku_id":"GW-SAGA-1","warehouse_id":"WH-1","quantity":3,"unit_price":100}]`
	status, body := restCall(t, srv, "POST", "/v1/orders", tok,
		fmt.Sprintf(`{"order_id":"gw-ord-1","lines":%s}`, lines),
		map[string]string{"Idempotency-Key": "gw-ord-1"})
	if status != http.StatusOK {
		t.Fatalf("create order: %d (%v)", status, body)
	}
	if body["status"] != "CONFIRMED" {
		t.Fatalf("saga status = %v, want CONFIRMED", body["status"])
	}

	// GET by path id, tenant from token. protojson uses camelCase keys.
	status, body = restCall(t, srv, "GET", "/v1/orders/gw-ord-1", tok, "", nil)
	if status != http.StatusOK || body["sagaState"] != "CONFIRMED" {
		t.Fatalf("get order: %d (%v)", status, body)
	}

	// FulfillOrder through REST. The stock decrement happens in the real
	// fulfillment consumer reacting to order.fulfilled on the wire, so run
	// it against the live topic and wait for it to clear the allocation.
	status, body = restCall(t, srv, "POST", "/v1/orders/gw-ord-1/fulfill", tok, `{}`,
		map[string]string{"Idempotency-Key": "gw-fulfill-1"})
	if status != http.StatusOK || body["status"] != "FULFILLED" {
		t.Fatalf("fulfill: %d (%v)", status, body)
	}
	brokers := kafkaBrokers(t)
	svc := fulfillment.New(e.stock, nil)
	fc := streaming.NewConsumer(streaming.Config{
		Brokers: brokers, Topic: "order.events", GroupID: fmt.Sprintf("gw-fulfill-%d", time.Now().UnixNano()),
	}, nil)
	fctx, fcancel := context.WithCancel(context.Background())
	defer fcancel()
	go func() {
		_ = fc.Run(fctx, func(ctx context.Context, m kafka.Message) error {
			var ev struct {
				EventType string `json:"event_type"`
				OrderID   string `json:"order_id"`
			}
			if json.Unmarshal(m.Value, &ev) != nil || ev.EventType != "order.fulfilled" || ev.OrderID != "gw-ord-1" {
				return nil // stale or unrelated event: skip, offsets still move
			}
			return svc.HandleOrderEvent(ctx, m)
		})
	}()

	deadline := time.Now().Add(45 * time.Second)
	for {
		available, _, allocated := e.stockLevel(t, tenantAUUID, "GW-SAGA-1", "WH-1")
		if available == 7 && allocated == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer never fulfilled: available=%d allocated=%d", available, allocated)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestGatewaySagaCompensation(t *testing.T) {
	e := setup(t)
	srv := newGateway(t, e, 0, 0)
	tok := gatewayToken(t, tenantAUUID)
	e.seed(t, tenantAUUID, "GW-SAGA-2", "WH-1", 10)

	status, body := restCall(t, srv, "POST", "/v1/orders", tok,
		`{"order_id":"gw-ord-2","lines":[{"sku_id":"GW-SAGA-2","warehouse_id":"WH-1","quantity":2,"unit_price":50}],"force_payment_failure":true}`, nil)
	if status != http.StatusOK {
		t.Fatalf("create order: %d (%v)", status, body)
	}
	if body["status"] != "COMPENSATED" {
		t.Fatalf("status = %v, want COMPENSATED", body["status"])
	}
	// Compensation released the reservation.
	available, reserved, _ := e.stockLevel(t, tenantAUUID, "GW-SAGA-2", "WH-1")
	if available != 10 || reserved != 0 {
		t.Fatalf("post-compensation stock: available=%d reserved=%d, want 10/0", available, reserved)
	}
}

func TestGatewayRateLimit(t *testing.T) {
	e := setup(t)
	srv := newGateway(t, e, 1, 2) // 1/s, burst 2
	tok := gatewayToken(t, tenantAUUID)

	for i := 0; i < 2; i++ {
		status, _ := restCall(t, srv, "POST", "/v1/stock/adjust", tok,
			`{"sku_id":"GW-RL","warehouse_id":"WH-1","quantity_delta":1,"reason":"test","source":"test"}`, nil)
		if status != http.StatusOK {
			t.Fatalf("request %d: %d, want 200", i, status)
		}
	}
	status, _ := restCall(t, srv, "POST", "/v1/stock/adjust", tok,
		`{"sku_id":"GW-RL","warehouse_id":"WH-1","quantity_delta":1,"reason":"test","source":"test"}`, nil)
	if status != http.StatusTooManyRequests {
		t.Fatalf("third request: %d, want 429", status)
	}
}
