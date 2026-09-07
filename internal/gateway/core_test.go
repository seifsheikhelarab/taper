package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
	"github.com/seifsheikhelarab/taper/pkg/ratelimit"
)

func newTestCore(t *testing.T) (*Core, *auth.Sandbox) {
	t.Helper()
	s := auth.NewSandbox([]byte("test-secret"))
	return &Core{
		Verifier:           s,
		Limiter:            ratelimit.New(1000, 1000),
		StockBreaker:       circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, Cooldown: time.Second}),
		ReservationBreaker: circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, Cooldown: time.Second}),
		OrderBreaker:       circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, Cooldown: time.Second}),
	}, s
}

func doJSON(t *testing.T, h http.Handler, method, target, token, body string, hdr map[string]string) (*httptest.ResponseRecorder, auth.Claims) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, auth.Claims{}
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		code   codes.Code
		status int
	}{
		{codes.InvalidArgument, 400},
		{codes.NotFound, 404},
		{codes.AlreadyExists, 409},
		{codes.FailedPrecondition, 409},
		{codes.Unauthenticated, 401},
		{codes.PermissionDenied, 403},
		{codes.ResourceExhausted, 429},
		{codes.Unavailable, 503},
		{codes.DeadlineExceeded, 504},
		{codes.Internal, 500},
	}
	for _, tc := range cases {
		if got := httpStatusFromCode(tc.code); got != tc.status {
			t.Errorf("%s -> %d, want %d", tc.code, got, tc.status)
		}
	}
}

func TestAuthRequired(t *testing.T) {
	c, _ := newTestCore(t)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := c.Middleware(mux)

	rec, _ := doJSON(t, h, "POST", "/v1/orders", "", `{}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d, want 401", rec.Code)
	}
	rec, _ = doJSON(t, h, "POST", "/v1/orders", "garbage-token", `{}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad token: %d, want 401", rec.Code)
	}
}

func TestTenantInjectionAndAntiSpoofing(t *testing.T) {
	c, s := newTestCore(t)
	tok, _ := s.Issue(context.Background(), "tenant-A", time.Minute)

	var captured *orderv1.CreateOrderRequest
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", func(w http.ResponseWriter, r *http.Request) {
		req := &orderv1.CreateOrderRequest{}
		if httpErr := c.decode(r, mustClaims(r), req); httpErr != nil {
			httpErr.write(w)
			return
		}
		captured = req
		w.WriteHeader(200)
	})
	h := c.Middleware(mux)

	// Spoofed tenant rejected.
	rec, _ := doJSON(t, h, "POST", "/v1/orders", tok, `{"tenant_id":"tenant-B","order_id":"o1"}`, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("spoofed body tenant: %d, want 403", rec.Code)
	}

	// Matching/absent tenant injected from the token.
	rec, _ = doJSON(t, h, "POST", "/v1/orders", tok, `{"order_id":"o1"}`, nil)
	if rec.Code != 200 {
		t.Fatalf("valid body: %d (%s)", rec.Code, rec.Body.String())
	}
	if captured.GetTenantId() != "tenant-A" {
		t.Fatalf("injected tenant = %q, want tenant-A", captured.GetTenantId())
	}

	// Idempotency-Key passthrough.
	rec, _ = doJSON(t, h, "POST", "/v1/orders", tok, `{"order_id":"o2"}`, map[string]string{"Idempotency-Key": "idem-1"})
	if rec.Code != 200 || captured.GetIdempotencyKey() != "idem-1" {
		t.Fatalf("idempotency passthrough failed: %d %q", rec.Code, captured.GetIdempotencyKey())
	}
}

func TestRateLimitEngages(t *testing.T) {
	c, s := newTestCore(t)
	c.Limiter = ratelimit.New(1, 2) // 1/s, burst 2
	tok, _ := s.Issue(context.Background(), "tenant-A", time.Minute)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/x", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := c.Middleware(mux)

	for i := 0; i < 2; i++ {
		rec, _ := doJSON(t, h, "POST", "/v1/x", tok, ``, nil)
		if rec.Code != 200 {
			t.Fatalf("request %d: %d, want 200", i, rec.Code)
		}
	}
	rec, _ := doJSON(t, h, "POST", "/v1/x", tok, ``, nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request: %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}
}

func TestOpenBreakerFailsFast503(t *testing.T) {
	c, s := newTestCore(t)
	tok, _ := s.Issue(context.Background(), "tenant-A", time.Minute)
	for i := 0; i < 3; i++ {
		c.OrderBreaker.RecordFailure()
	}
	if c.OrderBreaker.State() != circuitbreaker.Open {
		t.Fatal("breaker should be open")
	}
	rec, _ := doJSON(t, h(c), "POST", "/v1/orders", tok, `{}`, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("open breaker: %d, want 503", rec.Code)
	}
}

func TestInvokeMapsGRPCErrorsAndRecordsBreaker(t *testing.T) {
	c, _ := newTestCore(t)
	// Unavailable failures feed the breaker; the third opens it.
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		c.invoke(rec, httptest.NewRequest("POST", "/x", nil), c.StockBreaker,
			func(ctx context.Context) (proto.Message, error) {
				return nil, status.Error(codes.Unavailable, "stock down")
			})
		if rec.Code != 503 {
			t.Fatalf("unavailable -> %d, want 503", rec.Code)
		}
	}
	if c.StockBreaker.State() != circuitbreaker.Open {
		t.Fatal("3 unavailable calls must open the breaker")
	}

	// A domain error must NOT feed the breaker.
	c2, _ := newTestCore(t)
	rec := httptest.NewRecorder()
	c2.invoke(rec, httptest.NewRequest("POST", "/x", nil), c2.StockBreaker,
		func(ctx context.Context) (proto.Message, error) {
			return nil, status.Error(codes.InvalidArgument, "bad sku")
		})
	if rec.Code != 400 {
		t.Fatalf("invalid_argument -> %d, want 400", rec.Code)
	}
	if c2.StockBreaker.State() != circuitbreaker.Closed {
		t.Fatal("domain errors must not open breakers")
	}

	// Success writes protojson.
	rec = httptest.NewRecorder()
	c2.invoke(rec, httptest.NewRequest("POST", "/x", nil), c2.StockBreaker,
		func(ctx context.Context) (proto.Message, error) {
			return &orderv1.GetOrderResponse{OrderId: "o1", Status: "CONFIRMED"}, nil
		})
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["status"] != "CONFIRMED" {
		t.Fatalf("protojson response: %s (%v)", rec.Body.String(), err)
	}
}

func TestMalformedJSONRejected(t *testing.T) {
	c, s := newTestCore(t)
	tok, _ := s.Issue(context.Background(), "tenant-A", time.Minute)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", func(w http.ResponseWriter, r *http.Request) {
		req := &orderv1.CreateOrderRequest{}
		if httpErr := c.decode(r, mustClaims(r), req); httpErr != nil {
			httpErr.write(w)
			return
		}
		w.WriteHeader(200)
	})
	rec, _ := doJSON(t, c.Middleware(mux), "POST", "/v1/orders", tok, `{not-json`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed json: %d, want 400", rec.Code)
	}
}

func mustClaims(r *http.Request) auth.Claims {
	claims, ok := ClaimsFromContext(r.Context())
	if !ok {
		panic(errors.New("no claims in context"))
	}
	return claims
}

func h(c *Core) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", func(w http.ResponseWriter, r *http.Request) {
		c.invoke(w, r, c.OrderBreaker, func(ctx context.Context) (proto.Message, error) {
			return nil, errors.New("unused")
		})
	})
	return c.Middleware(mux)
}
