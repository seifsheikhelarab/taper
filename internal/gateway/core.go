// Package gateway implements the REST surface fronting the internal gRPC
// services (Phase 4, spec #35). The HTTP layer is deliberately hand-rolled
// (docs/research/0001-gateway-http-layer.md): routes live here instead of
// in proto annotations, keeping the gateway the only new surface.
//
// Security model: every request carries a bearer token whose tenant_id
// claim is the RLS identity. The gateway injects that tenant into the
// request body and rejects bodies that name a different tenant, so a
// client can never act across tenants by editing JSON.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
	"github.com/seifsheikhelarab/taper/pkg/ratelimit"
)

const maxBodyBytes = 1 << 20 // 1 MiB

const callTimeout = 10 * time.Second

type claimsKey struct{}

// ClaimsFromContext returns the verified token claims injected by the
// auth middleware.
func ClaimsFromContext(ctx context.Context) (auth.Claims, bool) {
	claims, ok := ctx.Value(claimsKey{}).(auth.Claims)
	return claims, ok
}

// httpError is an error with a decided HTTP response.
type httpError struct {
	status  int
	code    string
	message string
}

func (e *httpError) Error() string { return e.message }

func (e *httpError) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": e.code, "message": e.message},
	})
}

// httpStatusFromCode maps gRPC status codes to HTTP responses. Unavailable
// maps to 503 per the Fail-Fast Policy (CONTEXT.md): the gateway does not
// queue against a degraded dependency.
func httpStatusFromCode(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.FailedPrecondition:
		return http.StatusConflict
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func errorFromGRPC(err error) *httpError {
	st, ok := status.FromError(err)
	if !ok {
		return &httpError{status: http.StatusInternalServerError, code: "unknown", message: err.Error()}
	}
	return &httpError{
		status:  httpStatusFromCode(st.Code()),
		code:    st.Code().String(),
		message: st.Message(),
	}
}

// Core carries the gateway's shared dependencies and per-backend breakers.
type Core struct {
	Verifier *auth.Sandbox
	Limiter  *ratelimit.Limiter
	// One breaker per downstream service; an open breaker fails fast with
	// 503 instead of queuing.
	StockBreaker       *circuitbreaker.Breaker
	ReservationBreaker *circuitbreaker.Breaker
	OrderBreaker       *circuitbreaker.Breaker
	// BreakerGauge, when set, observes breaker transitions so the
	// Fail-Fast Policy is visible (Prometheus taper_breaker_state).
	BreakerGauge BreakerObserver
}

// authorize verifies the bearer token, enforcing the auth contract.
func (c *Core) authorize(r *http.Request) (auth.Claims, *httpError) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" {
		return auth.Claims{}, &httpError{status: http.StatusUnauthorized, code: "unauthenticated", message: "missing bearer token"}
	}
	claims, err := c.Verifier.Verify(r.Context(), token)
	if err != nil {
		return auth.Claims{}, &httpError{status: http.StatusUnauthorized, code: "unauthenticated", message: "invalid token"}
	}
	return claims, nil
}

// admit enforces the per-tenant rate limit.
func (c *Core) admit(w http.ResponseWriter, tenant string) bool {
	d := c.Limiter.Take(tenant)
	if d.Allowed {
		return true
	}
	secs := int(d.RetryAfter.Seconds()) + 1
	w.Header().Set("Retry-After", fmt.Sprintf("%d", secs))
	(&httpError{
		status:  http.StatusTooManyRequests,
		code:    "resource_exhausted",
		message: fmt.Sprintf("rate limit exceeded, retry after %s", d.RetryAfter),
	}).write(w)
	return false
}

// decode parses the JSON body into msg, enforcing tenant ownership and
// filling idempotency_key from the Idempotency-Key header. An empty body
// is allowed (read-style requests).
func (c *Core) decode(r *http.Request, claims auth.Claims, msg proto.Message) *httpError {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBodyBytes))
	if err != nil {
		return &httpError{status: http.StatusBadRequest, code: "invalid_argument", message: "unreadable body"}
	}
	if len(body) > 0 {
		if err := (protojson.UnmarshalOptions{}).Unmarshal(body, msg); err != nil {
			return &httpError{status: http.StatusBadRequest, code: "invalid_argument", message: "invalid JSON body: " + err.Error()}
		}
	}
	if bodyTenant := stringField(msg, "tenant_id"); bodyTenant != "" && bodyTenant != claims.TenantID {
		return &httpError{status: http.StatusForbidden, code: "permission_denied", message: "body tenant does not match token tenant"}
	}
	setStringField(msg, "tenant_id", claims.TenantID)
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		setStringField(msg, "idempotency_key", key)
	}
	return nil
}

// BreakerObserver consumes breaker state transitions (port so the gateway
// stays decoupled from the metrics implementation).
type BreakerObserver interface {
	SetBreakerState(dependency, state string)
}

// invoke runs one gRPC call through the named dependency's breaker and
// writes either the protojson response or the mapped error.
func (c *Core) invoke(w http.ResponseWriter, r *http.Request, dep string, br *circuitbreaker.Breaker, call func(ctx context.Context) (proto.Message, error)) {
	if err := br.Allow(); err != nil {
		c.publishBreaker(dep, br)
		(&httpError{status: http.StatusServiceUnavailable, code: "unavailable", message: err.Error()}).write(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), callTimeout)
	defer cancel()

	resp, err := call(ctx)
	if err != nil {
		// Transport-class failures feed the breaker; domain errors do not.
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
			br.RecordFailure()
		default:
			br.RecordSuccess()
		}
		c.publishBreaker(dep, br)
		httpErr := errorFromGRPC(err)
		httpErr.write(w)
		return
	}
	br.RecordSuccess()
	c.publishBreaker(dep, br)
	w.Header().Set("Content-Type", "application/json")
	body, err := (protojson.MarshalOptions{}).Marshal(resp)
	if err != nil {
		(&httpError{status: http.StatusInternalServerError, code: "internal", message: "response encoding failed"}).write(w)
		return
	}
	_, _ = w.Write(body)
}

// publishBreaker reports the breaker's state to the observer (the
// Prometheus taper_breaker_state gauge in production; no-op when unset).
func (c *Core) publishBreaker(dep string, br *circuitbreaker.Breaker) {
	if c.BreakerGauge == nil || dep == "" {
		return
	}
	c.BreakerGauge.SetBreakerState(dep, br.State())
}

// Middleware chains auth and rate limiting around the route mux. Every
// route requires a verified token; /healthz is exempt (liveness probes and
// the load harness have no token). Rate limiting still applies to all
// authenticated traffic.
func (c *Core) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		claims, httpErr := c.authorize(r)
		if httpErr != nil {
			httpErr.write(w)
			return
		}
		if !c.admit(w, claims.TenantID) {
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims)))
	})
}

// stringField reads a string field from a proto message by its proto name.
func stringField(msg proto.Message, field string) string {
	m := msg.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || fd.Kind() != protoreflect.StringKind || !m.Has(fd) {
		return ""
	}
	return m.Get(fd).String()
}

// setStringField sets a string field by proto name; no-op when the message
// has no such field (e.g. idempotency_key on read requests).
func setStringField(msg proto.Message, field, value string) {
	m := msg.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		return
	}
	m.Set(fd, protoreflect.ValueOfString(value))
}
