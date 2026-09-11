package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// recordingSetup installs a global tracer provider backed by a SpanRecorder
// and the W3C propagator, mirroring what Setup does without exporters.
func recordingSetup(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr
}

func TestSetupNoopProviders(t *testing.T) {
	p, err := Setup(context.Background(), Config{ServiceName: "test-svc"})
	if err != nil {
		t.Fatalf("Setup without OTLP endpoint: %v", err)
	}
	// Span contexts must still be valid so downstream services can join;
	// assert BEFORE Shutdown (a shutdown provider hands out non-recording
	// spans with invalid contexts).
	ctx, span := Tracer("test").Start(context.Background(), "op")
	valid := trace.SpanContextFromContext(ctx).IsValid()
	span.End()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !valid {
		t.Fatal("expected a valid (unsampled) span context without OTLP endpoint")
	}
}

func TestServerInterceptorExtractsTraceAndRecordsMetrics(t *testing.T) {
	sr := recordingSetup(t)
	p := &Providers{Metrics: NewMetrics()}

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanID = "00f067aa0ba902b7"
	md := metadata.Pairs("traceparent", "00-"+traceID+"-"+spanID+"-01")
	inCtx := metadata.NewIncomingContext(context.Background(), md)

	handlerErr := status.Error(grpccodes.NotFound, "boom")
	var handlerSpanCtx trace.SpanContext
	_, err := p.UnaryServerInterceptor()(inCtx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/stock.v1.StockService/GetStock"},
		func(ctx context.Context, req any) (any, error) {
			handlerSpanCtx = trace.SpanContextFromContext(ctx)
			return nil, handlerErr
		})
	if err == nil {
		t.Fatal("expected handler error to propagate")
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 server span, got %d", len(spans))
	}
	span := spans[0]
	if span.Name() != "stock.v1.StockService/GetStock" {
		t.Errorf("span name = %q", span.Name())
	}
	if got := span.Parent().TraceID(); got.String() != traceID {
		t.Errorf("parent trace ID = %s, want %s (W3C extraction failed)", got, traceID)
	}
	if handlerSpanCtx.TraceID().String() != traceID {
		t.Errorf("handler trace ID = %s, want %s", handlerSpanCtx.TraceID(), traceID)
	}
	if span.Status().Code != codes.Error {
		t.Errorf("span status = %v, want Error", span.Status().Code)
	}

	// RED metrics recorded with the gRPC status code label.
	if n := testutil.CollectAndCount(p.Metrics.reqDuration, "taper_grpc_request_duration_seconds"); n != 1 {
		t.Errorf("duration series = %d, want 1", n)
	}
	if got := testutil.ToFloat64(p.Metrics.inFlight.WithLabelValues("stock.v1.StockService/GetStock")); got != 0 {
		t.Errorf("in-flight after completion = %v, want 0", got)
	}
	if got := testutil.ToFloat64(p.Metrics.reqErrors.WithLabelValues("stock.v1.StockService/GetStock", "NotFound")); got != 1 {
		t.Errorf("error counter = %v, want 1", got)
	}
}

func TestClientInterceptorInjectsTraceparentAndObserves(t *testing.T) {
	recordingSetup(t)
	p := &Providers{Metrics: NewMetrics()}

	ctx, span := Tracer("test").Start(context.Background(), "caller")
	var gotMD metadata.MD
	err := p.UnaryClientInterceptor()(ctx, "/reservation.v1.ReservationService/ReserveStock",
		nil, nil, nil,
		func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Error("expected outgoing metadata")
			}
			gotMD = md
			return nil
		})
	span.End()
	if err != nil {
		t.Fatalf("invoker error: %v", err)
	}

	tp := strings.Join(gotMD.Get("traceparent"), "")
	if tp == "" {
		t.Fatal("expected traceparent injected into outgoing metadata")
	}
	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[1] != span.SpanContext().TraceID().String() {
		t.Errorf("traceparent %q does not carry the caller trace ID %s", tp, span.SpanContext().TraceID())
	}
	if n := testutil.CollectAndCount(p.Metrics.reqDuration, "taper_grpc_request_duration_seconds"); n != 1 {
		t.Errorf("client duration series = %d, want 1", n)
	}
	if got := testutil.ToFloat64(p.Metrics.reqErrors.WithLabelValues("reservation.v1.ReservationService/ReserveStock", "OK")); got != 0 {
		t.Errorf("error counter on success = %v, want 0", got)
	}
}

func TestRoundTripServerJoinsClientTrace(t *testing.T) {
	sr := recordingSetup(t)
	p := &Providers{Metrics: NewMetrics()}

	// Client side: span + inject traceparent into outgoing metadata.
	ctx, clientSpan := Tracer("test").Start(context.Background(), "client")
	var wire metadata.MD
	var injectedSC trace.SpanContext
	_ = p.UnaryClientInterceptor()(ctx, "/order.v1.OrderService/CreateOrder", nil, nil, nil,
		func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			wire, _ = metadata.FromOutgoingContext(ctx)
			// The interceptor's own client span is what gets injected.
			injectedSC = trace.SpanContextFromContext(ctx)
			return nil
		})
	clientSpan.End()

	// Simulate the wire: the client's outgoing metadata becomes the
	// server's incoming metadata on the other process.
	inCtx := metadata.NewIncomingContext(context.Background(), wire)
	_, _ = p.UnaryServerInterceptor()(inCtx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/order.v1.OrderService/CreateOrder"},
		func(ctx context.Context, req any) (any, error) {
			return nil, nil
		})

	spans := sr.Ended()
	var serverSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.SpanKind() == trace.SpanKindServer {
			serverSpan = s
		}
	}
	if serverSpan == nil {
		t.Fatal("no server span recorded")
	}
	if serverSpan.SpanContext().TraceID() != clientSpan.SpanContext().TraceID() {
		t.Errorf("server trace %s != client trace %s (round trip broken)",
			serverSpan.SpanContext().TraceID(), clientSpan.SpanContext().TraceID())
	}
	if serverSpan.Parent().SpanID() != injectedSC.SpanID() {
		t.Errorf("server span parent %s != injected client span %s (server did not join the client trace)",
			serverSpan.Parent().SpanID(), injectedSC.SpanID())
	}
}

func TestBreakerStateGauge(t *testing.T) {
	m := NewMetrics()
	m.SetBreakerState("stock", "closed")
	m.SetBreakerState("stock", "half-open")
	m.SetBreakerState("stock", "open")
	if got := testutil.ToFloat64(m.breakerState.WithLabelValues("stock")); got != 2 {
		t.Errorf("breaker gauge = %v, want 2 (open)", got)
	}
}

func TestNilMetricsNoop(t *testing.T) {
	var m *Metrics
	m.ObserveRPC("x/y", 1, nil)
	m.SetBreakerState("dep", "open") // must not panic
}

func TestMetricsHandlerServesPrometheus(t *testing.T) {
	m := NewMetrics()
	m.ObserveRPC("stock.v1.StockService/ReserveStock", 25_000_000, nil)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "taper_grpc_request_duration_seconds") {
		t.Fatalf("metrics endpoint status=%d body head=%q", resp.StatusCode, string(body[:min(200, len(body))]))
	}
}
