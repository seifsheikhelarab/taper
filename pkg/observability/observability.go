// Package observability wires the Phase 5 hardening stack (spec #44):
// OpenTelemetry tracing with W3C traceparent propagation (OTLP trace
// export) and Prometheus RED metrics for the gRPC surface.
//
// Services call Setup once at boot, chain UnaryServerInterceptor /
// UnaryClientInterceptor into their gRPC servers and clients, and serve
// Metrics.Handler() on a dedicated admin port. With no OTLPEndpoint the
// providers run in no-op mode (never-sampled spans) so local runs without
// Jaeger still propagate valid trace context downstream.
package observability

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Config tunes observability for one service binary.
type Config struct {
	// ServiceName labels every span and OTLP resource, e.g. "stock".
	ServiceName string
	// OTLPEndpoint is the OTLP gRPC collector host:port (e.g.
	// "localhost:4317"). Empty disables exporters: spans are never
	// sampled and metrics are not exported, but trace context still
	// propagates.
	OTLPEndpoint string
}

// Providers bundles the initialized observability stack for one binary.
type Providers struct {
	Metrics *Metrics

	// Readiness registry (spec #52, B1): binaries register dependency
	// checks at boot (Postgres ping, downstream gRPC health, Kafka dial);
	// the admin listener's /readyz runs them per request.
	mu        sync.Mutex
	readiness map[string]func(context.Context) error
	degraded  bool

	shutdowns []func(context.Context) error
}

// Setup initializes the global tracer/meter providers, the propagation
// scheme (W3C traceparent + baggage) and RED metrics. Call Shutdown on
// process exit to flush.
func Setup(ctx context.Context, cfg Config) (*Providers, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	p := &Providers{Metrics: NewMetrics(), readiness: map[string]func(context.Context) error{}}
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName)))
	if err != nil {
		return nil, err
	}

	if cfg.OTLPEndpoint == "" {
		// No backend configured: keep valid span contexts so downstream
		// services join the same trace, but never sample locally.
		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		otel.SetTracerProvider(tp)
		p.shutdowns = append(p.shutdowns, tp.Shutdown)
		return p, nil
	}

	// Accept standard OTel endpoint values (http://host:port). The OTLP
	// gRPC exporter wants a bare host:port and this build exports
	// insecure (dev default): strip the scheme, warn on https.
	endpoint := cfg.OTLPEndpoint
	if i := strings.Index(endpoint, "://"); i >= 0 {
		if endpoint[:i] == "https" {
			log.Printf("observability: https OTLP endpoint %q needs TLS; exporting insecure instead", cfg.OTLPEndpoint)
		}
		endpoint = endpoint[i+3:]
	}

	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	p.shutdowns = append(p.shutdowns, tp.Shutdown)

	// Metrics are served via Prometheus on each service's /metrics admin
	// port; the OTLP metric exporter is deliberately not installed. Jaeger
	// (the trace backend) rejects OTLP metrics with Unimplemented, and no
	// second metrics backend exists to ship them to.
	return p, nil
}

// Shutdown flushes all exporters. Safe to call once at process exit.
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	for i := len(p.shutdowns) - 1; i >= 0; i-- {
		if err := p.shutdowns[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// RegisterReadiness adds a named dependency check consulted by /readyz.
// Safe to call at any time; checks run concurrently per request.
func (p *Providers) RegisterReadiness(name string, check func(context.Context) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readiness[name] = check
}

// SetDegraded flips readiness to unhealthy without touching liveness: the
// operational lever for planned dependency maintenance (drain upstream
// traffic before the dependency drops). Documented in the ops runbook.
func (p *Providers) SetDegraded(degraded bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.degraded = degraded
}

// readyError describes why the process is not ready: degraded is checked
// first, then every registered check via closer.Readiness (the shared
// concurrent aggregation).
func (p *Providers) readyError(ctx context.Context) error {
	p.mu.Lock()
	degraded := p.degraded
	checks := make(map[string]func(context.Context) error, len(p.readiness))
	for name, check := range p.readiness {
		checks[name] = check
	}
	p.mu.Unlock()

	if degraded {
		return errors.New("degraded: planned maintenance (SetDegraded)")
	}
	return closer.Readiness(checks)(ctx)
}

// LiveHandler serves static-200 liveness: dependency-free by contract, so a
// deadlocked process is distinguishable from one waiting on a downstream.
func (p *Providers) LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("live"))
	})
}

// ReadyHandler serves readiness: 200 only when not degraded and every
// registered dependency check passes within its request context.
func (p *Providers) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := p.readyError(r.Context()); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
}

// MustRun boots observability for one service binary: Setup with the
// service's name and the OTEL_EXPORTER_OTLP_ENDPOINT env (empty = no-op
// providers), serves the admin listener on metricsAddr (/metrics, plus
// /healthz liveness and /readyz readiness), and returns a cleanup func that
// closes the server and flushes traces.
// Call cleanup via defer; main's runtime is the admin server lifecycle.
func MustRun(service, metricsAddr string) (*Providers, func()) {
	provs, err := Setup(context.Background(), Config{
		ServiceName:  service,
		OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	})
	if err != nil {
		log.Fatalf("observability setup: %v", err)
	}
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", provs.Metrics.Handler())
	adminMux.Handle("/healthz", provs.LiveHandler())
	adminMux.Handle("/readyz", provs.ReadyHandler())
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("%s metrics listening on %s", service, metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics serve: %v", err)
		}
	}()
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			_ = metricsSrv.Close() //nolint:errcheck // admin endpoint at exit
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := provs.Shutdown(shutdownCtx); err != nil {
				log.Printf("observability shutdown: %v", err)
			}
		})
	}
	return provs, cleanup
}

// Tracer returns a named tracer from the global provider. Consumer loops,
// sagas and handlers use this to add spans to the propagated trace.
func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}

// Propagator returns the global W3C propagator for non-gRPC carriers such
// as Kafka headers.
func Propagator() propagation.TextMapPropagator { return otel.GetTextMapPropagator() }

// RPCName normalizes a gRPC full method ("/pkg.Svc/Method") to the RED
// metric label form "pkg.Svc/Method".
func RPCName(fullMethod string) string { return strings.TrimPrefix(fullMethod, "/") }

// Traceparent returns the W3C traceparent for ctx's span context in the
// exact "00-<traceid>-<spanid>-<flags>" form, or "" when there is no valid
// span. Services store it in the outbox traceparent column so Debezium's
// EventRouter promotes it to a Kafka header and consumers join the trace.
func Traceparent(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	var b strings.Builder
	b.WriteString("00-")
	b.WriteString(sc.TraceID().String())
	b.WriteString("-")
	b.WriteString(sc.SpanID().String())
	b.WriteString("-")
	if sc.IsSampled() {
		b.WriteString("01")
	} else {
		b.WriteString("00")
	}
	return b.String()
}

// UnaryServerInterceptor extracts W3C trace context from incoming gRPC
// metadata, opens a server span, and records RED metrics around the
// handler. Chain it first so the span is a child of the remote context.
func (p *Providers) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	m := p.Metrics
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = Propagator().Extract(ctx, mdCarrier{md: md.Copy()})
		}
		name := RPCName(info.FullMethod)
		start := time.Now()
		ctx, span := Tracer("taper/grpc").Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer))
		m.inFlight.WithLabelValues(name).Inc()

		resp, err := handler(ctx, req)

		m.inFlight.WithLabelValues(name).Dec()
		setSpanStatus(span, err)
		m.ObserveRPC(name, time.Since(start), err)
		span.End()
		return resp, err
	}
}

// UnaryClientInterceptor opens a client span and injects W3C trace context
// into outgoing gRPC metadata so the downstream server joins this trace,
// then records RED metrics. Chain it after any span-creating interceptor
// so injection uses the client span's context.
func (p *Providers) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	m := p.Metrics
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		name := RPCName(method)
		ctx, span := Tracer("taper/grpc").Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient))

		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.MD{}
		} else {
			md = md.Copy()
		}
		Propagator().Inject(ctx, mdCarrier{md: md})
		ctx = metadata.NewOutgoingContext(ctx, md)

		start := time.Now()
		m.inFlight.WithLabelValues(name).Inc()
		err := invoker(ctx, method, req, reply, cc, opts...)
		m.inFlight.WithLabelValues(name).Dec()
		setSpanStatus(span, err)
		m.ObserveRPC(name, time.Since(start), err)
		span.End()
		return err
	}
}

func setSpanStatus(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return
	}
	span.SetStatus(codes.Ok, "")
}

// mdCarrier adapts gRPC metadata.MD to an OTel TextMapCarrier.
type mdCarrier struct{ md metadata.MD }

func (c mdCarrier) Get(key string) string {
	if vs := c.md.Get(key); len(vs) > 0 {
		return vs[0]
	}
	return ""
}

func (c mdCarrier) Set(key, value string) { c.md.Set(key, value) }

func (c mdCarrier) Keys() []string {
	keys := make([]string, 0, len(c.md))
	for k := range c.md {
		keys = append(keys, k)
	}
	return keys
}

// Metrics holds the Prometheus RED metrics for a service's gRPC surface.
type Metrics struct {
	reg          *prometheus.Registry
	reqDuration  *prometheus.HistogramVec
	reqErrors    *prometheus.CounterVec
	inFlight     *prometheus.GaugeVec
	breakerState *prometheus.GaugeVec
}

// NewMetrics builds and registers the RED metric set on a fresh registry
// (plus the Go and process collectors). One Metrics per service binary.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &Metrics{
		reg: reg,
		reqDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "taper_grpc_request_duration_seconds",
			Help:    "gRPC request latency by method and status code (RED).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
		}, []string{"method", "code"}),
		reqErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taper_grpc_request_errors_total",
			Help: "gRPC request errors by method and status code (RED).",
		}, []string{"method", "code"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taper_grpc_requests_in_flight",
			Help: "Currently executing gRPC requests by method.",
		}, []string{"method"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taper_breaker_state",
			Help: "Circuit breaker state per dependency: 0=closed, 1=half-open, 2=open.",
		}, []string{"dependency"}),
	}
	reg.MustRegister(m.reqDuration, m.reqErrors, m.inFlight, m.breakerState)
	return m
}

// ObserveRPC records one completed RPC. Safe on a nil *Metrics (no-op).
func (m *Metrics) ObserveRPC(method string, elapsed time.Duration, err error) {
	if m == nil {
		return
	}
	code := status.Code(err)
	m.reqDuration.WithLabelValues(method, code.String()).Observe(elapsed.Seconds())
	if err != nil {
		m.reqErrors.WithLabelValues(method, code.String()).Inc()
	}
}

// SetBreakerState publishes a circuit breaker's current state as a gauge
// (0=closed, 1=half-open, 2=open) so the Fail-Fast Policy is visible.
func (m *Metrics) SetBreakerState(dependency, state string) {
	if m == nil {
		return
	}
	var v float64
	switch state {
	case "half-open":
		v = 1
	case "open":
		v = 2
	default:
		v = 0
	}
	m.breakerState.WithLabelValues(dependency).Set(v)
}

// Handler serves the Prometheus scrape endpoint for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// MetricsServer wraps the metrics handler in an HTTP server for the
// service's dedicated admin port; the caller runs and shuts it down.
func MetricsServer(m *Metrics, addr string) *http.Server {
	return &http.Server{Addr: addr, Handler: m.Handler(), ReadHeaderTimeout: 5 * time.Second}
}
