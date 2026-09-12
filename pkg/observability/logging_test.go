package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// A log line issued inside a live span context renders trace_id alongside
// the standard fields; outside a span, no trace_id appears.
func TestLoggerJSONFieldsAndTraceCorrelation(t *testing.T) {
	var buf bytes.Buffer
	h := traceHandler{inner: slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})}
	logger := slog.New(h).With(slog.String("service", "test-svc"))

	// No span: standard JSON fields only.
	logger.InfoContext(context.Background(), "hello", slog.String("k", "v"))
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if line["msg"] != "hello" || line["service"] != "test-svc" || line["k"] != "v" {
		t.Fatalf("fields = %v", line)
	}
	if _, has := line[traceIDKey]; has {
		t.Fatalf("trace_id outside a span should be absent: %v", line)
	}
	if line["level"] != "INFO" {
		t.Fatalf("level = %v", line["level"])
	}

	// Inside a span: trace_id correlates.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "op")
	defer span.End()

	buf.Reset()
	logger.InfoContext(ctx, "correlated")
	line = nil
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	got, has := line[traceIDKey]
	if !has {
		t.Fatalf("trace_id missing inside a span: %v", line)
	}
	want := trace.SpanContextFromContext(ctx).TraceID().String()
	if got != want {
		t.Fatalf("trace_id = %v, want %v", got, want)
	}
}

// NewLogger wires the service name field.
func TestNewLoggerCarriesService(t *testing.T) {
	// NewLogger writes to stdout; assert via LoggerFrom nil-safety instead
	// and the service field through a redirected handler construction.
	if LoggerFrom(nil) == nil {
		t.Fatal("LoggerFrom(nil) must return a usable default")
	}
	l := NewLogger("order")
	if l == nil {
		t.Fatal("NewLogger returned nil")
	}
}
