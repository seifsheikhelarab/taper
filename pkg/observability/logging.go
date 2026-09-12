// Structured logging (spec #52, B4 / US22): log/slog with a JSON handler on
// stdout (pods) and a correlation field carried from the trace context
// already propagated (pkg/observability): every log line issued while a
// request's span context is in scope renders its W3C trace_id, so logs
// filter and aggregate alongside traces.
package observability

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// traceIDKey is the structured field name carrying the W3C trace id.
const traceIDKey = "trace_id"

// traceHandler wraps a slog.Handler, appending the span context's trace id
// from the record's context so correlation needs no call-site effort.
type traceHandler struct {
	inner slog.Handler
}

func (h traceHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String(traceIDKey, sc.TraceID().String()))
	}
	return h.inner.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{inner: h.inner.WithGroup(name)}
}

// NewLogger builds the service's slog.Logger: JSON on stdout with the
// service name on every record and trace_id appended when a valid span
// context is in scope.
func NewLogger(service string) *slog.Logger {
	h := traceHandler{inner: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})}
	return slog.New(h).With(slog.String("service", service))
}

// LoggerFrom returns logger, or the default when nil, so constructors stay
// nil-safe (the prior log.Printf func(format, args...) convention).
func LoggerFrom(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

// SetDefaultLogger makes a NewLogger(service) the package-default slog
// logger AND redirects the standard library logger to it, so every
// existing log.Printf call site (binaries, worker loops, deps like
// outboxprune) renders as correlated JSON without a mechanical sweep:
// log output goes through the slog handler and gains service + trace_id.
func SetDefaultLogger(service string) {
	logger := NewLogger(service)
	slog.SetDefault(logger)
	log.SetOutput(&slogWriter{logger: logger})
	log.SetFlags(0) // the JSON handler owns formatting
}

// slogWriter adapts a slog.Logger to io.Writer for log.SetOutput:
// each Write is one formatted line (log.Printf guarantees newline
// termination) emitted at Info with the message as-is.
type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimSuffix(string(p), "\n")
	w.logger.Info(msg)
	return len(p), nil
}
