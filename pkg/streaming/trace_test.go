package streaming

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordingSetup(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr
}

func TestHeaderInjectorAndExtractRoundTrip(t *testing.T) {
	recordingSetup(t)

	ctx, span := otel.GetTracerProvider().Tracer("test").Start(context.Background(), "produce")
	defer span.End()

	msg := kafka.Message{Topic: "stock.events", Key: []byte("k"), Value: []byte("v")}
	otel.GetTextMapPropagator().Inject(ctx, HeaderInjector(&msg))

	got := TraceContextFromMessage(context.Background(), msg)
	childSC := trace.SpanContextFromContext(got)
	if !childSC.IsValid() {
		t.Fatal("expected valid extracted span context")
	}
	if childSC.TraceID() != span.SpanContext().TraceID() {
		t.Errorf("extracted trace %s != producer trace %s", childSC.TraceID(), span.SpanContext().TraceID())
	}
	if childSC.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("extracted span %s != producer span %s (extraction should recover the producer span, not create one)",
			childSC.SpanID(), span.SpanContext().SpanID())
	}
}

func TestHeaderInjectorSetReplacesExisting(t *testing.T) {
	msg := kafka.Message{Headers: []kafka.Header{{Key: "traceparent", Value: []byte("00-stale-stale-00")}}}
	c := headerCarrier{h: &msg.Headers}
	c.Set("traceparent", "00-new-new-01")

	got := c.Get("traceparent")
	if got != "00-new-new-01" {
		t.Fatalf("Set must replace existing header, got %q", got)
	}
	if n := len(msg.Headers); n != 1 {
		t.Fatalf("expected 1 header after replace, got %d", n)
	}
}

func TestMessageSpanJoinsProducerTrace(t *testing.T) {
	sr := recordingSetup(t)

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanID = "00f067aa0ba902b7"
	msg := kafka.Message{
		Topic: "stock.events",
		Headers: []kafka.Header{
			{Key: TraceparentHeader, Value: []byte("00-" + traceID + "-" + spanID + "-01")},
		},
	}

	spanCtx, span := MessageSpan(context.Background(), msg)
	sc := trace.SpanContextFromContext(spanCtx)
	span.End()

	if sc.TraceID().String() != traceID {
		t.Errorf("consumer span trace %s, want producer trace %s", sc.TraceID(), traceID)
	}
	ended := sr.Ended()
	if len(ended) != 1 || ended[0].Name() != "stock.events process" {
		t.Fatalf("expected one 'stock.events process' span, got %+v", ended)
	}
	if ended[0].Parent().SpanID().String() != spanID {
		t.Errorf("consumer span parent %s, want producer span %s", ended[0].Parent().SpanID(), spanID)
	}
}

func TestMessageSpanWithoutTraceContextStartsFresh(t *testing.T) {
	recordingSetup(t)

	msg := kafka.Message{Topic: "stock.events"}
	spanCtx, span := MessageSpan(context.Background(), msg)
	sc := trace.SpanContextFromContext(spanCtx)
	span.End()

	if !sc.IsValid() {
		t.Fatal("expected a valid (root) consumer span when the message carries no trace context")
	}
}

func TestSpanLinkFromMessage(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanID = "00f067aa0ba902b7"
	msg := kafka.Message{
		Headers: []kafka.Header{
			{Key: TraceparentHeader, Value: []byte("00-" + traceID + "-" + spanID + "-01")},
		},
	}
	link := SpanLinkFromMessage(msg)
	if !link.SpanContext.IsValid() {
		t.Fatal("expected valid link span context")
	}
	if link.SpanContext.TraceID().String() != traceID {
		t.Errorf("link trace %s, want %s", link.SpanContext.TraceID(), traceID)
	}
	if got := SpanLinkFromMessage(kafka.Message{}); got.SpanContext.IsValid() {
		t.Error("expected invalid link for a message without trace context")
	}
}
