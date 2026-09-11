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

func TestMessageSpanJoinsProducerTrace(t *testing.T) {
	sr := recordingSetup(t)

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanID = "00f067aa0ba902b7"
	msg := kafka.Message{
		Topic: "stock.events",
		Headers: []kafka.Header{
			{Key: "traceparent", Value: []byte("00-" + traceID + "-" + spanID + "-01")},
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
