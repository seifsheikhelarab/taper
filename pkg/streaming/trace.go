package streaming

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TraceparentHeader is the Kafka header carrying the W3C traceparent of the
// trace that produced the event. Producers write it via the outbox
// traceparent column (Debezium's additional.placement promotes it to this
// header); direct producers set it with HeaderInjector.
const TraceparentHeader = "traceparent"

// headerCarrier adapts kafka-go message headers to an OTel TextMapCarrier.
type headerCarrier struct{ h *[]kafka.Header }

func (c headerCarrier) Get(key string) string {
	for _, kv := range *c.h {
		if kv.Key == key {
			return string(kv.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	h := *c.h
	for i := range h {
		if h[i].Key == key {
			h[i].Value = []byte(value)
			return
		}
	}
	*c.h = append(h, kafka.Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.h))
	for _, kv := range *c.h {
		keys = append(keys, kv.Key)
	}
	return keys
}

// HeaderInjector returns a TextMapCarrier that injects trace context into
// msg's headers. Direct producers (dlqreplay, DLQ writers) use it with
// Propagator().Inject so non-CDC messages join the trace too:
//
//	msg := kafka.Message{...}
//	observability.Propagator().Inject(ctx, streaming.HeaderInjector(&msg))
func HeaderInjector(msg *kafka.Message) propagation.TextMapCarrier {
	return headerCarrier{h: &msg.Headers}
}

// TraceContextFromMessage extracts the W3C trace context carried in msg's
// headers and returns a context descended from it. Use this as the parent
// for per-message consumer spans:
//
//	ctx := streaming.TraceContextFromMessage(ctx, msg)
//	ctx, span := obs.Tracer("consumer").Start(ctx, topic, trace.WithLinks(links...))
func TraceContextFromMessage(ctx context.Context, msg kafka.Message) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{h: &msg.Headers})
}

// SpanLinkFromMessage builds a span Link to the producing trace, for
// consumers that open their own consumer-span instead of joining the
// producer's trace outright (the sampled-trace linkage used by reactors).
func SpanLinkFromMessage(msg kafka.Message) trace.Link {
	sc := trace.SpanContextFromContext(TraceContextFromMessage(context.Background(), msg))
	if !sc.IsValid() {
		return trace.Link{}
	}
	return trace.Link{SpanContext: sc}
}

// MessageSpan extracts the producer trace context from msg and starts a
// per-message consumer span joined to it (named "<topic> process", kind
// consumer). The returned context carries both the span and the producer
// trace, so downstream work in the handler — including outbox writes —
// extends the same distributed trace. End the span after processing.
func MessageSpan(ctx context.Context, msg kafka.Message) (context.Context, trace.Span) {
	parent := TraceContextFromMessage(ctx, msg)
	return otel.GetTracerProvider().Tracer("taper/streaming").Start(
		parent, msg.Topic+" process", trace.WithSpanKind(trace.SpanKindConsumer))
}
