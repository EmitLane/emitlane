package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/emitlane/emitlane"

var propagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// TextMapPropagator returns the W3C propagator used across the outbox/Kafka
// boundary.
func TextMapPropagator() propagation.TextMapPropagator {
	return propagator
}

// Tracer returns the EmitLane tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string { return c[key] }

func (c mapCarrier) Set(key, value string) { c[key] = value }

func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTrace extracts W3C traceparent/tracestate from ctx for durable storage.
func InjectTrace(ctx context.Context) (traceparent, tracestate string) {
	carrier := mapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier["traceparent"], carrier["tracestate"]
}

// ExtractTrace returns a context parented by the stored W3C trace context.
func ExtractTrace(ctx context.Context, traceparent, tracestate string) context.Context {
	carrier := mapCarrier{}
	if traceparent != "" {
		carrier["traceparent"] = traceparent
	}
	if tracestate != "" {
		carrier["tracestate"] = tracestate
	}
	if len(carrier) == 0 {
		return ctx
	}
	return propagator.Extract(ctx, carrier)
}
