package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextRoundTrip(t *testing.T) {
	t.Parallel()
	traceID, err := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	if err != nil {
		t.Fatal(err)
	}
	state, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		TraceState: state,
		Remote:     false,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	traceparent, tracestate := InjectTrace(ctx)
	if traceparent != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Fatalf("traceparent %q", traceparent)
	}
	if tracestate != "vendor=value" {
		t.Fatalf("tracestate %q", tracestate)
	}

	restored := trace.SpanContextFromContext(ExtractTrace(context.Background(), traceparent, tracestate))
	if !restored.IsRemote() {
		t.Fatal("extracted span context must be remote")
	}
	if restored.TraceID() != traceID || restored.SpanID() != spanID || restored.TraceState().String() != state.String() {
		t.Fatalf("restored span context %#v", restored)
	}
}
