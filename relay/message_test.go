package relay

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/broker"
)

func TestToMessageMapping(t *testing.T) {
	t.Parallel()
	id := uuid.Must(uuid.NewV7())
	ev := Event{
		ID:            id,
		Destination:   "orders.events",
		Type:          "order.created",
		Key:           []byte("ord-1"),
		Payload:       []byte(`{}`),
		SchemaVersion: 2,
		Attempts:      3,
		Headers: map[string]string{
			"source":                   "app",
			broker.HeaderEventID:       "spoofed",
			broker.HeaderSchemaVersion: "999",
			broker.HeaderAttempt:       "999",
			broker.HeaderTraceparent:   "spoofed",
			broker.HeaderTracestate:    "spoofed",
		},
		Traceparent:   "00-trace-01",
		CorrelationID: "c1",
	}
	msg := toMessage(ev)
	if msg.Destination != "orders.events" || string(msg.Key) != "ord-1" {
		t.Fatalf("routing %#v", msg)
	}
	if msg.Headers[broker.HeaderEventID] != id.String() {
		t.Fatal("event id")
	}
	if msg.Headers[broker.HeaderAttempt] != "3" {
		t.Fatal("attempt")
	}
	if msg.Headers["source"] != "app" {
		t.Fatal("user header")
	}
	if msg.Headers[broker.HeaderTraceparent] != "00-trace-01" {
		t.Fatal("traceparent")
	}
	if _, exists := msg.Headers[broker.HeaderTracestate]; exists {
		t.Fatal("user tracestate must not survive without stored trace state")
	}
}

func BenchmarkToMessage(b *testing.B) {
	ev := Event{
		ID:            uuid.Must(uuid.NewV7()),
		Destination:   "orders.events",
		Type:          "order.created",
		Key:           []byte("ord-1"),
		Payload:       []byte(`{"ok":true}`),
		SchemaVersion: 1,
		Attempts:      1,
		Headers:       map[string]string{"source": "bench"},
		CreatedAt:     time.Now(),
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = toMessage(ev)
	}
}
