package relay

import (
	"github.com/emitlane/emitlane/consumer"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/broker"
)

func TestToMessageMapping(t *testing.T) {
	t.Parallel()
	id := uuid.Must(uuid.NewV7())
	sourceID := uuid.Must(uuid.NewV7())
	batchID := uuid.Must(uuid.NewV7())
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
			broker.HeaderOriginalEvent: "spoofed",
			broker.HeaderReplayBatch:   "spoofed",
		},
		Traceparent:         "00-trace-01",
		CorrelationID:       "c1",
		ReplayedFromEventID: &sourceID,
		ReplayBatchID:       &batchID,
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
	if msg.Headers[broker.HeaderOriginalEvent] != sourceID.String() || msg.Headers[broker.HeaderReplayBatch] != batchID.String() {
		t.Fatal("durable replay provenance must override user headers")
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

// A valid stored event must remain consumable even when application headers
// contain aliases of reserved metadata. The consumer normalizes those aliases.
func TestToMessageReservedHeaderAliases(t *testing.T) {
	reserved := []string{
		broker.HeaderEventID, broker.HeaderEventType, broker.HeaderSchemaVersion,
		broker.HeaderAttempt, broker.HeaderOriginalEvent, broker.HeaderReplayBatch,
		broker.HeaderOrderingKey, broker.HeaderSequence, broker.HeaderPartition,
		broker.HeaderTraceparent, broker.HeaderTracestate,
		"emitlane-correlation-id", "emitlane-causation-id",
	}
	for _, name := range reserved {
		for _, alias := range []string{name, strings.ToUpper(name), " " + strings.ToUpper(name) + "\t"} {
			t.Run(alias, func(t *testing.T) {
				ev := Event{ID: uuid.New(), Type: "created", SchemaVersion: 1, Attempts: 1,
					Headers: map[string]string{alias: "spoofed", "emitlane-custom": "app", " X-App ": "unchanged"}}
				msg := toMessage(ev)
				var headers []consumer.Header
				for key, value := range msg.Headers {
					if value == "spoofed" {
						t.Fatalf("reserved alias survived: %q", key)
					}
					headers = append(headers, consumer.Header{Key: key, Value: []byte(value)})
				}
				got, err := consumer.ResolveEventID(consumer.Message{Headers: headers})
				if err != nil || got != ev.ID {
					t.Fatalf("identity=%s err=%v; want %s", got, err, ev.ID)
				}
				if msg.Headers["emitlane-custom"] != "app" || msg.Headers[" X-App "] != "unchanged" {
					t.Fatal("application header modified")
				}
				if ev.Headers[alias] != "spoofed" {
					t.Fatal("stored headers mutated")
				}
			})
		}
	}
}

func TestToMessagePreservesUnorderedReplayProvenance(t *testing.T) {
	sourceID, batchID := uuid.New(), uuid.New()
	msg := toMessage(Event{
		ID: uuid.New(), ReplayedFromEventID: &sourceID, ReplayBatchID: &batchID,
		Headers: map[string]string{
			broker.HeaderOriginalOrderingKey: "order-123",
			broker.HeaderOriginalSequence:    "17",
			broker.HeaderOrderingKey:         "stale-current-order",
		},
	})
	if msg.Headers[broker.HeaderOriginalOrderingKey] != "order-123" || msg.Headers[broker.HeaderOriginalSequence] != "17" {
		t.Fatal("unordered replay lost its persisted ordering provenance")
	}
	if _, exists := msg.Headers[broker.HeaderOrderingKey]; exists {
		t.Fatal("unordered replay regained active ordering metadata")
	}
	if msg.Headers[broker.HeaderOriginalEvent] != sourceID.String() || msg.Headers[broker.HeaderReplayBatch] != batchID.String() {
		t.Fatal("unordered replay lost its durable identity provenance")
	}
}
