package outbox

import (
	"strings"
	"testing"
)

func TestEventValidate(t *testing.T) {
	t.Parallel()
	ok := Event{Destination: "orders.events", Type: "order.created"}
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Event{Type: "order.created"}).validate(); err == nil {
		t.Fatal("expected destination error")
	}
	if err := (Event{Destination: "orders.events"}).validate(); err == nil {
		t.Fatal("expected type error")
	}
	if err := (Event{Destination: "orders.events", Type: "t", ID: "not-a-uuid"}).validate(); err == nil {
		t.Fatal("expected id error")
	}
	if err := (Event{Destination: strings.Repeat("a", 250), Type: "t"}).validate(); err == nil {
		t.Fatal("expected destination length error")
	}
	if err := (Event{Destination: " orders.events ", Type: "t"}).validate(); err == nil {
		t.Fatal("expected destination whitespace error")
	}
	if err := (Event{Destination: "orders.events", Type: " t "}).validate(); err == nil {
		t.Fatal("expected type whitespace error")
	}
	if err := (Event{Destination: "orders.events", Type: "t", ContentType: "  "}).validate(); err == nil {
		t.Fatal("expected content type error")
	}
	if err := (Event{Destination: "orders.events", Type: "t", ContentType: " application/json "}).validate(); err == nil {
		t.Fatal("expected content type surrounding whitespace error")
	}
	if err := (Event{Destination: "d", Type: "t", Headers: map[string]string{"": "x"}}).validate(); err == nil {
		t.Fatal("expected header error")
	}
	empty := Event{Destination: "d", Type: "t", Payload: nil}
	if err := empty.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestNewEventID(t *testing.T) {
	t.Parallel()
	a, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("ids must be unique")
	}
	if len(a) != 36 {
		t.Fatalf("unexpected id %q", a)
	}
}
