// Package consumer provides the broker-neutral managed consumer runtime.
package consumer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const EventIDHeader = "emitlane-event-id"

// Header preserves Kafka header order and duplicate keys.
type Header struct {
	Key   string
	Value []byte
}

// Message is the business-facing record passed to a managed handler.
type Message struct {
	EventID   uuid.UUID
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Payload   []byte
	Headers   []Header
	Attempt   int
}

type Handler func(context.Context, pgx.Tx, Message) error

// IdentityResolver supports non-EmitLane producers. The default requires the
// emitlane-event-id header and never derives identity from payload or key.
type IdentityResolver func(Message) (uuid.UUID, error)

// ResolveEventID is the default strict EmitLane header resolver.
func ResolveEventID(message Message) (uuid.UUID, error) {
	var resolved uuid.UUID
	found := false
	for _, header := range message.Headers {
		if !strings.EqualFold(strings.TrimSpace(header.Key), EventIDHeader) {
			continue
		}
		parsed, err := uuid.Parse(strings.TrimSpace(string(header.Value)))
		if err != nil {
			return uuid.Nil, fmt.Errorf("consumer: invalid %s header: %w", EventIDHeader, err)
		}
		if found && parsed != resolved {
			return uuid.Nil, fmt.Errorf("consumer: conflicting %s headers", EventIDHeader)
		}
		resolved = parsed
		found = true
	}
	if !found {
		return uuid.Nil, fmt.Errorf("consumer: missing %s header", EventIDHeader)
	}
	return resolved, nil
}
