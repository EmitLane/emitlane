package broker

import (
	"context"
	"errors"
)

// Header names sent with every published record.
const (
	HeaderEventID             = "emitlane-event-id"
	HeaderEventType           = "emitlane-event-type"
	HeaderSchemaVersion       = "emitlane-schema-version"
	HeaderAttempt             = "emitlane-attempt"
	HeaderOriginalEvent       = "emitlane-original-event-id"
	HeaderReplayBatch         = "emitlane-replay-batch-id"
	HeaderOrderingKey         = "emitlane-ordering-key"
	HeaderSequence            = "emitlane-sequence"
	HeaderPartition           = "emitlane-ordering-partition"
	HeaderOriginalOrderingKey = "emitlane-original-ordering-key"
	HeaderOriginalSequence    = "emitlane-original-sequence"
	HeaderTraceparent         = "traceparent"
	HeaderTracestate          = "tracestate"
)

// ErrPermanent indicates a publish failure that should not be retried.
var ErrPermanent = errors.New("broker: permanent publish failure")

// Message is the broker-neutral publish envelope.
type Message struct {
	ID          string
	Destination string
	Key         []byte
	Payload     []byte
	Headers     map[string]string
}

// Publisher publishes a single message and waits for broker acknowledgement.
type Publisher interface {
	Publish(ctx context.Context, message Message) error
	Close() error
}

// IsPermanent reports whether err is a non-retryable publish failure.
func IsPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}
