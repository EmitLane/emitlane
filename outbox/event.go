package outbox

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultContentType is used when Event.ContentType is empty.
	DefaultContentType = "application/json"

	// DefaultSchemaVersion is used when Event.SchemaVersion is 0.
	DefaultSchemaVersion = 1

	// kafkaTopicMaxLen is the Kafka topic name limit.
	kafkaTopicMaxLen = 249
)

// Event is the public outbox envelope.
//
// Payload is opaque bytes. JSON is a convenience helper, not a storage
// requirement. Tracing metadata is captured from context at enqueue time and
// stored separately from user Headers.
type Event struct {
	ID string

	Destination string
	Type        string

	Key []byte

	Payload     []byte
	ContentType string

	Headers map[string]string

	SchemaVersion int

	CorrelationID string
	CausationID   string

	AvailableAt time.Time

	// OrderingKey and Sequence opt this event into destination-scoped ordered
	// delivery. OrderingStartSequence is used only to initialize a new stream;
	// zero defaults that new stream to sequence 1.
	OrderingKey           string
	Sequence              int64
	OrderingStartSequence int64
}

func (e Event) validate() error {
	if strings.TrimSpace(e.Destination) == "" {
		return fmt.Errorf("%w: destination is required", ErrInvalidEvent)
	}
	if e.Destination != strings.TrimSpace(e.Destination) {
		return fmt.Errorf("%w: destination must not have surrounding whitespace", ErrInvalidEvent)
	}
	if len(e.Destination) > kafkaTopicMaxLen {
		return fmt.Errorf("%w: destination exceeds %d characters", ErrInvalidEvent, kafkaTopicMaxLen)
	}
	if strings.TrimSpace(e.Type) == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidEvent)
	}
	if e.Type != strings.TrimSpace(e.Type) {
		return fmt.Errorf("%w: type must not have surrounding whitespace", ErrInvalidEvent)
	}
	if e.ID != "" {
		if _, err := uuid.Parse(e.ID); err != nil {
			return fmt.Errorf("%w: id must be a UUID: %v", ErrInvalidEvent, err)
		}
	}
	if e.SchemaVersion < 0 {
		return fmt.Errorf("%w: schema version must be >= 0", ErrInvalidEvent)
	}
	if e.ContentType != "" && e.ContentType != strings.TrimSpace(e.ContentType) {
		return fmt.Errorf("%w: content type must not have surrounding whitespace", ErrInvalidEvent)
	}
	for name := range e.Headers {
		if name == "" {
			return fmt.Errorf("%w: header name must not be empty", ErrInvalidEvent)
		}
	}
	if e.OrderingKey == "" {
		if e.Sequence != 0 {
			return fmt.Errorf("%w: sequence requires an ordering key", ErrInvalidEvent)
		}
		if e.OrderingStartSequence != 0 {
			return fmt.Errorf("%w: ordering start sequence requires an ordering key", ErrInvalidEvent)
		}
		return nil
	}
	if strings.TrimSpace(e.OrderingKey) == "" {
		return fmt.Errorf("%w: ordering key must not be blank", ErrInvalidEvent)
	}
	if e.OrderingKey != strings.TrimSpace(e.OrderingKey) {
		return fmt.Errorf("%w: ordering key must not have surrounding whitespace", ErrInvalidEvent)
	}
	if e.Sequence <= 0 {
		return fmt.Errorf("%w: ordered sequence must be > 0", ErrInvalidEvent)
	}
	if e.OrderingStartSequence < 0 {
		return fmt.Errorf("%w: ordering start sequence must be >= 0", ErrInvalidEvent)
	}
	if e.OrderingStartSequence > e.Sequence {
		return fmt.Errorf("%w: ordering start sequence must not exceed sequence", ErrInvalidEvent)
	}
	if len(e.Key) > 0 && !bytes.Equal(e.Key, []byte(e.OrderingKey)) {
		return fmt.Errorf("%w: message key must equal ordering key for ordered events", ErrInvalidEvent)
	}
	return nil
}

func newEventID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("outbox: generate event id: %w", err)
	}
	return id.String(), nil
}
