package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
	MaxReplayBatch  = 1000
)

var (
	ErrInvalid  = errors.New("admin: invalid request")
	ErrNotFound = errors.New("admin: not found")
	ErrConflict = errors.New("admin: conflict")
)

type Event struct {
	ID                  uuid.UUID         `json:"id"`
	Destination         string            `json:"destination"`
	Type                string            `json:"event_type"`
	ContentType         string            `json:"content_type"`
	SchemaVersion       int               `json:"schema_version"`
	CorrelationID       string            `json:"correlation_id,omitempty"`
	CausationID         string            `json:"causation_id,omitempty"`
	Status              string            `json:"status"`
	Attempts            int               `json:"attempts"`
	AvailableAt         time.Time         `json:"available_at"`
	LeaseOwner          string            `json:"lease_owner,omitempty"`
	LeaseUntil          *time.Time        `json:"lease_until,omitempty"`
	LastError           string            `json:"last_error,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	DeliveredAt         *time.Time        `json:"delivered_at,omitempty"`
	PayloadSize         int64             `json:"payload_size"`
	ReplayedFromEventID *uuid.UUID        `json:"replayed_from_event_id,omitempty"`
	ReplayBatchID       *uuid.UUID        `json:"replay_batch_id,omitempty"`
	OrderingKey         string            `json:"ordering_key,omitempty"`
	OrderingSequence    int64             `json:"ordering_sequence,omitempty"`
	OrderingPartition   *int16            `json:"ordering_partition,omitempty"`
	KeyBase64           *string           `json:"message_key_base64,omitempty"`
	PayloadBase64       *string           `json:"payload_base64,omitempty"`
	Headers             map[string]string `json:"headers,omitempty"`
}

type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

func EncodeCursor(c Cursor) (string, error) {
	if c.CreatedAt.IsZero() || c.ID == uuid.Nil {
		return "", fmt.Errorf("%w: cursor is incomplete", ErrInvalid)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeCursor(value string) (*Cursor, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	if len(value) > 1024 {
		return nil, fmt.Errorf("%w: cursor is too long", ErrInvalid)
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	var c Cursor
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil || c.CreatedAt.IsZero() || c.ID == uuid.Nil {
		return nil, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	return &c, nil
}

type EventFilter struct {
	Statuses      []string
	Destination   string
	EventType     string
	CreatedFrom   *time.Time
	CreatedTo     *time.Time
	ReplayBatchID *uuid.UUID
	Cursor        *Cursor
	Limit         int
}

func (f EventFilter) HasSelector() bool {
	return len(f.Statuses) > 0 || strings.TrimSpace(f.Destination) != "" ||
		strings.TrimSpace(f.EventType) != "" || f.CreatedFrom != nil || f.CreatedTo != nil || f.ReplayBatchID != nil
}

type EventPage struct {
	Events     []Event `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

type ControlState struct {
	Paused    bool      `json:"paused"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by"`
}

type RelayInstance struct {
	InstanceID      string     `json:"instance_id"`
	Hostname        string     `json:"hostname"`
	Version         string     `json:"version"`
	StartedAt       time.Time  `json:"started_at"`
	LastHeartbeatAt time.Time  `json:"last_heartbeat_at"`
	StoppedAt       *time.Time `json:"stopped_at,omitempty"`
	State           string     `json:"state"`
	OrderingCapable bool       `json:"ordering_capable"`
}

type Stats struct {
	Pending               int64   `json:"pending"`
	PendingDue            int64   `json:"pending_due"`
	PendingScheduled      int64   `json:"pending_scheduled"`
	Inflight              int64   `json:"inflight"`
	DeliveredRetained     int64   `json:"delivered_retained"`
	Dead                  int64   `json:"dead"`
	OldestPendingSeconds  float64 `json:"oldest_pending_seconds"`
	Paused                bool    `json:"paused"`
	ActiveRelays          int64   `json:"active_relays"`
	StaleRelays           int64   `json:"stale_relays"`
	StoppedRelays         int64   `json:"stopped_relays"`
	OrderedStreams        int64   `json:"ordered_streams"`
	BlockedOrderedStreams int64   `json:"blocked_ordered_streams"`
	GapStreams            int64   `json:"gap_streams"`
	DeadBlockedStreams    int64   `json:"dead_blocked_streams"`
	OwnedPartitions       int64   `json:"owned_partitions"`
	HandoffPartitions     int64   `json:"handoff_partitions"`
}

type OrderingStreamCursor struct {
	Destination string `json:"destination"`
	OrderingKey string `json:"ordering_key"`
}

func EncodeOrderingStreamCursor(c OrderingStreamCursor) (string, error) {
	if strings.TrimSpace(c.Destination) == "" || strings.TrimSpace(c.OrderingKey) == "" {
		return "", fmt.Errorf("%w: ordering stream cursor is incomplete", ErrInvalid)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode ordering stream cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeOrderingStreamCursor(value string) (*OrderingStreamCursor, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	if len(value) > 4096 {
		return nil, fmt.Errorf("%w: ordering stream cursor is too long", ErrInvalid)
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed ordering stream cursor", ErrInvalid)
	}
	var cursor OrderingStreamCursor
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil || strings.TrimSpace(cursor.Destination) == "" || strings.TrimSpace(cursor.OrderingKey) == "" {
		return nil, fmt.Errorf("%w: malformed ordering stream cursor", ErrInvalid)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: malformed ordering stream cursor", ErrInvalid)
	}
	return &cursor, nil
}

type OrderingStream struct {
	Destination          string     `json:"destination"`
	OrderingKey          string     `json:"ordering_key"`
	Partition            int16      `json:"partition"`
	StartSequence        int64      `json:"start_sequence"`
	NextSequence         int64      `json:"next_sequence"`
	State                string     `json:"state"`
	NextEventID          *uuid.UUID `json:"next_event_id,omitempty"`
	NextEventStatus      string     `json:"next_event_status,omitempty"`
	NextEventAttempts    int        `json:"next_event_attempts,omitempty"`
	LowestFutureSequence *int64     `json:"lowest_future_sequence,omitempty"`
	GapSize              int64      `json:"gap_size,omitempty"`
	GapAgeSeconds        float64    `json:"gap_age_seconds,omitempty"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

type OrderingStreamFilter struct {
	State       string
	Destination string
	Partition   *int16
	BlockedOnly bool
	Limit       int
	Cursor      *OrderingStreamCursor
}

type OrderingStreamPage struct {
	Streams    []OrderingStream `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type OrderingPartition struct {
	PartitionID      int16      `json:"partition_id"`
	DesiredOwner     string     `json:"desired_owner,omitempty"`
	ActualOwner      string     `json:"actual_owner,omitempty"`
	Epoch            int64      `json:"epoch"`
	LeaseUntil       *time.Time `json:"lease_until,omitempty"`
	HandoffNotBefore *time.Time `json:"handoff_not_before,omitempty"`
	State            string     `json:"state"`
}

type AuditRecord struct {
	ID            uuid.UUID      `json:"id"`
	Action        string         `json:"action"`
	Actor         string         `json:"actor"`
	Reason        string         `json:"reason,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
	TargetEventID *uuid.UUID     `json:"target_event_id,omitempty"`
	ReplayBatchID *uuid.UUID     `json:"replay_batch_id,omitempty"`
	Details       map[string]any `json:"details"`
	CreatedAt     time.Time      `json:"created_at"`
}

type AuditFilter struct {
	Action string
	Cursor *Cursor
	Limit  int
}

type AuditPage struct {
	Records    []AuditRecord `json:"records"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type Mutation struct {
	Actor        string
	Reason       string
	RequestID    string
	Traceparent  string
	Tracestate   string
	OrderingMode string
}

type ReplayPreview struct {
	Count  int     `json:"count"`
	Sample []Event `json:"sample"`
	Limit  int     `json:"execution_limit"`
}

type ReplayResult struct {
	ReplayBatchID uuid.UUID   `json:"replay_batch_id"`
	Created       int         `json:"created"`
	EventIDs      []uuid.UUID `json:"event_ids,omitempty"`
}

// Store is the durable administrative port. Mutating implementations must
// write the state change and its audit row in one PostgreSQL transaction.
type Store interface {
	OperationalStats(ctx context.Context, staleAfter time.Duration) (Stats, error)
	ListEvents(ctx context.Context, filter EventFilter) (EventPage, error)
	InspectEvent(ctx context.Context, id uuid.UUID, includeSensitive bool) (Event, error)
	ListRelays(ctx context.Context, staleAfter time.Duration) ([]RelayInstance, error)
	GetRelay(ctx context.Context, id string, staleAfter time.Duration) (RelayInstance, error)
	ControlState(ctx context.Context) (ControlState, error)
	SetPaused(ctx context.Context, paused bool, mutation Mutation) (ControlState, error)
	RetryDeadAudited(ctx context.Context, id uuid.UUID, mutation Mutation) error
	ReplayEvent(ctx context.Context, sourceID uuid.UUID, mutation Mutation) (ReplayResult, error)
	PreviewReplay(ctx context.Context, filter EventFilter, sampleLimit, executionLimit int) (ReplayPreview, error)
	ReplayBatch(ctx context.Context, filter EventFilter, mutation Mutation, limit int) (ReplayResult, error)
	ListAudit(ctx context.Context, filter AuditFilter) (AuditPage, error)
	ListOrderingStreams(ctx context.Context, filter OrderingStreamFilter) (OrderingStreamPage, error)
	InspectOrderingStream(ctx context.Context, destination, orderingKey string) (OrderingStream, error)
	ListOrderingPartitions(ctx context.Context, staleAfter time.Duration) ([]OrderingPartition, error)
}
