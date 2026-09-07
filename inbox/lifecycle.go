package inbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Status is the durable managed Inbox lifecycle state.
type Status string

const (
	StatusPending   Status = "pending"
	StatusInflight  Status = "inflight"
	StatusRetryWait Status = "retry_wait"
	StatusProcessed Status = "processed"
	StatusDead      Status = "dead"
)

// Source identifies the Kafka record without persisting its payload.
type Source struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Event is the durable lifecycle and source metadata for one Inbox identity.
type Event struct {
	Consumer    string
	EventID     uuid.UUID
	Status      Status
	Attempts    int
	AvailableAt time.Time
	LeaseOwner  string
	LeaseToken  uuid.UUID
	LeaseUntil  *time.Time
	LastError   string
	FirstSeenAt time.Time
	UpdatedAt   time.Time
	ProcessedAt *time.Time
	Source      *Source
}

// ClaimDisposition describes why Claim did or did not return ownership.
type ClaimDisposition string

const (
	Claimed          ClaimDisposition = "claimed"
	AlreadyProcessed ClaimDisposition = "processed"
	ClaimWaiting     ClaimDisposition = "waiting"
	ClaimDead        ClaimDisposition = "dead"
)

// ClaimRequest describes one real handler attempt. A successful claim is the
// only operation that increments Attempts.
type ClaimRequest struct {
	Consumer      string
	EventID       uuid.UUID
	Source        Source
	LeaseOwner    string
	LeaseDuration time.Duration
}

// ClaimResult contains the current row and the claim disposition.
type ClaimResult struct {
	Disposition ClaimDisposition
	Event       Event
}

// Store is the durable managed Inbox port. MarkProcessed must use the same tx
// as the handler's protected business writes.
type Store interface {
	Claim(context.Context, ClaimRequest) (ClaimResult, error)
	Renew(context.Context, string, uuid.UUID, uuid.UUID, time.Duration) error
	MarkProcessed(context.Context, pgx.Tx, string, uuid.UUID, uuid.UUID) error
	MarkRetry(context.Context, string, uuid.UUID, uuid.UUID, time.Duration, string) error
	MarkDead(context.Context, string, uuid.UUID, uuid.UUID, string) error
	Get(context.Context, string, uuid.UUID) (Event, error)
}
