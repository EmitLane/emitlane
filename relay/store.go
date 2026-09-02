package relay

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Event is a claimed outbox event. It is internal relay state rather than part
// of the producer-facing outbox API.
type Event struct {
	ID                  uuid.UUID
	Destination         string
	Type                string
	Key                 []byte
	Payload             []byte
	ContentType         string
	Headers             map[string]string
	SchemaVersion       int
	CorrelationID       string
	CausationID         string
	Traceparent         string
	Tracestate          string
	Status              string
	Attempts            int
	AvailableAt         time.Time
	LeaseOwner          string
	LeaseUntil          *time.Time
	LastError           string
	CreatedAt           time.Time
	DeliveredAt         *time.Time
	ReplayedFromEventID *uuid.UUID
	ReplayBatchID       *uuid.UUID
}

// Stats is a point-in-time snapshot of durable relay state.
type Stats struct {
	Pending              int64
	Inflight             int64
	Dead                 int64
	OldestPendingSeconds float64
	Paused               bool
	RelaysActive         int64
	RelaysStale          int64
}

// Store is the relay's durable outbox port. Implementations must commit Claim
// before returning and condition every state transition on the expected lease
// owner.
type Store interface {
	Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Event, error)
	BeginAttempt(ctx context.Context, id uuid.UUID, owner string, maxAttempts int) (int, error)
	MarkDelivered(ctx context.Context, id uuid.UUID, owner string) error
	MarkRetry(ctx context.Context, id uuid.UUID, owner string, delay time.Duration, lastError string) error
	MarkDead(ctx context.Context, id uuid.UUID, owner string, lastError string) error
	StatsSnapshot(ctx context.Context) (Stats, error)
	CleanupDelivered(ctx context.Context, retention time.Duration, limit int) (int64, error)
}

// WakeupListener turns an optional low-latency signal into relay wake-ups.
// Delivery correctness never depends on this interface because polling remains
// mandatory.
type WakeupListener interface {
	Run(ctx context.Context, wake chan<- struct{})
}

// PauseState is an optional Store capability. The PostgreSQL implementation
// also checks this state inside Claim, closing the race between the read and
// the claim transaction. Stores that do not implement it keep v0.1 behavior.
type PauseState interface {
	RelayPaused(ctx context.Context) (bool, error)
}

// StatsWithPresence is an optional capability for stores that can classify
// relay heartbeats using the caller's configured stale threshold.
type StatsWithPresence interface {
	StatsSnapshotWithPresence(ctx context.Context, staleAfter time.Duration) (Stats, error)
}

// PresenceStore is an optional visibility capability. Presence failures must
// never stop delivery because relay_instances is not delivery state.
type PresenceStore interface {
	RegisterRelay(ctx context.Context, presence RelayPresence) error
	HeartbeatRelay(ctx context.Context, instanceID string) error
	MarkRelayStopped(ctx context.Context, instanceID string) error
}

type RelayPresence struct {
	InstanceID string
	Hostname   string
	Version    string
	StartedAt  time.Time
}
