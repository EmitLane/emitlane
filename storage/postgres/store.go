package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/emitlane/emitlane/relay"
	"github.com/emitlane/emitlane/telemetry"
)

const notifyChannel = "emitlane_events"

const maxErrorBytes = 4096

// Store is the PostgreSQL outbox/inbox store used by the relay.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres store: pool is required")
	}
	return &Store{pool: pool}, nil
}

func sanitizeError(msg string) string {
	msg = strings.ToValidUTF8(strings.ReplaceAll(msg, "\x00", "�"), "�")
	if len(msg) <= maxErrorBytes {
		return msg
	}
	end := maxErrorBytes
	for end > 0 && !utf8.ValidString(msg[:end]) {
		end--
	}
	return msg[:end]
}

func intervalMS(d time.Duration) int64 {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return ms
}

func mapHeaders(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode event headers: %w", err)
	}
	if out == nil {
		return map[string]string{}, nil
	}
	return out, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(row rowScanner) (relay.Event, error) {
	var (
		e          relay.Event
		key        []byte
		headers    []byte
		corr       *string
		caus       *string
		tp         *string
		ts         *string
		owner      *string
		lastErr    *string
		leaseUntil *time.Time
		delivered  *time.Time
	)
	err := row.Scan(
		&e.ID,
		&e.Destination,
		&e.Type,
		&key,
		&e.Payload,
		&e.ContentType,
		&headers,
		&e.SchemaVersion,
		&corr,
		&caus,
		&tp,
		&ts,
		&e.Status,
		&e.Attempts,
		&e.AvailableAt,
		&owner,
		&leaseUntil,
		&lastErr,
		&e.CreatedAt,
		&delivered,
	)
	if err != nil {
		return relay.Event{}, err
	}
	e.Key = key
	e.Headers, err = mapHeaders(headers)
	if err != nil {
		return relay.Event{}, fmt.Errorf("event %s: %w", e.ID, err)
	}
	e.CorrelationID = deref(corr)
	e.CausationID = deref(caus)
	e.Traceparent = deref(tp)
	e.Tracestate = deref(ts)
	e.LeaseOwner = deref(owner)
	e.LeaseUntil = leaseUntil
	e.LastError = deref(lastErr)
	e.DeliveredAt = delivered
	return e, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

const eventColumns = `
    id,
    destination,
    event_type,
    message_key,
    payload,
    content_type,
    headers,
    schema_version,
    correlation_id,
    causation_id,
    traceparent,
    tracestate,
    status,
    attempts,
    available_at,
    lease_owner,
    lease_until,
    last_error,
    created_at,
    delivered_at`

// Claim marks a bounded batch inflight and commits before returning.
// Eligible rows are pending (available now) or inflight with an expired lease.
// Publish attempts are recorded separately by BeginAttempt, immediately before
// broker I/O.
func (s *Store) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]relay.Event, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "emitlane.claim",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()
	span.SetAttributes(
		attribute.String("emitlane.relay_instance", owner),
		attribute.Int("emitlane.batch_size", limit),
	)

	if limit <= 0 {
		return nil, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const sql = `
WITH picked AS (
    SELECT id
    FROM emitlane.outbox_events
    WHERE available_at <= NOW()
      AND (
            status = 'pending'
         OR (status = 'inflight' AND lease_until IS NOT NULL AND lease_until <= NOW())
      )
    ORDER BY available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE emitlane.outbox_events AS e
SET
    status = 'inflight',
    lease_owner = $2,
    lease_until = NOW() + ($3 * INTERVAL '1 millisecond')
FROM picked
WHERE e.id = picked.id
RETURNING
    e.id,
    e.destination,
    e.event_type,
    e.message_key,
    e.payload,
    e.content_type,
    e.headers,
    e.schema_version,
    e.correlation_id,
    e.causation_id,
    e.traceparent,
    e.tracestate,
    e.status,
    e.attempts,
    e.available_at,
    e.lease_owner,
    e.lease_until,
    e.last_error,
    e.created_at,
    e.delivered_at`

	rows, err := tx.Query(ctx, sql, limit, owner, intervalMS(lease))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("claim query: %w", err)
	}
	defer rows.Close()

	var events []relay.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("claim scan: %w", err)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("claim rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("claim commit: %w", err)
	}
	span.SetAttributes(attribute.Int("emitlane.claimed", len(events)))
	return events, nil
}

// BeginAttempt increments the persisted one-based publish attempt immediately
// before broker I/O. It is conditional on the current lease owner and budget.
func (s *Store) BeginAttempt(ctx context.Context, id uuid.UUID, owner string, maxAttempts int) (int, error) {
	const sql = `
UPDATE emitlane.outbox_events
SET attempts = attempts + 1
WHERE id = $1
  AND lease_owner = $2
  AND status = 'inflight'
  AND attempts < $3
RETURNING attempts`
	var attempt int
	if err := s.pool.QueryRow(ctx, sql, id, owner, maxAttempts).Scan(&attempt); err != nil {
		return 0, fmt.Errorf("begin publish attempt %s: %w", id, err)
	}
	return attempt, nil
}

// MarkDelivered transitions inflight → delivered for the owning worker only.
func (s *Store) MarkDelivered(ctx context.Context, id uuid.UUID, owner string) error {
	const sql = `
UPDATE emitlane.outbox_events
SET
    status = 'delivered',
    delivered_at = NOW(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = NULL
WHERE id = $1
  AND lease_owner = $2
  AND status = 'inflight'`
	tag, err := s.pool.Exec(ctx, sql, id, owner)
	if err != nil {
		return fmt.Errorf("mark delivered %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark delivered %s: no matching inflight row for owner %s", id, owner)
	}
	return nil
}

// MarkRetry transitions inflight → pending with a future available_at and clears the lease.
func (s *Store) MarkRetry(ctx context.Context, id uuid.UUID, owner string, delay time.Duration, lastError string) error {
	const sql = `
UPDATE emitlane.outbox_events
SET
    status = 'pending',
    available_at = NOW() + ($3 * INTERVAL '1 millisecond'),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = $4
WHERE id = $1
  AND lease_owner = $2
  AND status = 'inflight'`
	tag, err := s.pool.Exec(ctx, sql, id, owner, intervalMS(delay), sanitizeError(lastError))
	if err != nil {
		return fmt.Errorf("mark retry %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark retry %s: no matching inflight row for owner %s", id, owner)
	}
	return nil
}

// MarkDead transitions inflight → dead. Dead rows are never deleted automatically.
func (s *Store) MarkDead(ctx context.Context, id uuid.UUID, owner string, lastError string) error {
	const sql = `
UPDATE emitlane.outbox_events
SET
    status = 'dead',
    lease_owner = NULL,
    lease_until = NULL,
    last_error = $3
WHERE id = $1
  AND lease_owner = $2
  AND status = 'inflight'`
	tag, err := s.pool.Exec(ctx, sql, id, owner, sanitizeError(lastError))
	if err != nil {
		return fmt.Errorf("mark dead %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark dead %s: no matching inflight row for owner %s", id, owner)
	}
	return nil
}

// RetryDead moves a dead event back to pending with attempts reset.
func (s *Store) RetryDead(ctx context.Context, id uuid.UUID) error {
	const sql = `
UPDATE emitlane.outbox_events
SET
    status = 'pending',
    available_at = NOW(),
    attempts = 0,
    lease_owner = NULL,
    lease_until = NULL
WHERE id = $1
  AND status = 'dead'`
	tag, err := s.pool.Exec(ctx, sql, id)
	if err != nil {
		return fmt.Errorf("retry dead %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("retry dead %s: not found or not dead", id)
	}
	return nil
}

// ListDead returns the most recent dead events without payloads.
func (s *Store) ListDead(ctx context.Context, limit int) ([]relay.Event, error) {
	if limit <= 0 {
		limit = 50
	}
	const sql = `
SELECT
    id,
    destination,
    event_type,
    message_key,
    NULL::bytea AS payload,
    content_type,
    headers,
    schema_version,
    correlation_id,
    causation_id,
    traceparent,
    tracestate,
    status,
    attempts,
    available_at,
    lease_owner,
    lease_until,
    last_error,
    created_at,
    delivered_at
FROM emitlane.outbox_events
WHERE status = 'dead'
ORDER BY created_at DESC
LIMIT $1`
	rows, err := s.pool.Query(ctx, sql, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead: %w", err)
	}
	defer rows.Close()
	var events []relay.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// Get returns one event. Payload is included; callers must not log it by default.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (relay.Event, error) {
	sql := `SELECT` + eventColumns + `
FROM emitlane.outbox_events WHERE id = $1`
	ev, err := scanEvent(s.pool.QueryRow(ctx, sql, id))
	if err != nil {
		return relay.Event{}, err
	}
	return ev, nil
}

// StatsSnapshot returns queue gauges.
func (s *Store) StatsSnapshot(ctx context.Context) (relay.Stats, error) {
	const sql = `
SELECT
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'inflight') AS inflight,
    COUNT(*) FILTER (WHERE status = 'dead') AS dead,
    COALESCE(
        EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'pending'))),
        0
    ) AS oldest_pending_seconds
FROM emitlane.outbox_events`
	var st relay.Stats
	if err := s.pool.QueryRow(ctx, sql).Scan(&st.Pending, &st.Inflight, &st.Dead, &st.OldestPendingSeconds); err != nil {
		return relay.Stats{}, fmt.Errorf("stats: %w", err)
	}
	return st, nil
}

// CleanupDelivered deletes a bounded batch of delivered events older than the
// retention duration, using PostgreSQL's clock. Dead events are never deleted.
func (s *Store) CleanupDelivered(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	if retention <= 0 || limit <= 0 {
		return 0, nil
	}
	const sql = `
WITH doomed AS (
    SELECT id
    FROM emitlane.outbox_events
    WHERE status = 'delivered'
      AND delivered_at IS NOT NULL
      AND delivered_at < NOW() - ($1 * INTERVAL '1 millisecond')
    ORDER BY delivered_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
DELETE FROM emitlane.outbox_events AS e
USING doomed
WHERE e.id = doomed.id`
	tag, err := s.pool.Exec(ctx, sql, intervalMS(retention), limit)
	if err != nil {
		return 0, fmt.Errorf("cleanup delivered: %w", err)
	}
	return tag.RowsAffected(), nil
}
