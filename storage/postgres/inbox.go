package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/inbox"
)

// InboxStore implements the durable managed Inbox lifecycle using PostgreSQL.
type InboxStore struct {
	pool *pgxpool.Pool
}

func NewInboxStore(pool *pgxpool.Pool) (*InboxStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres inbox store: pool is required")
	}
	return &InboxStore{pool: pool}, nil
}

func (s *InboxStore) Claim(ctx context.Context, req inbox.ClaimRequest) (inbox.ClaimResult, error) {
	if err := validateClaim(req); err != nil {
		return inbox.ClaimResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return inbox.ClaimResult{}, fmt.Errorf("inbox claim: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	const insert = `
INSERT INTO emitlane.inbox_events (
    consumer, event_id, processed_at, status, available_at,
    source_topic, source_partition, source_offset, source_timestamp
) VALUES ($1, $2, NULL, 'pending', NOW(), $3, $4, $5, $6)
ON CONFLICT DO NOTHING`
	var sourceTimestamp *time.Time
	if !req.Source.Timestamp.IsZero() {
		t := req.Source.Timestamp
		sourceTimestamp = &t
	}
	if _, err := tx.Exec(ctx, insert, req.Consumer, req.EventID, req.Source.Topic,
		req.Source.Partition, req.Source.Offset, sourceTimestamp); err != nil {
		return inbox.ClaimResult{}, fmt.Errorf("inbox claim: create lifecycle: %w", err)
	}

	var coordinateEvent uuid.UUID
	err = tx.QueryRow(ctx, `
SELECT event_id FROM emitlane.inbox_events
WHERE consumer=$1 AND source_topic=$2 AND source_partition=$3 AND source_offset=$4`,
		req.Consumer, req.Source.Topic, req.Source.Partition, req.Source.Offset).Scan(&coordinateEvent)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return inbox.ClaimResult{}, fmt.Errorf("inbox claim: inspect source: %w", err)
	}
	if err == nil && coordinateEvent != req.EventID {
		return inbox.ClaimResult{}, fmt.Errorf("%w: Kafka source %s[%d]@%d maps to event %s, not %s",
			inbox.ErrLifecycleConflict, req.Source.Topic, req.Source.Partition, req.Source.Offset,
			coordinateEvent, req.EventID)
	}

	token := uuid.New()
	const claim = `
UPDATE emitlane.inbox_events
SET status='inflight', attempts=attempts+1,
    lease_owner=$3, lease_token=$4,
    lease_until=NOW()+($5*INTERVAL '1 millisecond'),
    last_error=NULL, updated_at=NOW()
WHERE consumer=$1 AND event_id=$2
  AND (
       (status IN ('pending','retry_wait') AND available_at <= NOW())
       OR (status='inflight' AND lease_until <= NOW())
  )
RETURNING ` + inboxEventColumns
	event, err := scanInboxEvent(tx.QueryRow(ctx, claim, req.Consumer, req.EventID,
		req.LeaseOwner, token, intervalMS(req.LeaseDuration)))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return inbox.ClaimResult{}, fmt.Errorf("inbox claim: update: %w", err)
	}
	disposition := inbox.Claimed
	if errors.Is(err, pgx.ErrNoRows) {
		event, err = scanInboxEvent(tx.QueryRow(ctx, `SELECT `+inboxEventColumns+`
FROM emitlane.inbox_events WHERE consumer=$1 AND event_id=$2`, req.Consumer, req.EventID))
		if err != nil {
			return inbox.ClaimResult{}, fmt.Errorf("inbox claim: inspect lifecycle: %w", err)
		}
		switch event.Status {
		case inbox.StatusProcessed:
			disposition = inbox.AlreadyProcessed
		case inbox.StatusDead:
			disposition = inbox.ClaimDead
		default:
			disposition = inbox.ClaimWaiting
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return inbox.ClaimResult{}, fmt.Errorf("inbox claim: commit: %w", err)
	}
	return inbox.ClaimResult{Disposition: disposition, Event: event}, nil
}

func validateClaim(req inbox.ClaimRequest) error {
	if strings.TrimSpace(req.Consumer) == "" || req.EventID == uuid.Nil ||
		strings.TrimSpace(req.Source.Topic) == "" || req.Source.Partition < 0 || req.Source.Offset < 0 ||
		strings.TrimSpace(req.LeaseOwner) == "" || req.LeaseDuration <= 0 {
		return fmt.Errorf("%w: complete consumer, event, source, owner, and positive lease are required", inbox.ErrInvalidRequest)
	}
	return nil
}

func (s *InboxStore) Renew(ctx context.Context, consumer string, eventID, token uuid.UUID, lease time.Duration) error {
	if lease <= 0 {
		return fmt.Errorf("%w: positive lease duration is required", inbox.ErrInvalidRequest)
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE emitlane.inbox_events
SET lease_until=NOW()+($4*INTERVAL '1 millisecond'), updated_at=NOW()
WHERE consumer=$1 AND event_id=$2 AND status='inflight' AND lease_token=$3`,
		consumer, eventID, token, intervalMS(lease))
	if err != nil {
		return fmt.Errorf("inbox renew: %w", err)
	}
	return requireInboxLease(tag.RowsAffected())
}

func (s *InboxStore) MarkProcessed(ctx context.Context, tx pgx.Tx, consumer string, eventID, token uuid.UUID) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", inbox.ErrInvalidRequest)
	}
	tag, err := tx.Exec(ctx, `
UPDATE emitlane.inbox_events
SET status='processed', processed_at=NOW(), available_at=NOW(),
    lease_owner=NULL, lease_token=NULL, lease_until=NULL,
    last_error=NULL, updated_at=NOW()
WHERE consumer=$1 AND event_id=$2 AND status='inflight' AND lease_token=$3`,
		consumer, eventID, token)
	if err != nil {
		return fmt.Errorf("inbox mark processed: %w", err)
	}
	return requireInboxLease(tag.RowsAffected())
}

func (s *InboxStore) MarkRetry(ctx context.Context, consumer string, eventID, token uuid.UUID, delay time.Duration, lastError string) error {
	if delay < 0 {
		return fmt.Errorf("%w: retry delay must not be negative", inbox.ErrInvalidRequest)
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE emitlane.inbox_events
SET status='retry_wait', processed_at=NULL,
    available_at=NOW()+($4*INTERVAL '1 millisecond'),
    lease_owner=NULL, lease_token=NULL, lease_until=NULL,
    last_error=$5, updated_at=NOW()
WHERE consumer=$1 AND event_id=$2 AND status='inflight' AND lease_token=$3`,
		consumer, eventID, token, intervalMS(delay), sanitizeError(lastError))
	if err != nil {
		return fmt.Errorf("inbox mark retry: %w", err)
	}
	return requireInboxLease(tag.RowsAffected())
}

func (s *InboxStore) MarkDead(ctx context.Context, consumer string, eventID, token uuid.UUID, lastError string) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE emitlane.inbox_events
SET status='dead', processed_at=NULL, available_at=NOW(),
    lease_owner=NULL, lease_token=NULL, lease_until=NULL,
    last_error=$4, updated_at=NOW()
WHERE consumer=$1 AND event_id=$2 AND status='inflight' AND lease_token=$3`,
		consumer, eventID, token, sanitizeError(lastError))
	if err != nil {
		return fmt.Errorf("inbox mark dead: %w", err)
	}
	return requireInboxLease(tag.RowsAffected())
}

func (s *InboxStore) Get(ctx context.Context, consumer string, eventID uuid.UUID) (inbox.Event, error) {
	event, err := scanInboxEvent(s.pool.QueryRow(ctx, `SELECT `+inboxEventColumns+`
FROM emitlane.inbox_events WHERE consumer=$1 AND event_id=$2`, consumer, eventID))
	if errors.Is(err, pgx.ErrNoRows) {
		return inbox.Event{}, inbox.ErrNotFound
	}
	if err != nil {
		return inbox.Event{}, fmt.Errorf("inbox get: %w", err)
	}
	return event, nil
}

func requireInboxLease(rows int64) error {
	if rows != 1 {
		return inbox.ErrLeaseLost
	}
	return nil
}

const inboxEventColumns = `
consumer, event_id, status, attempts, available_at,
lease_owner, lease_token, lease_until, last_error,
first_seen_at, updated_at, processed_at,
source_topic, source_partition, source_offset, source_timestamp`

func scanInboxEvent(row rowScanner) (inbox.Event, error) {
	var event inbox.Event
	var owner, lastError, topic *string
	var token *uuid.UUID
	var leaseUntil, processedAt, sourceTimestamp *time.Time
	var partition *int32
	var offset *int64
	if err := row.Scan(&event.Consumer, &event.EventID, &event.Status, &event.Attempts, &event.AvailableAt,
		&owner, &token, &leaseUntil, &lastError, &event.FirstSeenAt, &event.UpdatedAt, &processedAt,
		&topic, &partition, &offset, &sourceTimestamp); err != nil {
		return inbox.Event{}, err
	}
	event.LeaseOwner = deref(owner)
	if token != nil {
		event.LeaseToken = *token
	}
	event.LeaseUntil = leaseUntil
	event.LastError = deref(lastError)
	event.ProcessedAt = processedAt
	if topic != nil && partition != nil && offset != nil {
		event.Source = &inbox.Source{Topic: *topic, Partition: *partition, Offset: *offset}
		if sourceTimestamp != nil {
			event.Source.Timestamp = *sourceTimestamp
		}
	}
	return event, nil
}

var _ inbox.Store = (*InboxStore)(nil)
