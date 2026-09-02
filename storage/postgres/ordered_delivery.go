package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/relay"
)

// ClaimOrdered leases only the expected sequence of independently eligible
// streams whose authoritative partition lease is valid for the caller.
func (s *Store) ClaimOrdered(
	ctx context.Context,
	owner string,
	limit int,
	eventLease time.Duration,
	minimumPartitionLease time.Duration,
) ([]relay.Event, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim ordered begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const query = `
WITH picked AS (
    SELECT e.id, partition.epoch
    FROM emitlane.outbox_events AS e
    JOIN emitlane.ordering_streams AS stream
      ON stream.destination = e.destination
     AND stream.ordering_key = e.ordering_key
     AND stream.next_sequence = e.ordering_sequence
    JOIN emitlane.ordering_partitions AS partition
      ON partition.partition_id = e.ordering_partition
    WHERE e.ordering_key IS NOT NULL
      AND e.available_at <= NOW()
      AND (
            e.status = 'pending'
         OR (e.status = 'inflight' AND e.lease_until IS NOT NULL AND e.lease_until <= NOW())
      )
      AND partition.lease_owner = $2
      AND partition.lease_until >= NOW() + ($4 * INTERVAL '1 millisecond')
      AND COALESCE(partition.handoff_not_before, '-infinity'::timestamptz) <= NOW()
      AND EXISTS (
          SELECT 1 FROM emitlane.runtime_control
          WHERE singleton = TRUE AND paused = FALSE
      )
    ORDER BY e.available_at, e.created_at, e.id
    FOR UPDATE OF e SKIP LOCKED
    LIMIT $1
)
UPDATE emitlane.outbox_events AS e
SET status = 'inflight',
    lease_owner = $2,
    lease_until = NOW() + ($3 * INTERVAL '1 millisecond')
FROM picked
WHERE e.id = picked.id
RETURNING
    e.id, e.destination, e.event_type, e.message_key, e.payload,
    e.content_type, e.headers, e.schema_version, e.correlation_id,
    e.causation_id, e.traceparent, e.tracestate, e.status, e.attempts,
    e.available_at, e.lease_owner, e.lease_until, e.last_error,
    e.created_at, e.delivered_at, e.replayed_from_event_id,
    e.replay_batch_id, e.ordering_key, e.ordering_sequence,
    e.ordering_partition, picked.epoch`
	rows, err := tx.Query(ctx, query, limit, owner, intervalMS(eventLease), intervalMS(minimumPartitionLease))
	if err != nil {
		return nil, fmt.Errorf("claim ordered query: %w", err)
	}
	defer rows.Close()
	var events []relay.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("claim ordered scan: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim ordered rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim ordered commit: %w", err)
	}
	return events, nil
}

// BeginOrderedAttempt is the final database fence before broker I/O. It also
// requires enough partition lease to cover the configured publish bound and
// safety margin supplied by the Relay.
func (s *Store) BeginOrderedAttempt(
	ctx context.Context,
	id uuid.UUID,
	owner string,
	epoch int64,
	maxAttempts int,
	minimumPartitionLease time.Duration,
) (int, error) {
	const query = `
UPDATE emitlane.outbox_events AS event
SET attempts = attempts + 1
FROM emitlane.ordering_partitions AS partition
WHERE event.id = $1
  AND event.ordering_partition = partition.partition_id
  AND event.lease_owner = $2
  AND event.status = 'inflight'
  AND event.lease_until > NOW()
  AND event.attempts < $4
  AND partition.lease_owner = $2
  AND partition.epoch = $3
  AND partition.lease_until >= NOW() + ($5 * INTERVAL '1 millisecond')
  AND COALESCE(partition.handoff_not_before, '-infinity'::timestamptz) <= NOW()
RETURNING event.attempts`
	var attempt int
	if err := s.pool.QueryRow(ctx, query, id, owner, epoch, maxAttempts,
		intervalMS(minimumPartitionLease)).Scan(&attempt); err != nil {
		return 0, fmt.Errorf("begin ordered publish attempt %s: %w", id, err)
	}
	return attempt, nil
}

// MarkOrderedDelivered atomically advances stream progress and marks the
// acknowledged event delivered under the same event/partition fence.
func (s *Store) MarkOrderedDelivered(ctx context.Context, event relay.Event, owner string) error {
	const query = `
WITH authority AS (
    SELECT event.id, event.destination, event.ordering_key,
           event.ordering_sequence, event.ordering_partition
    FROM emitlane.outbox_events AS event
    JOIN emitlane.ordering_partitions AS partition
      ON partition.partition_id = event.ordering_partition
    WHERE event.id = $1
      AND event.status = 'inflight'
      AND event.lease_owner = $2
      AND event.lease_until > NOW()
      AND event.ordering_sequence = $4
      AND partition.lease_owner = $2
      AND partition.epoch = $3
      AND partition.lease_until > NOW()
      AND COALESCE(partition.handoff_not_before, '-infinity'::timestamptz) <= NOW()
    FOR UPDATE OF event
), advanced AS (
    UPDATE emitlane.ordering_streams AS stream
    SET next_sequence = authority.ordering_sequence + 1,
        updated_at = NOW()
    FROM authority
    WHERE stream.destination = authority.destination
      AND stream.ordering_key = authority.ordering_key
      AND stream.partition_id = authority.ordering_partition
      AND stream.next_sequence = authority.ordering_sequence
    RETURNING authority.id
)
UPDATE emitlane.outbox_events AS event
SET status = 'delivered', delivered_at = NOW(),
    lease_owner = NULL, lease_until = NULL, last_error = NULL
FROM advanced
WHERE event.id = advanced.id`
	tag, err := s.pool.Exec(ctx, query, event.ID, owner, event.OrderingEpoch, event.OrderingSequence)
	if err != nil {
		return fmt.Errorf("mark ordered delivered %s: %w", event.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark ordered delivered %s: stale owner, epoch, lease, or sequence", event.ID)
	}
	return nil
}

func (s *Store) MarkOrderedRetry(ctx context.Context, event relay.Event, owner string, delay time.Duration, lastError string) error {
	const query = `
UPDATE emitlane.outbox_events AS event
SET status='pending',
    available_at=NOW() + ($5 * INTERVAL '1 millisecond'),
    lease_owner=NULL, lease_until=NULL, last_error=$6
FROM emitlane.ordering_partitions AS partition
WHERE event.id=$1
  AND event.ordering_partition=partition.partition_id
  AND event.lease_owner=$2 AND event.status='inflight'
  AND partition.lease_owner=$2 AND partition.epoch=$3
  AND partition.lease_until > NOW()
  AND event.ordering_sequence=$4
  AND COALESCE(partition.handoff_not_before, '-infinity'::timestamptz) <= NOW()`
	tag, err := s.pool.Exec(ctx, query, event.ID, owner, event.OrderingEpoch,
		event.OrderingSequence, intervalMS(delay), sanitizeError(lastError))
	if err != nil {
		return fmt.Errorf("mark ordered retry %s: %w", event.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark ordered retry %s: stale owner, epoch, lease, or sequence", event.ID)
	}
	return nil
}

func (s *Store) MarkOrderedDead(ctx context.Context, event relay.Event, owner string, lastError string) error {
	const query = `
UPDATE emitlane.outbox_events AS event
SET status='dead', lease_owner=NULL, lease_until=NULL, last_error=$5
FROM emitlane.ordering_partitions AS partition
WHERE event.id=$1
  AND event.ordering_partition=partition.partition_id
  AND event.lease_owner=$2 AND event.status='inflight'
  AND partition.lease_owner=$2 AND partition.epoch=$3
  AND partition.lease_until > NOW()
  AND event.ordering_sequence=$4
  AND COALESCE(partition.handoff_not_before, '-infinity'::timestamptz) <= NOW()`
	tag, err := s.pool.Exec(ctx, query, event.ID, owner, event.OrderingEpoch,
		event.OrderingSequence, sanitizeError(lastError))
	if err != nil {
		return fmt.Errorf("mark ordered dead %s: %w", event.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark ordered dead %s: stale owner, epoch, lease, or sequence", event.ID)
	}
	return nil
}
