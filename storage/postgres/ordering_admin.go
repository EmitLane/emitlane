package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	adminapi "github.com/emitlane/emitlane/internal/admin"
	internalordering "github.com/emitlane/emitlane/internal/ordering"
)

const orderingStateSelect = `
SELECT
    stream.destination,
    stream.ordering_key,
    stream.partition_id,
    stream.start_sequence,
    stream.next_sequence,
    CASE
        WHEN next_event.status = 'dead' THEN 'dead_blocked'
        WHEN next_event.status = 'inflight' THEN 'inflight'
        WHEN next_event.status = 'pending' AND next_event.available_at > NOW() THEN 'retry_wait'
        WHEN next_event.id IS NOT NULL THEN 'ready'
        WHEN future.ordering_sequence IS NOT NULL THEN 'gap'
        ELSE 'ready'
    END AS state,
    next_event.id AS next_event_id,
    next_event.status AS next_event_status,
    COALESCE(next_event.attempts, 0) AS next_event_attempts,
    future.ordering_sequence AS lowest_future_sequence,
    CASE WHEN future.ordering_sequence IS NOT NULL
         THEN future.ordering_sequence - stream.next_sequence ELSE 0 END AS gap_size,
    CASE WHEN next_event.id IS NULL AND future.ordering_sequence IS NOT NULL
         THEN GREATEST(0, EXTRACT(EPOCH FROM (NOW() - future.created_at))) ELSE 0 END AS gap_age,
    stream.updated_at
FROM emitlane.ordering_streams AS stream
LEFT JOIN LATERAL (
    SELECT event.id, event.status, event.attempts, event.available_at
    FROM emitlane.outbox_events AS event
    WHERE event.destination = stream.destination
      AND event.ordering_key = stream.ordering_key
      AND event.ordering_sequence = stream.next_sequence
    LIMIT 1
) AS next_event ON TRUE
LEFT JOIN LATERAL (
    SELECT event.ordering_sequence, event.created_at
    FROM emitlane.outbox_events AS event
    WHERE event.destination = stream.destination
      AND event.ordering_key = stream.ordering_key
      AND event.ordering_sequence > stream.next_sequence
    ORDER BY event.ordering_sequence
    LIMIT 1
) AS future ON TRUE`

func scanOrderingStream(row rowScanner) (adminapi.OrderingStream, error) {
	var stream adminapi.OrderingStream
	var nextEventStatus *string
	if err := row.Scan(
		&stream.Destination,
		&stream.OrderingKey,
		&stream.Partition,
		&stream.StartSequence,
		&stream.NextSequence,
		&stream.State,
		&stream.NextEventID,
		&nextEventStatus,
		&stream.NextEventAttempts,
		&stream.LowestFutureSequence,
		&stream.GapSize,
		&stream.GapAgeSeconds,
		&stream.UpdatedAt,
	); err != nil {
		return adminapi.OrderingStream{}, err
	}
	stream.NextEventStatus = deref(nextEventStatus)
	return stream, nil
}

func (s *Store) ListOrderingStreams(ctx context.Context, filter adminapi.OrderingStreamFilter) (adminapi.OrderingStreamPage, error) {
	clauses := make([]string, 0, 5)
	args := make([]any, 0, 7)
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.State != "" {
		add("state = $%d", filter.State)
	}
	if filter.Destination != "" {
		add("destination = $%d", filter.Destination)
	}
	if filter.Partition != nil {
		add("partition_id = $%d", *filter.Partition)
	}
	if filter.BlockedOnly {
		clauses = append(clauses, "state IN ('retry_wait', 'gap', 'dead_blocked')")
	}
	if filter.Cursor != nil {
		args = append(args, filter.Cursor.Destination, filter.Cursor.OrderingKey)
		clauses = append(clauses, fmt.Sprintf("(destination, ordering_key) > ($%d, $%d)", len(args)-1, len(args)))
	}
	where := "TRUE"
	if len(clauses) > 0 {
		where = strings.Join(clauses, " AND ")
	}
	args = append(args, filter.Limit+1)
	query := `WITH stream_state AS (` + orderingStateSelect + `)
SELECT destination, ordering_key, partition_id, start_sequence, next_sequence,
       state, next_event_id, next_event_status, next_event_attempts,
       lowest_future_sequence, gap_size, gap_age, updated_at
FROM stream_state
WHERE ` + where + `
ORDER BY destination, ordering_key
LIMIT $` + fmt.Sprint(len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return adminapi.OrderingStreamPage{}, fmt.Errorf("list ordering streams: %w", err)
	}
	defer rows.Close()
	streams := make([]adminapi.OrderingStream, 0, filter.Limit+1)
	for rows.Next() {
		stream, err := scanOrderingStream(rows)
		if err != nil {
			return adminapi.OrderingStreamPage{}, fmt.Errorf("list ordering streams scan: %w", err)
		}
		streams = append(streams, stream)
	}
	if err := rows.Err(); err != nil {
		return adminapi.OrderingStreamPage{}, err
	}
	page := adminapi.OrderingStreamPage{Streams: streams}
	if len(streams) > filter.Limit {
		last := streams[filter.Limit-1]
		cursor, err := adminapi.EncodeOrderingStreamCursor(adminapi.OrderingStreamCursor{
			Destination: last.Destination, OrderingKey: last.OrderingKey,
		})
		if err != nil {
			return adminapi.OrderingStreamPage{}, err
		}
		page.Streams = streams[:filter.Limit]
		page.NextCursor = cursor
	}
	return page, nil
}

func (s *Store) InspectOrderingStream(ctx context.Context, destination, orderingKey string) (adminapi.OrderingStream, error) {
	query := `WITH stream_state AS (` + orderingStateSelect + `)
SELECT destination, ordering_key, partition_id, start_sequence, next_sequence,
       state, next_event_id, next_event_status, next_event_attempts,
       lowest_future_sequence, gap_size, gap_age, updated_at
FROM stream_state
WHERE destination=$1 AND ordering_key=$2`
	stream, err := scanOrderingStream(s.pool.QueryRow(ctx, query, destination, orderingKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return adminapi.OrderingStream{}, fmt.Errorf("%w: ordered stream %q / %q", adminapi.ErrNotFound, destination, orderingKey)
	}
	if err != nil {
		return adminapi.OrderingStream{}, fmt.Errorf("inspect ordering stream: %w", err)
	}
	return stream, nil
}

func (s *Store) ListOrderingPartitions(ctx context.Context, staleAfter time.Duration) ([]adminapi.OrderingPartition, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("list ordering partitions begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now, members, err := activeOrderingMembers(ctx, tx, staleAfter)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
SELECT partition_id, lease_owner, lease_until, epoch, handoff_not_before
FROM emitlane.ordering_partitions ORDER BY partition_id`)
	if err != nil {
		return nil, fmt.Errorf("list ordering partitions: %w", err)
	}
	defer rows.Close()
	partitions := make([]adminapi.OrderingPartition, 0, internalordering.PartitionCount)
	for rows.Next() {
		var partition adminapi.OrderingPartition
		var owner *string
		if err := rows.Scan(&partition.PartitionID, &owner, &partition.LeaseUntil,
			&partition.Epoch, &partition.HandoffNotBefore); err != nil {
			return nil, fmt.Errorf("list ordering partitions scan: %w", err)
		}
		partition.ActualOwner = deref(owner)
		partition.DesiredOwner = internalordering.DesiredOwner(partition.PartitionID, members)
		switch {
		case partition.ActualOwner == "":
			partition.State = "unowned"
		case partition.LeaseUntil == nil || !partition.LeaseUntil.After(now):
			partition.State = "expired"
		case partition.HandoffNotBefore != nil && partition.HandoffNotBefore.After(now):
			partition.State = "handoff"
		default:
			partition.State = "owned"
		}
		partitions = append(partitions, partition)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("list ordering partitions commit: %w", err)
	}
	return partitions, nil
}

type orderingStats struct {
	streams     int64
	blocked     int64
	gaps        int64
	deadBlocked int64
	owned       int64
	handoff     int64
	maxGapAge   float64
}

func (s *Store) readOrderingStats(ctx context.Context) (orderingStats, error) {
	query := `WITH stream_state AS (` + orderingStateSelect + `)
SELECT
    (SELECT COUNT(*) FROM stream_state),
    (SELECT COUNT(*) FROM stream_state WHERE state IN ('retry_wait', 'gap', 'dead_blocked')),
    (SELECT COUNT(*) FROM stream_state WHERE state='gap'),
    (SELECT COUNT(*) FROM stream_state WHERE state='dead_blocked'),
    (SELECT COUNT(*) FROM emitlane.ordering_partitions
     WHERE lease_owner IS NOT NULL AND lease_until > NOW()
       AND COALESCE(handoff_not_before, '-infinity'::timestamptz) <= NOW()),
    (SELECT COUNT(*) FROM emitlane.ordering_partitions
     WHERE lease_owner IS NOT NULL AND lease_until > NOW()
       AND handoff_not_before > NOW()),
    COALESCE((SELECT MAX(gap_age) FROM stream_state WHERE state='gap'), 0)`
	var stats orderingStats
	if err := s.pool.QueryRow(ctx, query).Scan(&stats.streams, &stats.blocked, &stats.gaps,
		&stats.deadBlocked, &stats.owned, &stats.handoff, &stats.maxGapAge); err != nil {
		return orderingStats{}, fmt.Errorf("ordering stats: %w", err)
	}
	return stats, nil
}
