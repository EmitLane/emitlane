package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/inbox"
	adminapi "github.com/emitlane/emitlane/internal/admin"
)

func (s *Store) InboxStats(ctx context.Context, consumer string) (adminapi.InboxStats, error) {
	const query = `
SELECT
    COUNT(*) FILTER (WHERE status='processed'),
    COUNT(*) FILTER (WHERE status='pending'),
    COUNT(*) FILTER (WHERE status='inflight'),
    COUNT(*) FILTER (WHERE status='retry_wait'),
    COUNT(*) FILTER (WHERE status='dead'),
    COUNT(*) FILTER (WHERE status='inflight' AND lease_until <= NOW()),
    COUNT(*) FILTER (WHERE status='retry_wait' AND available_at <= NOW()),
    COUNT(DISTINCT (source_topic, source_partition)) FILTER (
        WHERE status='dead' AND source_topic IS NOT NULL
    )
FROM emitlane.inbox_events
WHERE ($1='' OR consumer=$1)`
	var stats adminapi.InboxStats
	if err := s.pool.QueryRow(ctx, query, consumer).Scan(
		&stats.Processed, &stats.Pending, &stats.Inflight, &stats.RetryWait,
		&stats.Dead, &stats.StaleInflight, &stats.DueRetries, &stats.BlockedPartitions,
	); err != nil {
		return adminapi.InboxStats{}, fmt.Errorf("Inbox stats: %w", err)
	}
	return stats, nil
}

func (s *Store) ListDeadInbox(ctx context.Context, filter adminapi.InboxDeadFilter) ([]adminapi.InboxEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+inboxEventColumns+`
FROM emitlane.inbox_events
WHERE status='dead' AND ($1='' OR consumer=$1)
ORDER BY updated_at DESC, event_id
LIMIT $2 OFFSET $3`, filter.Consumer, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("list dead Inbox events: %w", err)
	}
	defer rows.Close()
	events := make([]adminapi.InboxEvent, 0, filter.Limit)
	for rows.Next() {
		event, err := scanInboxEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("list dead Inbox events: scan: %w", err)
		}
		events = append(events, adminInboxEvent(event))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list dead Inbox events: rows: %w", err)
	}
	return events, nil
}

func (s *Store) InspectInbox(ctx context.Context, consumer string, eventID uuid.UUID) (adminapi.InboxEvent, error) {
	store, _ := NewInboxStore(s.pool)
	event, err := store.Get(ctx, consumer, eventID)
	if errors.Is(err, inbox.ErrNotFound) {
		return adminapi.InboxEvent{}, fmt.Errorf("%w: Inbox event %s/%s", adminapi.ErrNotFound, consumer, eventID)
	}
	if err != nil {
		return adminapi.InboxEvent{}, err
	}
	return adminInboxEvent(event), nil
}

func (s *Store) RetryDeadInbox(ctx context.Context, consumer string, eventID uuid.UUID, mutation adminapi.Mutation) error {
	store, _ := NewInboxStore(s.pool)
	err := store.RetryDead(ctx, inbox.RetryRequest{
		Consumer: consumer, EventID: eventID, Actor: mutation.Actor,
		Reason: mutation.Reason, RequestID: mutation.RequestID,
	})
	if errors.Is(err, inbox.ErrLifecycleConflict) {
		return fmt.Errorf("%w: Inbox event %s/%s is not dead", adminapi.ErrConflict, consumer, eventID)
	}
	if errors.Is(err, inbox.ErrNotFound) {
		return fmt.Errorf("%w: Inbox event %s/%s", adminapi.ErrNotFound, consumer, eventID)
	}
	return err
}

func adminInboxEvent(event inbox.Event) adminapi.InboxEvent {
	result := adminapi.InboxEvent{
		Consumer: event.Consumer, EventID: event.EventID, Status: string(event.Status),
		Attempts: event.Attempts, AvailableAt: event.AvailableAt, LeaseOwner: event.LeaseOwner,
		LeaseUntil: event.LeaseUntil, LastError: strings.TrimSpace(event.LastError),
		FirstSeenAt: event.FirstSeenAt, UpdatedAt: event.UpdatedAt, ProcessedAt: event.ProcessedAt,
	}
	if event.Source != nil {
		partition, offset := event.Source.Partition, event.Source.Offset
		result.SourceTopic = event.Source.Topic
		result.SourcePartition = &partition
		result.SourceOffset = &offset
		if !event.Source.Timestamp.IsZero() {
			timestamp := event.Source.Timestamp
			result.SourceTimestamp = &timestamp
		}
	}
	return result
}
