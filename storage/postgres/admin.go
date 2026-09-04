package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/emitlane/emitlane/broker"
	adminapi "github.com/emitlane/emitlane/internal/admin"
)

const adminEventColumns = `
    id,
    destination,
    event_type,
    content_type,
    schema_version,
    correlation_id,
    causation_id,
    status,
    attempts,
    available_at,
    lease_owner,
    lease_until,
    last_error,
    created_at,
    delivered_at,
	octet_length(payload) AS payload_size,
    replayed_from_event_id,
    replay_batch_id,
    ordering_key,
    ordering_sequence,
    ordering_partition`

func scanAdminEvent(row rowScanner, includeSensitive bool) (adminapi.Event, error) {
	var (
		e                 adminapi.Event
		correlation       *string
		causation         *string
		owner             *string
		lastError         *string
		key               []byte
		payload           []byte
		headers           []byte
		orderingKey       *string
		orderingSequence  *int64
		orderingPartition *int16
	)
	dest := []any{
		&e.ID, &e.Destination, &e.Type, &e.ContentType, &e.SchemaVersion,
		&correlation, &causation, &e.Status, &e.Attempts, &e.AvailableAt,
		&owner, &e.LeaseUntil, &lastError, &e.CreatedAt, &e.DeliveredAt,
		&e.PayloadSize, &e.ReplayedFromEventID, &e.ReplayBatchID,
		&orderingKey, &orderingSequence, &orderingPartition,
	}
	if includeSensitive {
		dest = append(dest, &key, &payload, &headers)
	}
	if err := row.Scan(dest...); err != nil {
		return adminapi.Event{}, err
	}
	e.CorrelationID = deref(correlation)
	e.CausationID = deref(causation)
	e.LeaseOwner = deref(owner)
	e.LastError = deref(lastError)
	e.OrderingKey = deref(orderingKey)
	if orderingSequence != nil {
		e.OrderingSequence = *orderingSequence
	}
	e.OrderingPartition = orderingPartition
	if includeSensitive {
		keyBase64 := base64.StdEncoding.EncodeToString(key)
		payloadBase64 := base64.StdEncoding.EncodeToString(payload)
		e.KeyBase64 = &keyBase64
		e.PayloadBase64 = &payloadBase64
		var err error
		e.Headers, err = mapHeaders(headers)
		if err != nil {
			return adminapi.Event{}, err
		}
	}
	return e, nil
}

func normalizePageResult(events []adminapi.Event, limit int) (adminapi.EventPage, error) {
	page := adminapi.EventPage{Events: events}
	if len(events) <= limit {
		return page, nil
	}
	last := events[limit-1]
	cursor, err := adminapi.EncodeCursor(adminapi.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	if err != nil {
		return adminapi.EventPage{}, err
	}
	page.Events = events[:limit]
	page.NextCursor = cursor
	return page, nil
}

func eventWhere(filter adminapi.EventFilter, start int) (string, []any) {
	clauses := make([]string, 0, 6)
	args := make([]any, 0, 7)
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, start+len(args)-1))
	}
	if len(filter.Statuses) > 0 {
		add("status = ANY($%d::text[])", filter.Statuses)
	}
	if filter.Destination != "" {
		add("destination = $%d", filter.Destination)
	}
	if filter.EventType != "" {
		add("event_type = $%d", filter.EventType)
	}
	if filter.CreatedFrom != nil {
		add("created_at >= $%d", *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		add("created_at < $%d", *filter.CreatedTo)
	}
	if filter.ReplayBatchID != nil {
		add("replay_batch_id = $%d", *filter.ReplayBatchID)
	}
	if filter.Cursor != nil {
		args = append(args, filter.Cursor.CreatedAt, filter.Cursor.ID)
		clauses = append(clauses, fmt.Sprintf("(created_at, id) < ($%d, $%d)", start+len(args)-2, start+len(args)-1))
	}
	if len(clauses) == 0 {
		return "TRUE", args
	}
	return strings.Join(clauses, " AND "), args
}

func (s *Store) OperationalStats(ctx context.Context, staleAfter time.Duration) (adminapi.Stats, error) {
	const query = `
WITH queue AS (
	SELECT
		(SELECT COUNT(*) FROM emitlane.outbox_events WHERE status = 'pending') AS pending,
		(SELECT COUNT(*) FROM emitlane.outbox_events WHERE status = 'pending' AND available_at <= NOW()) AS pending_due,
		(SELECT COUNT(*) FROM emitlane.outbox_events WHERE status = 'pending' AND available_at > NOW()) AS pending_scheduled,
		(SELECT COUNT(*) FROM emitlane.outbox_events WHERE status = 'inflight') AS inflight,
		(SELECT COUNT(*) FROM emitlane.outbox_events WHERE status = 'delivered') AS delivered,
		(SELECT COUNT(*) FROM emitlane.outbox_events WHERE status = 'dead') AS dead,
		COALESCE((SELECT EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))
		          FROM emitlane.outbox_events WHERE status = 'pending'), 0) AS oldest
), relays AS (
    SELECT
        COUNT(*) FILTER (WHERE stopped_at IS NULL AND last_heartbeat_at >= NOW() - ($1 * INTERVAL '1 millisecond')) AS active,
        COUNT(*) FILTER (WHERE stopped_at IS NULL AND last_heartbeat_at < NOW() - ($1 * INTERVAL '1 millisecond')) AS stale,
        COUNT(*) FILTER (WHERE stopped_at IS NOT NULL) AS stopped
    FROM emitlane.relay_instances
)
SELECT queue.pending, queue.pending_due, queue.pending_scheduled, queue.inflight,
       queue.delivered, queue.dead, queue.oldest,
       control.paused, relays.active, relays.stale, relays.stopped
FROM queue, relays, emitlane.runtime_control AS control
WHERE control.singleton = TRUE`
	var st adminapi.Stats
	if err := s.pool.QueryRow(ctx, query, intervalMS(staleAfter)).Scan(
		&st.Pending, &st.PendingDue, &st.PendingScheduled, &st.Inflight,
		&st.DeliveredRetained, &st.Dead, &st.OldestPendingSeconds,
		&st.Paused, &st.ActiveRelays, &st.StaleRelays, &st.StoppedRelays,
	); err != nil {
		return adminapi.Stats{}, fmt.Errorf("operational stats: %w", err)
	}
	ordering, err := s.readOrderingStats(ctx)
	if err != nil {
		return adminapi.Stats{}, err
	}
	st.OrderedStreams = ordering.streams
	st.BlockedOrderedStreams = ordering.blocked
	st.GapStreams = ordering.gaps
	st.DeadBlockedStreams = ordering.deadBlocked
	st.OwnedPartitions = ordering.owned
	st.HandoffPartitions = ordering.handoff
	return st, nil
}

func (s *Store) ListEvents(ctx context.Context, filter adminapi.EventFilter) (adminapi.EventPage, error) {
	where, args := eventWhere(filter, 1)
	args = append(args, filter.Limit+1)
	query := `SELECT ` + adminEventColumns + `
FROM emitlane.outbox_events
WHERE ` + where + `
ORDER BY created_at DESC, id DESC
LIMIT $` + fmt.Sprint(len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return adminapi.EventPage{}, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	events := make([]adminapi.Event, 0, filter.Limit+1)
	for rows.Next() {
		event, err := scanAdminEvent(rows, false)
		if err != nil {
			return adminapi.EventPage{}, fmt.Errorf("list events scan: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return adminapi.EventPage{}, fmt.Errorf("list events rows: %w", err)
	}
	return normalizePageResult(events, filter.Limit)
}

func (s *Store) InspectEvent(ctx context.Context, id uuid.UUID, includeSensitive bool) (adminapi.Event, error) {
	query := `SELECT ` + adminEventColumns
	if includeSensitive {
		query += `, message_key, payload, headers`
	}
	query += ` FROM emitlane.outbox_events WHERE id = $1`
	event, err := scanAdminEvent(s.pool.QueryRow(ctx, query, id), includeSensitive)
	if errors.Is(err, pgx.ErrNoRows) {
		return adminapi.Event{}, fmt.Errorf("%w: event %s", adminapi.ErrNotFound, id)
	}
	if err != nil {
		return adminapi.Event{}, fmt.Errorf("inspect event: %w", err)
	}
	return event, nil
}

func (s *Store) ControlState(ctx context.Context) (adminapi.ControlState, error) {
	const query = `SELECT paused, COALESCE(reason, ''), updated_at, updated_by
FROM emitlane.runtime_control WHERE singleton = TRUE`
	var state adminapi.ControlState
	if err := s.pool.QueryRow(ctx, query).Scan(&state.Paused, &state.Reason, &state.UpdatedAt, &state.UpdatedBy); err != nil {
		return adminapi.ControlState{}, fmt.Errorf("control state: %w", err)
	}
	return state, nil
}

func (s *Store) SetPaused(ctx context.Context, paused bool, mutation adminapi.Mutation) (adminapi.ControlState, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return adminapi.ControlState{}, fmt.Errorf("set pause begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	const update = `
UPDATE emitlane.runtime_control
SET paused = $1, reason = NULLIF($2, ''), updated_at = NOW(), updated_by = $3
WHERE singleton = TRUE
RETURNING paused, COALESCE(reason, ''), updated_at, updated_by`
	var state adminapi.ControlState
	if err := tx.QueryRow(ctx, update, paused, mutation.Reason, mutation.Actor).Scan(
		&state.Paused, &state.Reason, &state.UpdatedAt, &state.UpdatedBy,
	); err != nil {
		return adminapi.ControlState{}, fmt.Errorf("set pause: %w", err)
	}
	action := "relay.resume"
	if paused {
		action = "relay.pause"
	}
	if err := insertAudit(ctx, tx, action, mutation, nil, nil, map[string]any{"paused": paused}); err != nil {
		return adminapi.ControlState{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify('emitlane_control', $1)`, action); err != nil {
		return adminapi.ControlState{}, fmt.Errorf("notify control: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return adminapi.ControlState{}, fmt.Errorf("set pause commit: %w", err)
	}
	return state, nil
}

func (s *Store) RetryDeadAudited(ctx context.Context, id uuid.UUID, mutation adminapi.Mutation) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("retry dead begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM emitlane.outbox_events WHERE id = $1 FOR UPDATE`, id).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: event %s", adminapi.ErrNotFound, id)
		}
		return fmt.Errorf("retry dead select: %w", err)
	}
	if status != "dead" {
		return fmt.Errorf("%w: event %s is %s, not dead", adminapi.ErrConflict, id, status)
	}
	if _, err := tx.Exec(ctx, `
UPDATE emitlane.outbox_events
SET status = 'pending', available_at = NOW(), attempts = 0,
    lease_owner = NULL, lease_until = NULL, last_error = NULL, delivered_at = NULL
WHERE id = $1`, id); err != nil {
		return fmt.Errorf("retry dead update: %w", err)
	}
	if err := insertAudit(ctx, tx, "event.retry", mutation, &id, nil, map[string]any{"from_status": "dead"}); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify('emitlane_events', $1)`, id.String()); err != nil {
		return fmt.Errorf("retry dead notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("retry dead commit: %w", err)
	}
	return nil
}

type replaySource struct {
	ID                uuid.UUID
	Destination       string
	EventType         string
	MessageKey        []byte
	Payload           []byte
	ContentType       string
	Headers           []byte
	SchemaVersion     int
	CorrelationID     *string
	CausationID       *string
	OrderingKey       *string
	OrderingSequence  *int64
	OrderingPartition *int16
}

const replaySourceColumns = `id, destination, event_type, message_key, payload, content_type,
headers, schema_version, correlation_id, causation_id, ordering_key, ordering_sequence, ordering_partition`

func scanReplaySource(row rowScanner) (replaySource, error) {
	var source replaySource
	err := row.Scan(&source.ID, &source.Destination, &source.EventType, &source.MessageKey,
		&source.Payload, &source.ContentType, &source.Headers, &source.SchemaVersion,
		&source.CorrelationID, &source.CausationID, &source.OrderingKey,
		&source.OrderingSequence, &source.OrderingPartition)
	return source, err
}

func newV7() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate UUIDv7: %w", err)
	}
	return id, nil
}

func cloneReplay(ctx context.Context, tx pgx.Tx, source replaySource, eventID, batchID uuid.UUID, mutation adminapi.Mutation) error {
	headers := source.Headers
	if source.OrderingKey != nil {
		if mutation.OrderingMode != "unordered" {
			return fmt.Errorf("%w: ordered event %s requires ordering_mode=unordered", adminapi.ErrConflict, source.ID)
		}
		values, err := mapHeaders(source.Headers)
		if err != nil {
			return fmt.Errorf("ordered replay headers %s: %w", source.ID, err)
		}
		values[broker.HeaderOriginalOrderingKey] = *source.OrderingKey
		if source.OrderingSequence != nil {
			values[broker.HeaderOriginalSequence] = fmt.Sprint(*source.OrderingSequence)
		}
		headers, err = json.Marshal(values)
		if err != nil {
			return fmt.Errorf("encode ordered replay provenance %s: %w", source.ID, err)
		}
	}
	const insert = `
INSERT INTO emitlane.outbox_events (
    id, destination, event_type, message_key, payload, content_type, headers,
    schema_version, correlation_id, causation_id, traceparent, tracestate,
    status, attempts, available_at, created_at, replayed_from_event_id, replay_batch_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, NULLIF($11, ''), NULLIF($12, ''),
    'pending', 0, NOW(), NOW(), $13, $14
)`
	_, err := tx.Exec(ctx, insert, eventID, source.Destination, source.EventType, source.MessageKey,
		source.Payload, source.ContentType, headers, source.SchemaVersion,
		source.CorrelationID, source.CausationID, mutation.Traceparent, mutation.Tracestate,
		source.ID, batchID)
	if err != nil {
		return fmt.Errorf("clone replay event %s: %w", source.ID, err)
	}
	return nil
}

func (s *Store) ReplayEvent(ctx context.Context, sourceID uuid.UUID, mutation adminapi.Mutation) (adminapi.ReplayResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay event begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	query := `SELECT ` + replaySourceColumns + ` FROM emitlane.outbox_events
WHERE id = $1 AND status IN ('delivered', 'dead') FOR SHARE`
	source, err := scanReplaySource(tx.QueryRow(ctx, query, sourceID))
	if errors.Is(err, pgx.ErrNoRows) {
		var status string
		statusErr := tx.QueryRow(ctx, `SELECT status FROM emitlane.outbox_events WHERE id = $1`, sourceID).Scan(&status)
		if errors.Is(statusErr, pgx.ErrNoRows) {
			return adminapi.ReplayResult{}, fmt.Errorf("%w: event %s", adminapi.ErrNotFound, sourceID)
		}
		if statusErr != nil {
			return adminapi.ReplayResult{}, fmt.Errorf("replay event status: %w", statusErr)
		}
		return adminapi.ReplayResult{}, fmt.Errorf("%w: event %s is %s", adminapi.ErrConflict, sourceID, status)
	}
	if err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay event select: %w", err)
	}
	eventID, err := newV7()
	if err != nil {
		return adminapi.ReplayResult{}, err
	}
	batchID, err := newV7()
	if err != nil {
		return adminapi.ReplayResult{}, err
	}
	if err := cloneReplay(ctx, tx, source, eventID, batchID, mutation); err != nil {
		return adminapi.ReplayResult{}, err
	}
	if err := insertAudit(ctx, tx, "event.replay", mutation, &sourceID, &batchID,
		map[string]any{"new_event_id": eventID.String(), "count": 1, "ordering_mode": mutation.OrderingMode}); err != nil {
		return adminapi.ReplayResult{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify('emitlane_events', $1)`, eventID.String()); err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay event commit: %w", err)
	}
	return adminapi.ReplayResult{ReplayBatchID: batchID, Created: 1, EventIDs: []uuid.UUID{eventID}}, nil
}

func (s *Store) PreviewReplay(ctx context.Context, filter adminapi.EventFilter, sampleLimit, executionLimit int) (adminapi.ReplayPreview, error) {
	where, args := eventWhere(filter, 1)
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE `+where, args...).Scan(&count); err != nil {
		return adminapi.ReplayPreview{}, fmt.Errorf("preview replay count: %w", err)
	}
	args = append(args, sampleLimit)
	query := `SELECT ` + adminEventColumns + ` FROM emitlane.outbox_events WHERE ` + where +
		` ORDER BY created_at DESC, id DESC LIMIT $` + fmt.Sprint(len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return adminapi.ReplayPreview{}, fmt.Errorf("preview replay sample: %w", err)
	}
	defer rows.Close()
	sample := make([]adminapi.Event, 0, min(count, sampleLimit))
	for rows.Next() {
		event, err := scanAdminEvent(rows, false)
		if err != nil {
			return adminapi.ReplayPreview{}, fmt.Errorf("preview replay scan: %w", err)
		}
		sample = append(sample, event)
	}
	if err := rows.Err(); err != nil {
		return adminapi.ReplayPreview{}, err
	}
	return adminapi.ReplayPreview{Count: count, Sample: sample, Limit: executionLimit}, nil
}

func (s *Store) ReplayBatch(ctx context.Context, filter adminapi.EventFilter, mutation adminapi.Mutation, limit int) (adminapi.ReplayResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay batch begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	where, args := eventWhere(filter, 1)
	args = append(args, limit+1)
	query := `SELECT ` + replaySourceColumns + ` FROM emitlane.outbox_events WHERE ` + where +
		` ORDER BY created_at DESC, id DESC LIMIT $` + fmt.Sprint(len(args)) + ` FOR SHARE`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay batch select: %w", err)
	}
	sources := make([]replaySource, 0, limit+1)
	for rows.Next() {
		source, err := scanReplaySource(rows)
		if err != nil {
			rows.Close()
			return adminapi.ReplayResult{}, fmt.Errorf("replay batch scan: %w", err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return adminapi.ReplayResult{}, err
	}
	rows.Close()
	if len(sources) == 0 {
		return adminapi.ReplayResult{}, fmt.Errorf("%w: replay selector matched no eligible events", adminapi.ErrNotFound)
	}
	if len(sources) > limit {
		return adminapi.ReplayResult{}, fmt.Errorf("%w: replay selector exceeds %d events", adminapi.ErrConflict, limit)
	}
	batchID, err := newV7()
	if err != nil {
		return adminapi.ReplayResult{}, err
	}
	eventIDs := make([]uuid.UUID, 0, len(sources))
	for _, source := range sources {
		eventID, err := newV7()
		if err != nil {
			return adminapi.ReplayResult{}, err
		}
		if err := cloneReplay(ctx, tx, source, eventID, batchID, mutation); err != nil {
			return adminapi.ReplayResult{}, err
		}
		eventIDs = append(eventIDs, eventID)
	}
	if err := insertAudit(ctx, tx, "replay.batch", mutation, nil, &batchID,
		map[string]any{"count": len(eventIDs), "statuses": filter.Statuses, "destination": filter.Destination,
			"event_type": filter.EventType, "ordering_mode": mutation.OrderingMode}); err != nil {
		return adminapi.ReplayResult{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify('emitlane_events', $1)`, batchID.String()); err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay batch notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return adminapi.ReplayResult{}, fmt.Errorf("replay batch commit: %w", err)
	}
	return adminapi.ReplayResult{ReplayBatchID: batchID, Created: len(eventIDs), EventIDs: eventIDs}, nil
}

func insertAudit(ctx context.Context, tx pgx.Tx, action string, mutation adminapi.Mutation, target, batch *uuid.UUID, details map[string]any) error {
	id, err := newV7()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("encode audit details: %w", err)
	}
	const insert = `
INSERT INTO emitlane.admin_audit_log
    (id, action, actor, reason, request_id, target_event_id, replay_batch_id, details)
VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, $8::jsonb)`
	if _, err := tx.Exec(ctx, insert, id, action, mutation.Actor, mutation.Reason, mutation.RequestID, target, batch, raw); err != nil {
		return fmt.Errorf("insert admin audit: %w", err)
	}
	return nil
}

func (s *Store) ListAudit(ctx context.Context, filter adminapi.AuditFilter) (adminapi.AuditPage, error) {
	clauses := []string{"TRUE"}
	args := make([]any, 0, 4)
	if filter.Action != "" {
		args = append(args, filter.Action)
		clauses = append(clauses, fmt.Sprintf("action = $%d", len(args)))
	}
	if filter.Cursor != nil {
		args = append(args, filter.Cursor.CreatedAt, filter.Cursor.ID)
		clauses = append(clauses, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, filter.Limit+1)
	query := `SELECT id, action, actor, COALESCE(reason, ''), COALESCE(request_id, ''),
target_event_id, replay_batch_id, details, created_at
FROM emitlane.admin_audit_log WHERE ` + strings.Join(clauses, " AND ") +
		` ORDER BY created_at DESC, id DESC LIMIT $` + fmt.Sprint(len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return adminapi.AuditPage{}, fmt.Errorf("list audit: %w", err)
	}
	defer rows.Close()
	records := make([]adminapi.AuditRecord, 0, filter.Limit+1)
	for rows.Next() {
		var record adminapi.AuditRecord
		var details []byte
		if err := rows.Scan(&record.ID, &record.Action, &record.Actor, &record.Reason, &record.RequestID,
			&record.TargetEventID, &record.ReplayBatchID, &details, &record.CreatedAt); err != nil {
			return adminapi.AuditPage{}, fmt.Errorf("list audit scan: %w", err)
		}
		if err := json.Unmarshal(details, &record.Details); err != nil {
			return adminapi.AuditPage{}, fmt.Errorf("decode audit details: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return adminapi.AuditPage{}, err
	}
	page := adminapi.AuditPage{Records: records}
	if len(records) > filter.Limit {
		last := records[filter.Limit-1]
		cursor, err := adminapi.EncodeCursor(adminapi.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if err != nil {
			return adminapi.AuditPage{}, err
		}
		page.Records = records[:filter.Limit]
		page.NextCursor = cursor
	}
	return page, nil
}
