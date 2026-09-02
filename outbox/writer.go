package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
	"github.com/emitlane/emitlane/telemetry"
)

const notifyChannel = "emitlane_events"

// Writer inserts outbox events into PostgreSQL using the caller's transaction.
type Writer struct {
	notify  bool
	metrics *telemetry.Metrics
	idFn    func() (string, error)
}

// Option configures a Writer.
type Option func(*Writer)

// WithMetrics records successful INSERT calls in this process. Because the
// caller owns the transaction, the counter cannot know whether it later commits
// or rolls back.
func WithMetrics(m *telemetry.Metrics) Option {
	return func(w *Writer) {
		w.metrics = m
	}
}

// WithoutNotify disables pg_notify after insert. Polling remains the source
// of truth; this is intended for tests of the poll fallback.
func WithoutNotify() Option {
	return func(w *Writer) {
		w.notify = false
	}
}

// NewWriter returns a Writer that notifies relay listeners after enqueue.
func NewWriter(opts ...Option) *Writer {
	w := &Writer{
		notify: true,
		idFn:   newEventID,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	return w
}

// Enqueue validates and inserts event in tx, then optionally emits a
// PostgreSQL notification. It does not commit or roll back tx.
//
// An empty event ID is replaced with a UUIDv7 generated before INSERT.
// Empty AvailableAt means the event is immediately available.
// Empty Payload is allowed.
func (w *Writer) Enqueue(ctx context.Context, tx pgx.Tx, event Event) (string, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "emitlane.enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()
	if tx == nil {
		err := fmt.Errorf("%w: transaction is required", ErrInvalidEvent)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	if err := event.validate(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	var orderingPartition any
	var orderingSequence any
	if event.OrderingKey != "" {
		partition := internalordering.Partition(event.Destination, event.OrderingKey)
		start := event.OrderingStartSequence
		if start == 0 {
			start = 1
		}
		if err := initializeOrderingStream(ctx, tx, event, partition, start); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return "", err
		}
		if len(event.Key) == 0 {
			event.Key = []byte(event.OrderingKey)
		}
		orderingPartition = partition
		orderingSequence = event.Sequence
		span.SetAttributes(
			attribute.String("emitlane.ordering_key", event.OrderingKey),
			attribute.Int64("emitlane.ordering_sequence", event.Sequence),
			attribute.Int("emitlane.ordering_partition", int(partition)),
		)
	}

	id := event.ID
	if id == "" {
		generated, err := w.idFn()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return "", err
		}
		id = generated
	} else {
		parsed, _ := uuid.Parse(id) // validation already established that this succeeds.
		id = parsed.String()
	}

	contentType := event.ContentType
	if contentType == "" {
		contentType = DefaultContentType
	}
	schemaVersion := event.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = DefaultSchemaVersion
	}

	headers := event.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	headerJSON, err := json.Marshal(headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("outbox: encode headers: %w", err)
	}

	traceparent, tracestate := telemetry.InjectTrace(ctx)

	payload := event.Payload
	if payload == nil {
		payload = []byte{}
	}

	var availableAt any
	if !event.AvailableAt.IsZero() {
		availableAt = event.AvailableAt
	}

	span.SetAttributes(
		attribute.String("messaging.destination.name", event.Destination),
		attribute.String("emitlane.event_id", id),
		attribute.String("emitlane.event_type", event.Type),
	)

	const insertSQL = `
INSERT INTO emitlane.outbox_events (
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
    available_at,
    ordering_key,
    ordering_sequence,
    ordering_partition
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($13, NOW()),
    $14, $15, $16
)`

	if _, err := tx.Exec(ctx, insertSQL,
		id,
		event.Destination,
		event.Type,
		event.Key,
		payload,
		contentType,
		headerJSON,
		schemaVersion,
		nullString(event.CorrelationID),
		nullString(event.CausationID),
		nullString(traceparent),
		nullString(tracestate),
		availableAt,
		nullString(event.OrderingKey),
		orderingSequence,
		orderingPartition,
	); err != nil {
		if duplicateOrderingSequence(err) {
			err = fmt.Errorf("%w: destination %q key %q sequence %d", ErrDuplicateSequence,
				event.Destination, event.OrderingKey, event.Sequence)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("outbox: insert event %s: %w", id, err)
	}

	if w.notify {
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannel, id); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return "", fmt.Errorf("outbox: notify after insert %s: %w", id, err)
		}
	}

	w.metrics.IncEnqueued()
	return id, nil
}

func initializeOrderingStream(ctx context.Context, tx pgx.Tx, event Event, partition int16, start int64) error {
	const initializeSQL = `
INSERT INTO emitlane.ordering_streams (
    destination, ordering_key, partition_id, start_sequence, next_sequence
) VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (destination, ordering_key) DO NOTHING`
	if _, err := tx.Exec(ctx, initializeSQL, event.Destination, event.OrderingKey, partition, start); err != nil {
		return fmt.Errorf("outbox: initialize ordering stream: %w", err)
	}

	const inspectSQL = `
SELECT partition_id, start_sequence, next_sequence
FROM emitlane.ordering_streams
WHERE destination = $1 AND ordering_key = $2
FOR UPDATE`
	var storedPartition int16
	var storedStart, next int64
	if err := tx.QueryRow(ctx, inspectSQL, event.Destination, event.OrderingKey).Scan(&storedPartition, &storedStart, &next); err != nil {
		return fmt.Errorf("outbox: inspect ordering stream: %w", err)
	}
	if storedPartition != partition {
		return fmt.Errorf("%w: destination %q key %q partition is %d, calculated %d",
			ErrOrderingConflict, event.Destination, event.OrderingKey, storedPartition, partition)
	}
	if event.OrderingStartSequence > 0 && storedStart != event.OrderingStartSequence {
		return fmt.Errorf("%w: destination %q key %q starts at %d, requested %d",
			ErrOrderingConflict, event.Destination, event.OrderingKey, storedStart, event.OrderingStartSequence)
	}
	if event.Sequence < next {
		return fmt.Errorf("%w: destination %q key %q expects %d, received %d",
			ErrSequenceAlreadyPassed, event.Destination, event.OrderingKey, next, event.Sequence)
	}
	return nil
}

func duplicateOrderingSequence(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == "outbox_ordering_sequence_unique_idx"
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
