package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

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
    available_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($13, NOW())
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
	); err != nil {
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

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
