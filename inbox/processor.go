package inbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/emitlane/emitlane/telemetry"
)

const insertSQL = `
INSERT INTO emitlane.inbox_events (consumer, event_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
`

// Process records (consumer, eventID) in tx and runs fn only for a new pair.
// Duplicate delivery for the same consumer returns nil and does not call fn.
// Different consumers may process the same event independently.
//
// Duplicate protection is the table primary key; it is not an in-memory lock.
// If fn returns an error, Process rolls marker and callback database writes
// back to an internal savepoint. The caller should still treat the transaction
// as failed. External side effects inside fn need their own idempotency.
func Process(ctx context.Context, tx pgx.Tx, consumer, eventID string, fn func(context.Context, pgx.Tx) error) error {
	_, err := process(ctx, tx, consumer, eventID, fn, false)
	return err
}

// ProcessStrict is Process, but returns ErrAlreadyProcessed on duplicates.
func ProcessStrict(ctx context.Context, tx pgx.Tx, consumer, eventID string, fn func(context.Context, pgx.Tx) error) error {
	_, err := process(ctx, tx, consumer, eventID, fn, true)
	return err
}

func process(ctx context.Context, tx pgx.Tx, consumer, eventID string, fn func(context.Context, pgx.Tx) error, strictDuplicate bool) (already bool, err error) {
	ctx, span := telemetry.Tracer().Start(ctx, "emitlane.inbox.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	consumer = strings.TrimSpace(consumer)
	eventID = strings.TrimSpace(eventID)
	if tx == nil {
		err := fmt.Errorf("%w: transaction is required", ErrInvalidRequest)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, err
	}
	if fn == nil {
		err := fmt.Errorf("%w: callback is required", ErrInvalidRequest)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, err
	}
	if consumer == "" {
		err := fmt.Errorf("%w: consumer is required", ErrInvalidRequest)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, err
	}
	parsed, err := uuid.Parse(eventID)
	if err != nil {
		err = fmt.Errorf("%w: event id must be a UUID: %v", ErrInvalidRequest, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, err
	}

	span.SetAttributes(
		attribute.String("emitlane.consumer", consumer),
		attribute.String("emitlane.event_id", parsed.String()),
	)

	nested, err := tx.Begin(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("inbox: create savepoint: %w", err)
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if rollbackErr := nested.Rollback(cleanupCtx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			rollbackErr = fmt.Errorf("inbox: rollback savepoint: %w", rollbackErr)
			if err == nil {
				err = rollbackErr
			} else {
				err = errors.Join(err, rollbackErr)
			}
		}
	}()

	tag, err := nested.Exec(ctx, insertSQL, consumer, parsed)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("inbox: insert marker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		span.SetAttributes(attribute.Bool("emitlane.already_processed", true))
		if err := nested.Commit(ctx); err != nil {
			return true, fmt.Errorf("inbox: release duplicate savepoint: %w", err)
		}
		finished = true
		if strictDuplicate {
			return true, ErrAlreadyProcessed
		}
		return true, nil
	}
	if err := fn(ctx, nested); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("inbox: callback: %w", err)
	}
	if err := nested.Commit(ctx); err != nil {
		return false, fmt.Errorf("inbox: release savepoint: %w", err)
	}
	finished = true
	return false, nil
}
