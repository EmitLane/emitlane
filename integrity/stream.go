package integrity

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
)

const MaxStreamEvents = 200

var ErrStreamNotFound = errors.New("integrity: ordered stream not found")

// InspectStream returns a bounded targeted diagnosis from one read-only,
// repeatable-read snapshot. It never selects payloads or headers.
func (v *Verifier) InspectStream(ctx context.Context, destination, orderingKey string) (StreamInspection, error) {
	destination = strings.TrimSpace(destination)
	orderingKey = strings.TrimSpace(orderingKey)
	if destination == "" || orderingKey == "" || !safeDiagnosticText(destination) || !safeDiagnosticText(orderingKey) {
		return StreamInspection{}, fmt.Errorf("integrity: destination and ordering key are required and must be valid UTF-8 without NUL")
	}
	startedAt := time.Now().UTC()
	acc := newAccumulator(ModeStream, v.config.MaxFindings, startedAt)
	tx, err := v.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return StreamInspection{}, fmt.Errorf("integrity: begin stream snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	timeoutValue := strconv.FormatInt(int64((v.config.StatementTimeout+time.Millisecond-1)/time.Millisecond), 10)
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout', $1, true)`, timeoutValue); err != nil {
		return StreamInspection{}, fmt.Errorf("integrity: set stream statement timeout: %w", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&databaseNow); err != nil {
		return StreamInspection{}, fmt.Errorf("integrity: read stream snapshot time: %w", err)
	}
	runtimeReady, err := checkSchema(ctx, tx, acc)
	if err != nil {
		return StreamInspection{}, err
	}
	inspection := StreamInspection{
		Destination: destination, OrderingKey: orderingKey,
		ExpectedPartition: internalordering.Partition(destination, orderingKey),
		Events:            []StreamEvent{},
	}
	if !runtimeReady {
		if err := tx.Commit(ctx); err != nil {
			return StreamInspection{}, fmt.Errorf("integrity: finish stream snapshot: %w", err)
		}
		inspection.Report = acc.finish(time.Now().UTC())
		return inspection, nil
	}

	var stream StreamCursor
	if err := tx.QueryRow(ctx, `
SELECT destination, ordering_key, partition_id, start_sequence, next_sequence, created_at, updated_at
FROM emitlane.ordering_streams
WHERE destination=$1 AND ordering_key=$2`, destination, orderingKey).Scan(
		&stream.Destination, &stream.OrderingKey, &stream.PartitionID, &stream.StartSequence,
		&stream.NextSequence, &stream.CreatedAt, &stream.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return StreamInspection{}, fmt.Errorf("%w: destination %q", ErrStreamNotFound, destination)
		}
		return StreamInspection{}, fmt.Errorf("integrity: inspect stream cursor: %w", err)
	}
	inspection.Stream = &stream
	acc.report.Summary.OrderedStreams = 1
	partitionID := stream.PartitionID
	if stream.PartitionID != inspection.ExpectedPartition {
		acc.add(Finding{Code: CodeStreamPartitionMismatch, Severity: SeverityViolation,
			Message: "stream partition differs from the deterministic mapping", Destination: destination,
			OrderingKey: orderingKey, PartitionID: &partitionID, Expected: inspection.ExpectedPartition, Observed: stream.PartitionID})
	}
	if stream.StartSequence <= 0 || stream.NextSequence < stream.StartSequence {
		acc.add(Finding{Code: CodeStreamCursorInvalid, Severity: SeverityViolation,
			Message: "stream cursor is outside its valid range", Destination: destination, OrderingKey: orderingKey,
			Observed: map[string]any{"start_sequence": stream.StartSequence, "next_sequence": stream.NextSequence}})
	}

	var observation CursorObservation
	observation.StartSequence, observation.NextSequence = stream.StartSequence, stream.NextSequence
	var expectedStatus *string
	if err := tx.QueryRow(ctx, `
SELECT COUNT(*) FILTER (WHERE ordering_sequence=$3),
       MIN(status) FILTER (WHERE ordering_sequence=$3),
       MIN(available_at) FILTER (WHERE ordering_sequence=$3),
       MIN(lease_until) FILTER (WHERE ordering_sequence=$3),
       MIN(ordering_sequence) FILTER (WHERE ordering_sequence>$3)
FROM emitlane.outbox_events
WHERE destination=$1 AND ordering_key=$2 AND status IN ('pending','inflight','dead')`,
		destination, orderingKey, stream.NextSequence).Scan(
		&observation.ExpectedCount, &expectedStatus, &observation.ExpectedAvailableAt,
		&observation.ExpectedLeaseUntil, &observation.LowestFutureSequence); err != nil {
		return StreamInspection{}, fmt.Errorf("integrity: classify stream cursor: %w", err)
	}
	if expectedStatus != nil {
		observation.ExpectedStatus = *expectedStatus
	}
	inspection.BlockingCondition = ClassifyCursor(databaseNow, observation)
	addStreamStateFinding(acc, inspection, observation)

	rows, err := tx.Query(ctx, `
SELECT id::text, ordering_sequence, ordering_partition, status, attempts,
       available_at, lease_owner, lease_until, created_at
FROM emitlane.outbox_events
WHERE destination=$1 AND ordering_key=$2
  AND (status IN ('pending','inflight','dead') OR ordering_sequence >= $3 - 5)
ORDER BY ordering_sequence, id`, destination, orderingKey, stream.NextSequence)
	if err != nil {
		return StreamInspection{}, fmt.Errorf("integrity: list retained stream events: %w", err)
	}
	for rows.Next() {
		var event StreamEvent
		var leaseOwner *string
		if err := rows.Scan(&event.ID, &event.Sequence, &event.PartitionID, &event.Status, &event.Attempts,
			&event.AvailableAt, &leaseOwner, &event.LeaseUntil, &event.CreatedAt); err != nil {
			rows.Close()
			return StreamInspection{}, fmt.Errorf("integrity: scan retained stream event: %w", err)
		}
		if leaseOwner != nil {
			event.LeaseOwner = *leaseOwner
		}
		if len(inspection.Events) == MaxStreamEvents {
			inspection.EventsTruncated = true
			continue
		}
		inspection.Events = append(inspection.Events, event)
		if event.PartitionID != inspection.ExpectedPartition {
			eventPartition := event.PartitionID
			acc.add(Finding{Code: CodeOrderingPartitionMismatch, Severity: SeverityViolation,
				Message: "ordered event partition differs from the deterministic mapping", Destination: destination,
				OrderingKey: orderingKey, EventID: event.ID, PartitionID: &eventPartition,
				Expected: inspection.ExpectedPartition, Observed: event.PartitionID})
		}
		active := event.Status == "pending" || event.Status == "inflight" || event.Status == "dead"
		if active && event.Sequence < stream.NextSequence {
			acc.add(Finding{Code: CodeActiveSequenceBehindCursor, Severity: SeverityViolation,
				Message: "active ordered event is below the durable stream cursor", Destination: destination,
				OrderingKey: orderingKey, EventID: event.ID, Expected: stream.NextSequence, Observed: event.Sequence})
		}
		if event.Status == "inflight" && event.LeaseUntil != nil && !event.LeaseUntil.After(databaseNow) {
			acc.report.Summary.StaleEventLeases++
			acc.add(Finding{Code: CodeStaleEventLease, Severity: SeverityWarning,
				Message: "inflight event lease has expired and awaits recovery", Destination: destination,
				OrderingKey: orderingKey, EventID: event.ID, Observed: event.LeaseUntil.UTC()})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return StreamInspection{}, fmt.Errorf("integrity: list retained stream events: %w", err)
	}
	rows.Close()

	var partition PartitionState
	var leaseOwner *string
	if err := tx.QueryRow(ctx, `
SELECT partition_id, lease_owner, lease_until, epoch, handoff_not_before,
       publish_timeout_ms, updated_at
FROM emitlane.ordering_partitions WHERE partition_id=$1`, stream.PartitionID).Scan(
		&partition.PartitionID, &leaseOwner, &partition.LeaseUntil, &partition.Epoch,
		&partition.HandoffNotBefore, &partition.PublishTimeoutMS, &partition.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			acc.add(Finding{Code: CodePartitionIDInvalid, Severity: SeverityViolation,
				Message: "stream partition metadata row is missing", Destination: destination,
				OrderingKey: orderingKey, PartitionID: &partitionID, Expected: partitionID, Observed: "missing"})
		} else {
			return StreamInspection{}, fmt.Errorf("integrity: inspect stream partition: %w", err)
		}
	} else {
		if leaseOwner != nil {
			partition.LeaseOwner = *leaseOwner
		}
		inspection.Partition = &partition
	}
	if err := tx.Commit(ctx); err != nil {
		return StreamInspection{}, fmt.Errorf("integrity: finish stream snapshot: %w", err)
	}
	inspection.Report = acc.finish(time.Now().UTC())
	return inspection, nil
}

func addStreamStateFinding(acc *accumulator, inspection StreamInspection, observation CursorObservation) {
	switch inspection.BlockingCondition {
	case StreamStateReady:
		acc.report.Summary.ReadyStreams = 1
	case StreamStateInflight:
		acc.report.Summary.InflightStreams = 1
		acc.report.Summary.BlockedStreams = 1
	case StreamStateRetryWait:
		acc.report.Summary.RetryWaitStreams = 1
		acc.report.Summary.BlockedStreams = 1
		acc.add(Finding{Code: CodeStreamRetryWait, Severity: SeverityWarning,
			Message: "expected stream sequence is waiting for its retry time", Destination: inspection.Destination,
			OrderingKey: inspection.OrderingKey, Expected: observation.NextSequence, Observed: observation.ExpectedAvailableAt})
	case StreamStateGap:
		acc.report.Summary.GapStreams = 1
		acc.report.Summary.BlockedStreams = 1
		acc.add(Finding{Code: CodeStreamGap, Severity: SeverityWarning,
			Message: "expected stream sequence is absent while a future sequence is retained", Destination: inspection.Destination,
			OrderingKey: inspection.OrderingKey, Expected: observation.NextSequence, Observed: observation.LowestFutureSequence})
	case StreamStateDeadBlocked:
		acc.report.Summary.DeadBlockedStreams = 1
		acc.report.Summary.BlockedStreams = 1
		acc.add(Finding{Code: CodeStreamDeadBlocked, Severity: SeverityWarning,
			Message: "dead event blocks the expected stream sequence", Destination: inspection.Destination,
			OrderingKey: inspection.OrderingKey, Expected: observation.NextSequence, Observed: "dead"})
	}
	if observation.ExpectedCount > 1 {
		acc.add(Finding{Code: CodeExpectedSequenceDuplicate, Severity: SeverityViolation,
			Message: "multiple active events occupy the expected sequence", Destination: inspection.Destination,
			OrderingKey: inspection.OrderingKey, Expected: 1, Observed: observation.ExpectedCount})
	}
}

func safeDiagnosticText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}
