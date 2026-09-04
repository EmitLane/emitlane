package integrity

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
)

const (
	DefaultMaxFindings = 100
	MaximumMaxFindings = 1000
)

type Config struct {
	MaxFindings        int
	StatementTimeout   time.Duration
	PresenceStaleAfter time.Duration
	HandoffLongAfter   time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxFindings:        DefaultMaxFindings,
		StatementTimeout:   30 * time.Second,
		PresenceStaleAfter: 30 * time.Second,
		HandoffLongAfter:   30 * time.Second,
	}
}

type Verifier struct {
	pool   *pgxpool.Pool
	config Config
}

func NewVerifier(pool *pgxpool.Pool, config Config) (*Verifier, error) {
	if pool == nil {
		return nil, fmt.Errorf("integrity: PostgreSQL pool is required")
	}
	if config.MaxFindings == 0 {
		config.MaxFindings = DefaultMaxFindings
	}
	if config.StatementTimeout == 0 {
		config.StatementTimeout = 30 * time.Second
	}
	if config.PresenceStaleAfter == 0 {
		config.PresenceStaleAfter = 30 * time.Second
	}
	if config.HandoffLongAfter == 0 {
		config.HandoffLongAfter = 30 * time.Second
	}
	if config.MaxFindings < 1 || config.MaxFindings > MaximumMaxFindings {
		return nil, fmt.Errorf("integrity: max findings must be between 1 and %d", MaximumMaxFindings)
	}
	if config.StatementTimeout < time.Millisecond || config.PresenceStaleAfter <= 0 || config.HandoffLongAfter <= 0 {
		return nil, fmt.Errorf("integrity: timeouts and stale thresholds must be positive")
	}
	return &Verifier{pool: pool, config: config}, nil
}

// Check verifies a summary or full scope in one read-only repeatable-read
// snapshot. The caller's context is the outer execution bound.
func (v *Verifier) Check(ctx context.Context, mode Mode) (Report, error) {
	if mode != ModeSummary && mode != ModeFull {
		return Report{}, fmt.Errorf("integrity: unsupported check mode %q", mode)
	}
	startedAt := time.Now().UTC()
	acc := newAccumulator(mode, v.config.MaxFindings, startedAt)
	tx, err := v.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return Report{}, fmt.Errorf("integrity: begin read-only snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	timeoutValue := strconv.FormatInt(int64((v.config.StatementTimeout+time.Millisecond-1)/time.Millisecond), 10)
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout', $1, true)`, timeoutValue); err != nil {
		return Report{}, fmt.Errorf("integrity: set statement timeout: %w", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&databaseNow); err != nil {
		return Report{}, fmt.Errorf("integrity: read snapshot time: %w", err)
	}

	runtimeReady, err := checkSchema(ctx, tx, acc)
	if err != nil {
		return Report{}, err
	}
	if runtimeReady {
		checks := []func(context.Context, pgx.Tx, *accumulator, time.Time, Config, Mode) error{
			checkRuntimeControl,
			checkPartitions,
			checkRelayPresence,
			checkEvents,
			checkStreams,
		}
		for _, check := range checks {
			if err := check(ctx, tx, acc, databaseNow, v.config, mode); err != nil {
				return Report{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Report{}, fmt.Errorf("integrity: finish read-only snapshot: %w", err)
	}
	return acc.finish(time.Now().UTC()), nil
}

var requiredTables = []string{
	"schema_migrations", "outbox_events", "inbox_events", "runtime_control",
	"relay_instances", "admin_audit_log", "ordering_streams", "ordering_partitions",
}

var requiredIndexes = []string{
	"outbox_pending_idx", "outbox_inflight_lease_idx", "outbox_dead_idx",
	"outbox_created_idx", "outbox_status_created_idx", "outbox_destination_type_created_idx",
	"outbox_replay_batch_idx", "admin_audit_created_idx", "outbox_ordering_sequence_unique_idx",
	"outbox_ordered_claim_idx", "ordering_stream_partition_idx",
}

var requiredConstraints = []string{
	"outbox_ordering_state_check", "ordering_stream_partition_check",
	"ordering_stream_start_check", "ordering_stream_next_check",
	"ordering_partition_lease_check", "ordering_partition_epoch_check",
}

var requiredColumns = map[string][]string{
	"outbox_events":       {"id", "destination", "status", "available_at", "lease_owner", "lease_until", "ordering_key", "ordering_sequence", "ordering_partition"},
	"runtime_control":     {"singleton", "paused", "updated_at", "updated_by"},
	"relay_instances":     {"instance_id", "last_heartbeat_at", "stopped_at", "ordering_capable"},
	"ordering_streams":    {"destination", "ordering_key", "partition_id", "start_sequence", "next_sequence"},
	"ordering_partitions": {"partition_id", "lease_owner", "lease_until", "epoch", "handoff_not_before", "publish_timeout_ms", "updated_at"},
}

func checkSchema(ctx context.Context, tx pgx.Tx, acc *accumulator) (bool, error) {
	tables, err := stringSet(ctx, tx, `
SELECT table_name FROM information_schema.tables
WHERE table_schema='emitlane'`)
	if err != nil {
		return false, fmt.Errorf("integrity: inspect schema tables: %w", err)
	}
	runtimeReady := true
	for _, table := range requiredTables {
		if !tables[table] {
			acc.add(Finding{Code: CodeSchemaVersionMismatch, Severity: SeverityViolation,
				Message: "required v3 table is missing", Expected: table, Observed: "missing"})
			runtimeReady = false
		}
	}
	if tables["schema_migrations"] {
		var version int
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM emitlane.schema_migrations`).Scan(&version); err != nil {
			return false, fmt.Errorf("integrity: inspect schema version: %w", err)
		}
		if version != 3 {
			acc.add(Finding{Code: CodeSchemaVersionMismatch, Severity: SeverityViolation,
				Message: "applied migration version does not match this binary", Expected: 3, Observed: version})
		}
	}

	indexes, err := stringSet(ctx, tx, `SELECT indexname FROM pg_indexes WHERE schemaname='emitlane'`)
	if err != nil {
		return false, fmt.Errorf("integrity: inspect schema indexes: %w", err)
	}
	for _, index := range requiredIndexes {
		if !indexes[index] {
			acc.add(Finding{Code: CodeSchemaVersionMismatch, Severity: SeverityViolation,
				Message: "required v3 index is missing", Expected: index, Observed: "missing"})
		}
	}

	constraints, err := stringSet(ctx, tx, `
SELECT constraint_record.conname
FROM pg_catalog.pg_constraint AS constraint_record
JOIN pg_catalog.pg_class AS relation ON relation.oid=constraint_record.conrelid
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
WHERE namespace.nspname='emitlane'`)
	if err != nil {
		return false, fmt.Errorf("integrity: inspect schema constraints: %w", err)
	}
	for _, constraint := range requiredConstraints {
		if !constraints[constraint] {
			acc.add(Finding{Code: CodeSchemaVersionMismatch, Severity: SeverityViolation,
				Message: "required v3 constraint is missing", Expected: constraint, Observed: "missing"})
		}
	}

	rows, err := tx.Query(ctx, `
SELECT table_name, column_name FROM information_schema.columns
WHERE table_schema='emitlane'`)
	if err != nil {
		return false, fmt.Errorf("integrity: inspect schema columns: %w", err)
	}
	columns := make(map[string]map[string]bool)
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			rows.Close()
			return false, fmt.Errorf("integrity: scan schema column: %w", err)
		}
		if columns[table] == nil {
			columns[table] = make(map[string]bool)
		}
		columns[table][column] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, fmt.Errorf("integrity: inspect schema columns: %w", err)
	}
	rows.Close()
	for table, names := range requiredColumns {
		for _, column := range names {
			if !columns[table][column] {
				acc.add(Finding{Code: CodeSchemaVersionMismatch, Severity: SeverityViolation,
					Message: "required v3 column is missing", Expected: table + "." + column, Observed: "missing"})
				runtimeReady = false
			}
		}
	}
	return runtimeReady, nil
}

func stringSet(ctx context.Context, tx pgx.Tx, query string) (map[string]bool, error) {
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]bool)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		result[value] = true
	}
	return result, rows.Err()
}

func checkRuntimeControl(ctx context.Context, tx pgx.Tx, acc *accumulator, _ time.Time, _ Config, _ Mode) error {
	var total, singleton, invalidActor int64
	if err := tx.QueryRow(ctx, `
SELECT COUNT(*), COUNT(*) FILTER (WHERE singleton=TRUE),
       COUNT(*) FILTER (WHERE BTRIM(updated_by)='')
FROM emitlane.runtime_control`).Scan(&total, &singleton, &invalidActor); err != nil {
		return fmt.Errorf("integrity: inspect runtime control: %w", err)
	}
	if total != 1 || singleton != 1 || invalidActor != 0 {
		acc.add(Finding{Code: CodeRuntimeControlInvalid, Severity: SeverityViolation,
			Message: "runtime-control singleton is invalid", Expected: map[string]any{"rows": 1, "singleton_rows": 1, "blank_actors": 0},
			Observed: map[string]any{"rows": total, "singleton_rows": singleton, "blank_actors": invalidActor}})
	}
	return nil
}

func checkPartitions(ctx context.Context, tx pgx.Tx, acc *accumulator, now time.Time, config Config, _ Mode) error {
	rows, err := tx.Query(ctx, `
SELECT p.partition_id, p.lease_owner, p.lease_until, p.epoch,
       p.handoff_not_before, p.publish_timeout_ms, p.updated_at,
       r.instance_id, r.last_heartbeat_at, r.stopped_at, r.ordering_capable
FROM emitlane.ordering_partitions AS p
LEFT JOIN emitlane.relay_instances AS r ON r.instance_id=p.lease_owner
ORDER BY p.partition_id`)
	if err != nil {
		return fmt.Errorf("integrity: inspect ordering partitions: %w", err)
	}
	defer rows.Close()
	seen := make(map[int16]bool, internalordering.PartitionCount)
	for rows.Next() {
		var id int16
		var owner, relayID *string
		var leaseUntil, handoff, updatedAt, heartbeat, stoppedAt *time.Time
		var epoch int64
		var publishTimeout *int
		var orderingCapable *bool
		if err := rows.Scan(&id, &owner, &leaseUntil, &epoch, &handoff, &publishTimeout, &updatedAt,
			&relayID, &heartbeat, &stoppedAt, &orderingCapable); err != nil {
			return fmt.Errorf("integrity: scan ordering partition: %w", err)
		}
		acc.report.Summary.OrderingPartitions++
		seen[id] = true
		partitionID := id
		if id < 0 || int(id) >= internalordering.PartitionCount {
			acc.add(Finding{Code: CodePartitionIDInvalid, Severity: SeverityViolation,
				Message: "unexpected ordering partition ID", PartitionID: &partitionID,
				Expected: "0..63", Observed: id})
		}
		if epoch < 0 {
			acc.add(Finding{Code: CodePartitionEpochInvalid, Severity: SeverityViolation,
				Message: "ordering partition epoch is negative", PartitionID: &partitionID,
				Expected: ">= 0", Observed: epoch})
		}
		leaseShapeInvalid := owner == nil && leaseUntil != nil || owner != nil && leaseUntil == nil ||
			owner != nil && strings.TrimSpace(*owner) == "" || publishTimeout != nil && *publishTimeout <= 0
		if leaseShapeInvalid {
			acc.add(Finding{Code: CodePartitionLeaseShapeInvalid, Severity: SeverityViolation,
				Message: "ordering partition lease fields form an invalid shape", PartitionID: &partitionID})
		}
		if owner != nil && leaseUntil != nil && leaseUntil.After(now) {
			acc.report.Summary.OwnedPartitions++
			stale := relayID == nil || heartbeat == nil || stoppedAt != nil || orderingCapable == nil || !*orderingCapable ||
				heartbeat.Before(now.Add(-config.PresenceStaleAfter))
			if stale {
				acc.add(Finding{Code: CodePartitionOwnerStale, Severity: SeverityWarning,
					Message: "active partition lease names an unavailable Relay", PartitionID: &partitionID})
			}
		}
		if handoff != nil && handoff.After(now) {
			acc.report.Summary.HandoffPartitions++
			if updatedAt != nil && updatedAt.Before(now.Add(-config.HandoffLongAfter)) {
				acc.add(Finding{Code: CodePartitionHandoffOverdue, Severity: SeverityWarning,
					Message: "ordering partition handoff has remained pending beyond the diagnostic threshold", PartitionID: &partitionID,
					Observed: handoff.UTC()})
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("integrity: inspect ordering partitions: %w", err)
	}
	if len(seen) != internalordering.PartitionCount {
		acc.add(Finding{Code: CodePartitionCountInvalid, Severity: SeverityViolation,
			Message: "ordering partition seed count is invalid", Expected: internalordering.PartitionCount, Observed: len(seen)})
	}
	for id := int16(0); id < internalordering.PartitionCount; id++ {
		if !seen[id] {
			partitionID := id
			acc.add(Finding{Code: CodePartitionIDInvalid, Severity: SeverityViolation,
				Message: "required ordering partition ID is missing", PartitionID: &partitionID,
				Expected: id, Observed: "missing"})
		}
	}
	return nil
}

func checkRelayPresence(ctx context.Context, tx pgx.Tx, acc *accumulator, now time.Time, config Config, _ Mode) error {
	rows, err := tx.Query(ctx, `
SELECT instance_id FROM emitlane.relay_instances
WHERE stopped_at IS NULL AND last_heartbeat_at < $1`, now.Add(-config.PresenceStaleAfter))
	if err != nil {
		return fmt.Errorf("integrity: inspect Relay presence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ignored string
		if err := rows.Scan(&ignored); err != nil {
			return fmt.Errorf("integrity: scan Relay presence: %w", err)
		}
		acc.report.Summary.StaleRelays++
		acc.add(Finding{Code: CodeRelayPresenceStale, Severity: SeverityWarning,
			Message: "Relay presence heartbeat is stale"})
	}
	return rows.Err()
}

func checkEvents(ctx context.Context, tx pgx.Tx, acc *accumulator, now time.Time, _ Config, mode Mode) error {
	var pending, inflight, dead, activeOrdered int64
	if err := tx.QueryRow(ctx, `
SELECT COUNT(*) FILTER (WHERE status='pending'),
       COUNT(*) FILTER (WHERE status='inflight'),
       COUNT(*) FILTER (WHERE status='dead'),
       COUNT(*) FILTER (WHERE status IN ('pending','inflight','dead') AND ordering_key IS NOT NULL)
FROM emitlane.outbox_events`).Scan(&pending, &inflight, &dead, &activeOrdered); err != nil {
		return fmt.Errorf("integrity: count active events: %w", err)
	}
	acc.report.Summary.PendingEvents = pending
	acc.report.Summary.InflightEvents = inflight
	acc.report.Summary.DeadEvents = dead
	acc.report.Summary.ActiveOrderedEvents = activeOrdered

	where := `e.status IN ('pending','inflight','dead') OR
        e.ordering_key IS NOT NULL OR e.ordering_sequence IS NOT NULL OR e.ordering_partition IS NOT NULL`
	if mode == ModeSummary {
		where = `e.status IN ('pending','inflight','dead')`
	}
	rows, err := tx.Query(ctx, `
SELECT e.id::text, e.destination, e.status, e.available_at, e.lease_until,
       e.ordering_key, e.ordering_sequence, e.ordering_partition,
       s.next_sequence
FROM emitlane.outbox_events AS e
LEFT JOIN emitlane.ordering_streams AS s
  ON s.destination=e.destination AND s.ordering_key=e.ordering_key
WHERE `+where+`
ORDER BY e.id`)
	if err != nil {
		return fmt.Errorf("integrity: inspect retained events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, destination, status string
		var availableAt time.Time
		var leaseUntil *time.Time
		var key *string
		var sequence *int64
		var partition *int16
		var nextSequence *int64
		if err := rows.Scan(&id, &destination, &status, &availableAt, &leaseUntil, &key, &sequence, &partition, &nextSequence); err != nil {
			return fmt.Errorf("integrity: scan retained event: %w", err)
		}
		if status == "inflight" && leaseUntil != nil && !leaseUntil.After(now) {
			acc.report.Summary.StaleEventLeases++
			acc.add(Finding{Code: CodeStaleEventLease, Severity: SeverityWarning,
				Message: "inflight event lease has expired and awaits recovery", Destination: destination, EventID: id,
				Observed: leaseUntil.UTC()})
		}
		orderedFields := 0
		if key != nil {
			orderedFields++
		}
		if sequence != nil {
			orderedFields++
		}
		if partition != nil {
			orderedFields++
		}
		if orderedFields != 0 && orderedFields != 3 || key != nil && strings.TrimSpace(*key) == "" || sequence != nil && *sequence <= 0 {
			acc.add(Finding{Code: CodeOrderingFieldsPartial, Severity: SeverityViolation,
				Message: "event has incomplete or invalid ordering metadata", Destination: destination, EventID: id})
			continue
		}
		if orderedFields == 0 {
			continue
		}
		expectedPartition := internalordering.Partition(destination, *key)
		if *partition != expectedPartition {
			partitionCopy := *partition
			acc.add(Finding{Code: CodeOrderingPartitionMismatch, Severity: SeverityViolation,
				Message: "ordered event partition differs from the deterministic mapping", Destination: destination,
				EventID: id, PartitionID: &partitionCopy, Expected: expectedPartition, Observed: *partition})
		}
		active := status == "pending" || status == "inflight" || status == "dead"
		if active && nextSequence == nil {
			acc.add(Finding{Code: CodeStreamCursorInvalid, Severity: SeverityViolation,
				Message: "active ordered event has no durable stream cursor", Destination: destination, EventID: id})
		} else if active && *sequence < *nextSequence {
			acc.add(Finding{Code: CodeActiveSequenceBehindCursor, Severity: SeverityViolation,
				Message: "active ordered event is below the durable stream cursor", Destination: destination, EventID: id,
				Expected: map[string]any{"minimum_sequence": *nextSequence}, Observed: *sequence})
		} else if status == "delivered" && nextSequence != nil && *sequence >= *nextSequence {
			acc.add(Finding{Code: CodeStreamCursorInvalid, Severity: SeverityViolation,
				Message: "retained delivered event is not below the durable stream cursor", Destination: destination, EventID: id,
				Expected: map[string]any{"next_sequence_above": *sequence}, Observed: *nextSequence})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("integrity: inspect retained events: %w", err)
	}
	return nil
}

func checkStreams(ctx context.Context, tx pgx.Tx, acc *accumulator, now time.Time, _ Config, _ Mode) error {
	rows, err := tx.Query(ctx, `
SELECT s.destination, s.ordering_key, s.partition_id, s.start_sequence, s.next_sequence,
       COUNT(e.id) FILTER (WHERE e.ordering_sequence=s.next_sequence),
       MIN(e.status) FILTER (WHERE e.ordering_sequence=s.next_sequence),
       MIN(e.available_at) FILTER (WHERE e.ordering_sequence=s.next_sequence),
       MIN(e.lease_until) FILTER (WHERE e.ordering_sequence=s.next_sequence),
       MIN(e.ordering_sequence) FILTER (WHERE e.ordering_sequence>s.next_sequence)
FROM emitlane.ordering_streams AS s
LEFT JOIN emitlane.outbox_events AS e
  ON e.destination=s.destination AND e.ordering_key=s.ordering_key
 AND e.status IN ('pending','inflight','dead')
GROUP BY s.destination, s.ordering_key, s.partition_id, s.start_sequence, s.next_sequence
ORDER BY s.destination, s.ordering_key`)
	if err != nil {
		return fmt.Errorf("integrity: inspect ordering streams: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var destination, key string
		var partition int16
		var observation CursorObservation
		var expectedStatus *string
		if err := rows.Scan(&destination, &key, &partition, &observation.StartSequence, &observation.NextSequence,
			&observation.ExpectedCount, &expectedStatus, &observation.ExpectedAvailableAt,
			&observation.ExpectedLeaseUntil, &observation.LowestFutureSequence); err != nil {
			return fmt.Errorf("integrity: scan ordering stream: %w", err)
		}
		if expectedStatus != nil {
			observation.ExpectedStatus = *expectedStatus
		}
		acc.report.Summary.OrderedStreams++
		partitionCopy := partition
		expectedPartition := internalordering.Partition(destination, key)
		if partition != expectedPartition {
			acc.add(Finding{Code: CodeStreamPartitionMismatch, Severity: SeverityViolation,
				Message: "stream partition differs from the deterministic mapping", Destination: destination,
				PartitionID: &partitionCopy, Expected: expectedPartition, Observed: partition})
		}
		if observation.StartSequence <= 0 || observation.NextSequence < observation.StartSequence {
			acc.add(Finding{Code: CodeStreamCursorInvalid, Severity: SeverityViolation,
				Message: "stream cursor is outside its valid range", Destination: destination,
				Expected: map[string]any{"start_sequence": "> 0", "next_sequence": ">= start_sequence"},
				Observed: map[string]any{"start_sequence": observation.StartSequence, "next_sequence": observation.NextSequence}})
		}
		if observation.ExpectedCount > 1 {
			acc.add(Finding{Code: CodeExpectedSequenceDuplicate, Severity: SeverityViolation,
				Message: "multiple active events occupy the expected sequence", Destination: destination,
				Expected: 1, Observed: observation.ExpectedCount})
		}
		state := ClassifyCursor(now, observation)
		switch state {
		case StreamStateReady:
			acc.report.Summary.ReadyStreams++
		case StreamStateInflight:
			acc.report.Summary.InflightStreams++
			acc.report.Summary.BlockedStreams++
		case StreamStateRetryWait:
			acc.report.Summary.RetryWaitStreams++
			acc.report.Summary.BlockedStreams++
			acc.add(Finding{Code: CodeStreamRetryWait, Severity: SeverityWarning,
				Message: "expected stream sequence is waiting for its retry time", Destination: destination,
				Expected: observation.NextSequence, Observed: observation.ExpectedAvailableAt})
		case StreamStateGap:
			acc.report.Summary.GapStreams++
			acc.report.Summary.BlockedStreams++
			acc.add(Finding{Code: CodeStreamGap, Severity: SeverityWarning,
				Message: "expected stream sequence is absent while a future sequence is retained", Destination: destination,
				Expected: observation.NextSequence, Observed: observation.LowestFutureSequence})
		case StreamStateDeadBlocked:
			acc.report.Summary.DeadBlockedStreams++
			acc.report.Summary.BlockedStreams++
			acc.add(Finding{Code: CodeStreamDeadBlocked, Severity: SeverityWarning,
				Message: "dead event blocks the expected stream sequence", Destination: destination,
				Expected: observation.NextSequence, Observed: "dead"})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("integrity: inspect ordering streams: %w", err)
	}
	return nil
}

// SortedStableCodes is useful to presentation layers that expose the bounded
// automation contract.
func SortedStableCodes() []Code {
	codes := StableCodes()
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	return codes
}
