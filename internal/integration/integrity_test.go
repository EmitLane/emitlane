//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/inbox"
	"github.com/emitlane/emitlane/integrity"
	internalordering "github.com/emitlane/emitlane/internal/ordering"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestIntegrityCleanV4Database(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []integrity.Mode{integrity.ModeSummary, integrity.ModeFull} {
		report, err := verifier.Check(ctx, mode)
		if err != nil {
			t.Fatalf("%s check: %v", mode, err)
		}
		if !report.Clean || report.Summary.Violations != 0 || len(report.Findings) != 0 {
			t.Fatalf("%s report is not clean: %+v", mode, report)
		}
		if report.Summary.OrderingPartitions != 64 {
			t.Fatalf("%s partition count=%d", mode, report.Summary.OrderingPartitions)
		}
	}
	adminReport, err := e.store.IntegritySummary(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("Admin integrity summary: %v", err)
	}
	if adminReport.Mode != integrity.ModeSummary || !adminReport.Clean {
		t.Fatalf("Admin integrity report: %+v", adminReport)
	}
}

func TestIntegrityUnderstandsValidInboxLifecycle(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}

	retryRequest := inbox.ClaimRequest{
		Consumer: "integrity-inbox", EventID: uuid.New(),
		Source:     inbox.Source{Topic: "orders", Partition: 0, Offset: 1},
		LeaseOwner: "integrity-a", LeaseDuration: time.Second,
	}
	retryClaim, err := store.Claim(ctx, retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRetry(ctx, retryRequest.Consumer, retryRequest.EventID,
		retryClaim.Event.LeaseToken, time.Hour, "retryable"); err != nil {
		t.Fatal(err)
	}

	deadRequest := retryRequest
	deadRequest.EventID = uuid.New()
	deadRequest.Source.Offset = 2
	deadClaim, err := store.Claim(ctx, deadRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDead(ctx, deadRequest.Consumer, deadRequest.EventID,
		deadClaim.Event.LeaseToken, "permanent"); err != nil {
		t.Fatal(err)
	}

	staleRequest := retryRequest
	staleRequest.EventID = uuid.New()
	staleRequest.Source.Offset = 3
	staleRequest.LeaseDuration = 40 * time.Millisecond
	if _, err := store.Claim(ctx, staleRequest); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)

	legacyID := uuid.New()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Process(ctx, tx, "integrity-legacy", legacyID.String(),
		func(context.Context, pgx.Tx) error { return nil }); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Violations != 0 {
		t.Fatalf("valid Inbox states produced violations: %+v", report.Findings)
	}
	if report.Summary.InboxRetryWait != 1 || report.Summary.InboxDead != 1 ||
		report.Summary.InboxInflight != 1 || report.Summary.InboxProcessed != 1 ||
		report.Summary.InboxStaleLeases != 1 {
		t.Fatalf("Inbox summary: %+v", report.Summary)
	}
	for _, code := range []integrity.Code{
		integrity.CodeInboxRetryWait, integrity.CodeInboxDeadBlocked, integrity.CodeInboxStaleLease,
	} {
		if !hasIntegrityCode(report, code) {
			t.Errorf("finding %s is missing: %+v", code, report.Findings)
		}
	}
}

func TestIntegrityDetectsImpossibleInboxState(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	constraints := []string{
		"inbox_status_check", "inbox_attempts_check", "inbox_lease_state_check",
		"inbox_processed_state_check", "inbox_source_metadata_check",
	}
	for _, constraint := range constraints {
		if _, err := e.pool.Exec(ctx, `ALTER TABLE emitlane.inbox_events DROP CONSTRAINT `+constraint); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = e.pool.Exec(cleanupCtx, `DELETE FROM emitlane.inbox_events WHERE consumer='integrity-corrupt'`)
		statements := []string{
			`ALTER TABLE emitlane.inbox_events ADD CONSTRAINT inbox_status_check CHECK (status IN ('pending', 'inflight', 'retry_wait', 'processed', 'dead'))`,
			`ALTER TABLE emitlane.inbox_events ADD CONSTRAINT inbox_attempts_check CHECK (attempts >= 0)`,
			`ALTER TABLE emitlane.inbox_events ADD CONSTRAINT inbox_lease_state_check CHECK ((status = 'inflight' AND lease_owner IS NOT NULL AND BTRIM(lease_owner) <> '' AND lease_token IS NOT NULL AND lease_until IS NOT NULL) OR (status <> 'inflight' AND lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL))`,
			`ALTER TABLE emitlane.inbox_events ADD CONSTRAINT inbox_processed_state_check CHECK ((status = 'processed') = (processed_at IS NOT NULL))`,
			`ALTER TABLE emitlane.inbox_events ADD CONSTRAINT inbox_source_metadata_check CHECK ((source_topic IS NULL AND source_partition IS NULL AND source_offset IS NULL AND source_timestamp IS NULL AND status = 'processed') OR (source_topic IS NOT NULL AND BTRIM(source_topic) <> '' AND source_partition >= 0 AND source_offset >= 0))`,
		}
		for _, statement := range statements {
			if _, err := e.pool.Exec(cleanupCtx, statement); err != nil {
				t.Errorf("restore Inbox constraint: %v", err)
			}
		}
	})
	eventID := uuid.New()
	if _, err := e.pool.Exec(ctx, `
INSERT INTO emitlane.inbox_events (
    consumer, event_id, processed_at, status, attempts,
    lease_owner, source_topic
) VALUES ('integrity-corrupt', $1, NOW(), 'impossible', -1, 'partial-owner', 'orders')`, eventID); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []integrity.Code{
		integrity.CodeInboxStateInvalid,
		integrity.CodeInboxAttemptsInvalid,
		integrity.CodeInboxLeaseShapeInvalid,
		integrity.CodeInboxProcessedTimestampInvalid,
		integrity.CodeInboxSourceMetadataInvalid,
	} {
		if !hasIntegrityCode(report, code) {
			t.Errorf("corruption finding %s is missing: %+v", code, report.Findings)
		}
	}
}

func TestIntegrityRetentionDoesNotReportHistoricalGap(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id := enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.created",
		OrderingKey: "order:retained", Sequence: 1,
	})
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.outbox_events
SET status='delivered', delivered_at=NOW()
	WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.ordering_streams
SET next_sequence=2
	WHERE destination='orders.integrity' AND ordering_key='order:retained'`); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Clean || hasIntegrityCode(report, integrity.CodeStreamGap) || hasIntegrityCode(report, integrity.CodeActiveSequenceBehindCursor) {
		t.Fatalf("retained delivered history produced a false finding: %+v", report.Findings)
	}
}

func TestIntegrityDetectsGapAndWrongEventPartition(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id := enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.changed",
		OrderingKey: "order:gap", Sequence: 2,
	})
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.outbox_events
SET ordering_partition=(ordering_partition + 1) % 64
WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if !hasIntegrityCode(report, integrity.CodeStreamGap) {
		t.Fatalf("gap finding is missing: %+v", report.Findings)
	}
	if !hasIntegrityCode(report, integrity.CodeOrderingPartitionMismatch) {
		t.Fatalf("partition finding is missing: %+v", report.Findings)
	}
}

func TestIntegrityDetectsActiveSequenceBehindCursor(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.created",
		OrderingKey: "order:behind", Sequence: 1,
	})
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.ordering_streams
SET next_sequence=2
WHERE destination='orders.integrity' AND ordering_key='order:behind'`); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if !hasIntegrityCode(report, integrity.CodeActiveSequenceBehindCursor) {
		t.Fatalf("behind-cursor finding is missing: %+v", report.Findings)
	}
}

func TestIntegrityTargetedStreamInspection(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.changed",
		OrderingKey: "order:inspect", Sequence: 2,
	})
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.InspectStream(ctx, "orders.integrity", "order:inspect")
	if err != nil {
		t.Fatal(err)
	}
	if report.Mode != integrity.ModeStream || report.BlockingCondition != integrity.StreamStateGap {
		t.Fatalf("mode=%q condition=%q", report.Mode, report.BlockingCondition)
	}
	if report.Stream == nil || report.Stream.NextSequence != 1 || report.Partition == nil {
		t.Fatalf("incomplete stream diagnosis: %+v", report)
	}
	if report.Stream.PartitionID != report.ExpectedPartition || len(report.Events) != 1 || report.Events[0].Sequence != 2 {
		t.Fatalf("unexpected stream state: %+v", report)
	}
	if !hasIntegrityCode(report.Report, integrity.CodeStreamGap) {
		t.Fatalf("stream gap finding is missing: %+v", report.Findings)
	}
}

func TestIntegrityWarningsAndExpiredLeaseRecovery(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deadID := enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.dead", OrderingKey: "order:dead", Sequence: 1,
	})
	retryID := enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.retry", OrderingKey: "order:retry", Sequence: 1,
	})
	staleID := enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.stale", OrderingKey: "order:stale", Sequence: 1,
	})
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET status='dead' WHERE id=$1`, deadID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET available_at=NOW()+INTERVAL '1 hour' WHERE id=$1`, retryID); err != nil {
		t.Fatal(err)
	}
	registerOrderingRelay(t, e, "expired-relay")
	if _, err := e.store.ReconcileOrderingPartitions(ctx, "expired-relay", 2*time.Second,
		100*time.Millisecond, 5*time.Second, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	claimed, err := e.store.ClaimOrdered(ctx, "expired-relay", 1, 100*time.Millisecond, 200*time.Millisecond)
	if err != nil || len(claimed) != 1 || claimed[0].ID != staleID {
		t.Fatalf("claim event for lease expiry: count=%d event=%v err=%v", len(claimed), claimed, err)
	}
	time.Sleep(150 * time.Millisecond)
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []integrity.Code{integrity.CodeStreamDeadBlocked, integrity.CodeStreamRetryWait, integrity.CodeStaleEventLease} {
		if !hasIntegrityCode(report, code) {
			t.Errorf("finding %s is missing: %+v", code, report.Findings)
		}
	}
	if report.Summary.Violations != 0 {
		t.Fatalf("valid warning states produced violations: %+v", report.Findings)
	}

	runRelay(t, e.newRelay(t, orderedRelayConfig("expired-relay"), e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, staleID.String(), "delivered", 20*time.Second)
	recovered, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if hasIntegrityCode(recovered, integrity.CodeStaleEventLease) {
		t.Fatalf("recovered expired lease remains stale: %+v", recovered.Findings)
	}
}

func TestIntegrityDetectsPartitionSeedCorruption(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `DELETE FROM emitlane.ordering_partitions WHERE partition_id=63`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := e.pool.Exec(context.Background(), `INSERT INTO emitlane.ordering_partitions (partition_id) VALUES (63) ON CONFLICT DO NOTHING`); err != nil {
			t.Errorf("restore partition seed: %v", err)
		}
	})
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []integrity.Code{integrity.CodePartitionCountInvalid, integrity.CodePartitionIDInvalid} {
		if !hasIntegrityCode(report, code) {
			t.Errorf("partition corruption finding %s is missing: %+v", code, report.Findings)
		}
	}
}

func TestIntegrityDetectsInvalidCursorAndLeaseShape(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	enqueueIntegrityEvent(t, e, outbox.Event{
		Destination: "orders.integrity", Type: "order.cursor", OrderingKey: "order:cursor", Sequence: 1,
	})
	if _, err := e.pool.Exec(ctx, `ALTER TABLE emitlane.ordering_streams DROP CONSTRAINT ordering_stream_next_check`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `ALTER TABLE emitlane.ordering_partitions DROP CONSTRAINT ordering_partition_lease_check`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = e.pool.Exec(cleanupCtx, `UPDATE emitlane.ordering_streams SET next_sequence=start_sequence WHERE next_sequence<start_sequence`)
		_, _ = e.pool.Exec(cleanupCtx, `UPDATE emitlane.ordering_partitions SET lease_owner=NULL, lease_until=NULL WHERE lease_owner IS NOT NULL AND lease_until IS NULL`)
		_, err := e.pool.Exec(cleanupCtx, `ALTER TABLE emitlane.ordering_streams ADD CONSTRAINT ordering_stream_next_check CHECK (next_sequence >= start_sequence)`)
		if err != nil {
			t.Errorf("restore stream constraint: %v", err)
		}
		_, err = e.pool.Exec(cleanupCtx, `ALTER TABLE emitlane.ordering_partitions ADD CONSTRAINT ordering_partition_lease_check CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR (lease_owner IS NOT NULL AND BTRIM(lease_owner) <> '' AND lease_until IS NOT NULL))`)
		if err != nil {
			t.Errorf("restore partition constraint: %v", err)
		}
	})
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.ordering_streams SET start_sequence=2, next_sequence=1 WHERE ordering_key='order:cursor'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.ordering_partitions SET lease_owner='broken-owner', lease_until=NULL WHERE partition_id=0`); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []integrity.Code{integrity.CodeSchemaVersionMismatch, integrity.CodeStreamCursorInvalid, integrity.CodePartitionLeaseShapeInvalid} {
		if !hasIntegrityCode(report, code) {
			t.Errorf("corruption finding %s is missing: %+v", code, report.Findings)
		}
	}
}

func TestIntegrityStatementTimeoutIsOperationalFailure(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lock, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(ctx, `LOCK TABLE emitlane.outbox_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	config := integrity.DefaultConfig()
	config.StatementTimeout = 50 * time.Millisecond
	verifier, err := integrity.NewVerifier(e.pool, config)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := verifier.Check(ctx, integrity.ModeFull); err == nil {
		t.Fatal("integrity check unexpectedly ignored statement timeout")
	} else if time.Since(started) > time.Second {
		t.Fatalf("statement timeout was not bounded: elapsed=%s error=%v", time.Since(started), err)
	}
}

func TestIntegrityWorksWithSelectOnlyDatabaseRole(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	role := "integrity_reader_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	password := "reader_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{role}.Sanitize()
	if _, err := e.pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", identifier, password)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA emitlane TO %s; GRANT SELECT ON ALL TABLES IN SCHEMA emitlane TO %s", identifier, identifier)); err != nil {
		_, _ = e.pool.Exec(context.Background(), "DROP ROLE "+identifier)
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(e.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User = role
	config.ConnConfig.Password = password
	reader, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reader.Close()
		if _, err := e.pool.Exec(context.Background(), "DROP OWNED BY "+identifier); err != nil {
			t.Errorf("revoke integrity reader grants: %v", err)
		}
		if _, err := e.pool.Exec(context.Background(), "DROP ROLE "+identifier); err != nil {
			t.Errorf("drop integrity reader role: %v", err)
		}
	})
	if _, err := reader.Exec(ctx, `UPDATE emitlane.runtime_control SET updated_at=NOW()`); err == nil {
		t.Fatal("select-only verifier role unexpectedly mutated protocol state")
	}
	verifier, err := integrity.NewVerifier(reader, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Clean || report.Summary.Violations != 0 {
		t.Fatalf("select-only verification report: %+v", report)
	}
}

func TestIntegrityHealthyRelayConcurrencyHasNoFalseViolations(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	const streams = 6
	const perStream = 4
	for sequence := int64(1); sequence <= perStream; sequence++ {
		for stream := range streams {
			enqueueOrdered(t, e, topic, fmt.Sprintf("integrity-live-%d", stream), sequence)
		}
	}
	newPublisher := func(failures int) *transientKafkaPublisher {
		return &transientKafkaPublisher{inner: e.publisher(t), failures: failures, delay: 40 * time.Millisecond}
	}
	stopA := runRelay(t, e.newRelay(t, orderedRelayConfig("integrity-live-a"), newPublisher(4), relay.FailureHooks{}))
	runRelay(t, e.newRelay(t, orderedRelayConfig("integrity-live-b"), newPublisher(3), relay.FailureHooks{}))
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	checks, maxWarnings := 0, int64(0)
	rebalanced := false
	for time.Now().Before(deadline) {
		report, err := verifier.Check(context.Background(), integrity.ModeFull)
		if err != nil {
			t.Fatal(err)
		}
		checks++
		if report.Summary.Violations != 0 {
			t.Fatalf("healthy concurrent Relay produced false violations: %+v", report.Findings)
		}
		if report.Summary.Warnings > maxWarnings {
			maxWarnings = report.Summary.Warnings
		}
		var delivered int
		if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM emitlane.outbox_events WHERE destination=$1 AND status='delivered'`, topic).Scan(&delivered); err != nil {
			t.Fatal(err)
		}
		if !rebalanced && delivered >= 4 {
			stopA()
			runRelay(t, e.newRelay(t, orderedRelayConfig("integrity-live-c"), newPublisher(2), relay.FailureHooks{}))
			rebalanced = true
		}
		if delivered == streams*perStream {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rebalanced || checks < 2 {
		t.Fatalf("concurrency scenario did not exercise enough live state: checks=%d rebalanced=%t", checks, rebalanced)
	}
	for stream := range streams {
		key := fmt.Sprintf("integrity-live-%d", stream)
		var next int64
		if err := e.pool.QueryRow(context.Background(), `SELECT next_sequence FROM emitlane.ordering_streams WHERE destination=$1 AND ordering_key=$2`, topic, key).Scan(&next); err != nil {
			t.Fatal(err)
		}
		if next != perStream+1 {
			t.Fatalf("stream %s ended at next=%d", key, next)
		}
	}
	t.Logf("healthy concurrency verifier checks=%d maximum_warnings=%d", checks, maxWarnings)
}

func TestIntegrityScale10000Streams100000ActiveEvents(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const streamCount = 10_000
	const eventsPerStream = 10
	const eventCount = streamCount * eventsPerStream
	destination := "integrity.scale"
	if _, err := e.pool.CopyFrom(ctx, pgx.Identifier{"emitlane", "ordering_streams"},
		[]string{"destination", "ordering_key", "partition_id", "start_sequence", "next_sequence"},
		pgx.CopyFromSlice(streamCount, func(i int) ([]any, error) {
			key := fmt.Sprintf("scale:%05d", i)
			return []any{destination, key, internalordering.Partition(destination, key), int64(1), int64(1)}, nil
		})); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.CopyFrom(ctx, pgx.Identifier{"emitlane", "outbox_events"},
		[]string{"id", "destination", "event_type", "payload", "ordering_key", "ordering_sequence", "ordering_partition"},
		pgx.CopyFromSlice(eventCount, func(i int) ([]any, error) {
			stream := i / eventsPerStream
			key := fmt.Sprintf("scale:%05d", stream)
			sequence := int64(i%eventsPerStream + 1)
			return []any{uuid.New(), destination, "scale.event", []byte{}, key, sequence, internalordering.Partition(destination, key)}, nil
		})); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	config := integrity.DefaultConfig()
	config.StatementTimeout = 2 * time.Minute
	config.MaxFindings = 10
	verifier, err := integrity.NewVerifier(e.pool, config)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	report, err := verifier.Check(ctx, integrity.ModeFull)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Violations != 0 || report.Summary.OrderedStreams != streamCount || report.Summary.ActiveOrderedEvents != eventCount {
		t.Fatalf("scale report mismatch: %+v findings=%+v", report.Summary, report.Findings)
	}
	t.Logf("integrity scale scan streams=%d active_events=%d duration=%s total_alloc_delta_bytes=%d heap_alloc_after_bytes=%d",
		streamCount, eventCount, elapsed, after.TotalAlloc-before.TotalAlloc, after.HeapAlloc)
}

func enqueueIntegrityEvent(t *testing.T, e *env, event outbox.Event) uuid.UUID {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.writer.Enqueue(ctx, tx, event)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func hasIntegrityCode(report integrity.Report, code integrity.Code) bool {
	for _, finding := range report.Findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
