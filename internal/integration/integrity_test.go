//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/integrity"
	"github.com/emitlane/emitlane/outbox"
)

func TestIntegrityCleanV3Database(t *testing.T) {
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
