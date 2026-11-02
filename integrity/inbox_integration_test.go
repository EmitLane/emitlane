//go:build integration

package integrity

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	containernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Exercise the actual SQL filter and decoder against the pre-optimization
// row-by-row verifier, including states the schema normally prevents.
func TestCheckInboxMatchesLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("emitlane"), postgres.WithUsername("emitlane"),
		postgres.WithPassword("emitlane"), postgres.BasicWaitStrategies(),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.PortBindings = containernetwork.PortMap{
				containernetwork.MustParsePort("5432/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1")}},
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(pg); err != nil {
			t.Error(err)
		}
	})
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `CREATE SCHEMA emitlane;
 CREATE TABLE emitlane.inbox_events (
 consumer TEXT, event_id UUID, status TEXT, attempts INTEGER, processed_at TIMESTAMPTZ,
 lease_owner TEXT, lease_token UUID, lease_until TIMESTAMPTZ, source_topic TEXT,
 source_partition INTEGER, source_offset BIGINT, source_timestamp TIMESTAMPTZ,
 available_at TIMESTAMPTZ)`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	columns := []string{"consumer", "event_id", "status", "attempts", "processed_at",
		"lease_owner", "lease_token", "lease_until", "source_topic", "source_partition",
		"source_offset", "source_timestamp", "available_at"}
	base := func(i int) []any {
		return []any{"consumer", fmt.Sprintf("%08x-0000-0000-0000-000000000000", i),
			"processed", int32(1), now, nil, nil, nil, "orders", int32(0), int64(1), now, now}
	}
	replace := func(t *testing.T, rows [][]any) {
		t.Helper()
		if _, err := pool.Exec(ctx, "TRUNCATE emitlane.inbox_events"); err != nil {
			t.Fatal(err)
		}
		if len(rows) > 0 {
			if _, err := pool.CopyFrom(ctx, pgx.Identifier{"emitlane", "inbox_events"}, columns, pgx.CopyFromRows(rows)); err != nil {
				t.Fatal(err)
			}
		}
	}
	compare := func(t *testing.T, wantError bool, limits ...int) {
		t.Helper()
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout='30s'"); err != nil {
			t.Fatal(err)
		}
		for _, limit := range limits {
			want, got := newAccumulator(ModeFull, limit, now), newAccumulator(ModeFull, limit, now)
			oldErr := legacyCheckInbox(ctx, tx, want, now, DefaultConfig(), ModeFull)
			newErr := checkInbox(ctx, tx, got, now, DefaultConfig(), ModeFull)
			if (oldErr != nil) != wantError || (newErr != nil) != wantError {
				t.Fatalf("limit=%d expected error=%v legacy=%v new=%v", limit, wantError, oldErr, newErr)
			}
			if !wantError && !reflect.DeepEqual(want.finish(now), got.finish(now)) {
				t.Fatalf("limit=%d reports differ\nlegacy=%+v\nnew=%+v", limit, want.report, got.report)
			}
		}
	}
	t.Run("empty", func(t *testing.T) { replace(t, nil); compare(t, false, 1, 1000) })
	t.Run("valid-history", func(t *testing.T) {
		rows := make([][]any, 512)
		for i := range rows {
			rows[i] = base(i)
			if i%2 == 0 {
				for _, c := range []int{8, 9, 10, 11} {
					rows[i][c] = nil
				}
			}
		}
		replace(t, rows)
		compare(t, false, 1, 1000)
	})
	t.Run("corruption-and-whitespace", func(t *testing.T) {
		texts := []any{nil, "", " ", "\t\r\n", "orders", " orders", "заказы", "\u200b", "\uFEFF"}
		for r := rune(1); r <= unicode.MaxRune; r++ {
			if unicode.IsSpace(r) {
				texts = append(texts, string(r), string(r)+"orders")
			}
		}
		choices := map[int][]any{
			2: {"pending", "inflight", "retry_wait", "processed", "dead", "invalid"},
			3: {int32(-1), int32(0), int32(12)}, 4: {nil, now}, 5: texts,
			6: {nil, "00000000-0000-0000-0000-000000000001"}, 7: {nil, now.Add(-time.Second), now, now.Add(time.Second)},
			8: texts, 9: {nil, int32(-1), int32(0)}, 10: {nil, int64(-1), int64(0)}, 11: {nil, now},
		}
		random := rand.New(rand.NewPCG(23, 41))
		rows := make([][]any, 4096)
		for i := range rows {
			rows[i] = base(i)
			for c := 2; c <= 11; c++ {
				values := choices[c]
				rows[i][c] = values[random.IntN(len(values))]
			}
		}
		// Also vary one field at a time, preserving otherwise valid row shapes.
		for c := 2; c <= 11; c++ {
			for _, v := range choices[c] {
				row := base(len(rows))
				row[c] = v
				rows = append(rows, row)
			}
		}
		replace(t, rows)
		compare(t, false, 1, 100, 1000)
	})
	t.Run("scan-errors-remain-errors", func(t *testing.T) {
		for _, c := range []int{0, 1, 2, 3, 12} {
			row := base(1)
			row[c] = nil
			replace(t, [][]any{row})
			compare(t, true, 100)
		}
		for _, c := range []string{"processed_at", "lease_until", "source_timestamp", "available_at"} {
			for _, v := range []string{"infinity", "-infinity"} {
				replace(t, [][]any{base(1)})
				if _, err := pool.Exec(ctx, "UPDATE emitlane.inbox_events SET "+c+"='"+v+"'"); err != nil {
					t.Fatal(err)
				}
				compare(t, true, 100)
			}
		}
	})
	t.Run("repeatable-read-snapshot", func(t *testing.T) {
		replace(t, [][]any{base(1)})
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "SELECT COUNT(*) FROM emitlane.inbox_events"); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "UPDATE emitlane.inbox_events SET attempts=-1"); err != nil {
			t.Fatal(err)
		}
		got := newAccumulator(ModeFull, 100, now)
		if err := checkInbox(ctx, tx, got, now, DefaultConfig(), ModeFull); err != nil {
			t.Fatal(err)
		}
		if got.report.Summary.InboxProcessed != 1 || got.report.Summary.Violations != 0 {
			t.Fatalf("snapshot changed: %+v", got.report)
		}
	})
	t.Run("statement-and-caller-timeouts", func(t *testing.T) {
		replace(t, nil)
		locker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = locker.Rollback(ctx) }()
		if _, err := locker.Exec(ctx, "LOCK TABLE emitlane.inbox_events IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		for _, server := range []bool{true, false} {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			timeout := "30s"
			if server {
				timeout = "30ms"
			}
			if _, err := tx.Exec(ctx, "SELECT set_config('statement_timeout',$1,true)", timeout); err != nil {
				t.Fatal(err)
			}
			callCtx, stop := context.WithTimeout(ctx, time.Second)
			if !server {
				stop()
				callCtx, stop = context.WithTimeout(ctx, 30*time.Millisecond)
			}
			acc := newAccumulator(ModeFull, 100, now)
			err = checkInbox(callCtx, tx, acc, now, DefaultConfig(), ModeFull)
			stop()
			_ = tx.Rollback(ctx)
			if server {
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
					t.Fatalf("statement timeout lost: %v", err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("caller timeout lost: %v", err)
			}
		}
	})
}

// Frozen 2c049cc reference: full ordered transfer and original Go validation.
// Keep independent of candidate SQL and summary aggregation above.
func legacyCheckInbox(ctx context.Context, tx pgx.Tx, acc *accumulator, now time.Time, _ Config, _ Mode) error {
	rows, err := tx.Query(ctx, `
SELECT consumer, event_id::TEXT, status, attempts, processed_at,
       lease_owner, lease_token::TEXT, lease_until,
       source_topic, source_partition, source_offset, source_timestamp,
       available_at
FROM emitlane.inbox_events
ORDER BY consumer, event_id`)
	if err != nil {
		return fmt.Errorf("integrity: inspect Inbox lifecycle: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var consumer, eventID, status string
		var attempts int
		var processedAt, leaseUntil, sourceTimestamp *time.Time
		var leaseOwner, leaseToken, sourceTopic *string
		var sourcePartition *int32
		var sourceOffset *int64
		var availableAt time.Time
		if err := rows.Scan(&consumer, &eventID, &status, &attempts, &processedAt,
			&leaseOwner, &leaseToken, &leaseUntil, &sourceTopic, &sourcePartition,
			&sourceOffset, &sourceTimestamp, &availableAt); err != nil {
			return fmt.Errorf("integrity: scan Inbox lifecycle: %w", err)
		}
		switch status {
		case "pending":
			acc.report.Summary.InboxPending++
		case "inflight":
			acc.report.Summary.InboxInflight++
		case "retry_wait":
			acc.report.Summary.InboxRetryWait++
			acc.add(Finding{Code: CodeInboxRetryWait, Severity: SeverityWarning,
				Message: "Inbox event is waiting for retry", Consumer: consumer, EventID: eventID,
				SourcePartition: sourcePartition, Observed: availableAt.UTC()})
		case "processed":
			acc.report.Summary.InboxProcessed++
		case "dead":
			acc.report.Summary.InboxDead++
			acc.add(Finding{Code: CodeInboxDeadBlocked, Severity: SeverityWarning,
				Message: "dead Inbox event blocks its Kafka partition", Consumer: consumer,
				EventID: eventID, SourcePartition: sourcePartition})
		default:
			acc.add(Finding{Code: CodeInboxStateInvalid, Severity: SeverityViolation,
				Message: "Inbox lifecycle status is invalid", Consumer: consumer,
				EventID: eventID, Expected: "pending|inflight|retry_wait|processed|dead", Observed: status})
		}
		if attempts < 0 {
			acc.add(Finding{Code: CodeInboxAttemptsInvalid, Severity: SeverityViolation,
				Message: "Inbox attempt count is negative", Consumer: consumer, EventID: eventID,
				Expected: ">= 0", Observed: attempts})
		}
		leaseFields := 0
		if leaseOwner != nil && strings.TrimSpace(*leaseOwner) != "" {
			leaseFields++
		}
		if leaseToken != nil && strings.TrimSpace(*leaseToken) != "" {
			leaseFields++
		}
		if leaseUntil != nil {
			leaseFields++
		}
		if status == "inflight" && leaseFields != 3 || status != "inflight" && leaseFields != 0 {
			acc.add(Finding{Code: CodeInboxLeaseShapeInvalid, Severity: SeverityViolation,
				Message: "Inbox lease fields form an invalid shape", Consumer: consumer, EventID: eventID})
		}
		if status == "inflight" && leaseUntil != nil && !leaseUntil.After(now) {
			acc.report.Summary.InboxStaleLeases++
			acc.add(Finding{Code: CodeInboxStaleLease, Severity: SeverityWarning,
				Message: "Inbox lease expired and is reclaimable", Consumer: consumer,
				EventID: eventID, SourcePartition: sourcePartition, Observed: leaseUntil.UTC()})
		}
		if (status == "processed") != (processedAt != nil) {
			acc.add(Finding{Code: CodeInboxProcessedTimestampInvalid, Severity: SeverityViolation,
				Message: "Inbox processed timestamp does not match status", Consumer: consumer, EventID: eventID})
		}
		sourceFields := 0
		if sourceTopic != nil && strings.TrimSpace(*sourceTopic) != "" {
			sourceFields++
		}
		if sourcePartition != nil {
			sourceFields++
		}
		if sourceOffset != nil {
			sourceFields++
		}
		sourceInvalid := sourceFields != 0 && sourceFields != 3 ||
			sourcePartition != nil && *sourcePartition < 0 || sourceOffset != nil && *sourceOffset < 0 ||
			sourceTimestamp != nil && sourceFields != 3 || status != "processed" && sourceFields != 3
		if sourceInvalid {
			acc.add(Finding{Code: CodeInboxSourceMetadataInvalid, Severity: SeverityViolation,
				Message: "Inbox Kafka source metadata forms an invalid shape", Consumer: consumer,
				EventID: eventID, SourcePartition: sourcePartition})
		}
	}
	return rows.Err()
}
