//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/integrity"
	"github.com/emitlane/emitlane/outbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func unorderedIndexData(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
 'outbox',(SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM emitlane.outbox_events e),
 'inbox',(SELECT jsonb_agg(to_jsonb(e) ORDER BY consumer,event_id) FROM emitlane.inbox_events e),
 'streams',(SELECT jsonb_agg(to_jsonb(s) ORDER BY destination,ordering_key) FROM emitlane.ordering_streams s),
 'partitions',(SELECT jsonb_agg(to_jsonb(p) ORDER BY partition_id) FROM emitlane.ordering_partitions p)
)::text`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestUnorderedPendingIndexMigrationPreservesState(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := migrationCompatibilityDatabase(t, ctx, e)
	migrationCompatibilityPrefix(t, ctx, pool, 4)
	migrationCompatibilitySeed(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
INSERT INTO emitlane.outbox_events
(id,destination,event_type,payload,status,lease_owner,lease_until,delivered_at)
SELECT md5('unordered-index:'||status)::uuid,'index-test','index-test',$1,status,
 CASE WHEN status='inflight' THEN 'unfinished-relay' END,
 CASE WHEN status='inflight' THEN NOW()+INTERVAL '1 minute' END,
 CASE WHEN status='delivered' THEN NOW() END
FROM (VALUES ('pending'),('inflight'),('dead'),('delivered')) AS states(status);
`, []byte("preserved-payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO emitlane.inbox_events
(consumer,event_id,status,processed_at,source_topic,source_partition,source_offset)
VALUES ('index-test',$1,'pending',NULL,'index-test-source',0,0)`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	for sequence := int64(1); sequence <= 2; sequence++ {
		if _, err := e.writer.Enqueue(ctx, tx, outbox.Event{
			Destination: "index-test", Type: "ordered", OrderingKey: "stream",
			Sequence: sequence, Payload: []byte("ordered-payload"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	before := unorderedIndexData(t, ctx, pool)
	if err := pgstore.MigrateUp(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if after := unorderedIndexData(t, ctx, pool); after != before {
		t.Fatal("index installation changed durable event, Inbox, cursor or lease state")
	}
	var valid bool
	var keys []string
	var predicate string
	if err := pool.QueryRow(ctx, `SELECT i.indisvalid AND i.indisready,
 ARRAY(SELECT pg_get_indexdef(i.indexrelid,k,TRUE) FROM generate_series(1,i.indnkeyatts) AS k),
 pg_get_expr(i.indpred,i.indrelid)
FROM pg_index i WHERE i.indexrelid='emitlane.outbox_unordered_pending_idx'::regclass`).Scan(&valid, &keys, &predicate); err != nil {
		t.Fatal(err)
	}
	if !valid || fmt.Sprint(keys) != "[available_at created_at id]" ||
		!strings.Contains(predicate, "status = 'pending'::text") || !strings.Contains(predicate, "ordering_key IS NULL") {
		t.Fatalf("wrong unordered pending index: valid=%t keys=%v predicate=%s", valid, keys, predicate)
	}
	// Evaluate the catalog predicate against mixed statuses and ordered rows.
	// It must select the legacy seed's and the new fixture's pending unordered events.
	var indexedRows int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM emitlane.outbox_events WHERE "+predicate).Scan(&indexedRows); err != nil || indexedRows != 2 {
		t.Fatalf("index predicate selected %d rows, want 2: %v", indexedRows, err)
	}
	if err := pgstore.MigrateDown(ctx, pool); err != nil {
		t.Fatal(err)
	}
	version, err := pgstore.SchemaVersion(ctx, pool)
	if err != nil || version != 4 {
		t.Fatalf("index-only rollback returned schema %d, want 4: %v", version, err)
	}
	for index, want := range map[string]bool{"outbox_unordered_pending_idx": false, "outbox_pending_idx": true} {
		exists, err := pgstore.IndexExists(ctx, pool, index)
		if err != nil || exists != want {
			t.Fatalf("rollback index %s exists=%t, want %t: %v", index, exists, want, err)
		}
	}
	if after := unorderedIndexData(t, ctx, pool); after != before {
		t.Fatal("index-only rollback changed unfinished work, cursor or lease state")
	}
	var pgErr *pgconn.PgError
	if err := pgstore.MigrateDown(ctx, pool); !errors.As(err, &pgErr) || pgErr.Code != "55000" {
		t.Fatalf("schema 4 still must protect pending Inbox state: %v", err)
	}
	if err := pgstore.MigrateUp(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if after := unorderedIndexData(t, ctx, pool); after != before {
		t.Fatal("reinstalling the index changed durable state")
	}
}

func TestIntegrityRequiresUnorderedPendingIndex(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := migrationCompatibilityDatabase(t, ctx, e)
	if err := pgstore.MigrateUp(ctx, pool); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil || report.FindingsTruncated || len(report.Findings) != 0 {
		t.Fatalf("fresh current schema failed full integrity: %+v err=%v", report, err)
	}
	if _, err := pool.Exec(ctx, `DROP INDEX emitlane.outbox_unordered_pending_idx`); err != nil {
		t.Fatal(err)
	}
	report, err = verifier.Check(ctx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range report.Findings {
		if finding.Code == integrity.CodeSchemaVersionMismatch && finding.Expected == "outbox_unordered_pending_idx" && finding.Severity == integrity.SeverityViolation {
			return
		}
	}
	t.Fatalf("missing unordered pending index was not a schema violation: %+v", report)
}
