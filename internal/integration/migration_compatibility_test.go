//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/migrations"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestMigrationRejectsIncompatibleHistoryWithoutMutation(t *testing.T) {
	e := startEnv(t)
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"future-version", `INSERT INTO emitlane.schema_migrations (version) VALUES (5)`},
		{"missing-version", `DELETE FROM emitlane.schema_migrations WHERE version = 2`},
		{"zero-version", `INSERT INTO emitlane.schema_migrations (version) VALUES (0)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pool := migrationCompatibilityDatabase(t, ctx, e)
			migrationCompatibilityPrefix(t, ctx, pool, 4)
			migrationCompatibilitySeed(t, ctx, pool)
			if _, err := pool.Exec(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			before := migrationCompatibilitySnapshot(t, ctx, pool)
			for _, operation := range []struct {
				name string
				run  func() error
			}{
				{"up", func() error { return pgstore.MigrateUp(ctx, pool) }},
				{"down", func() error { return pgstore.MigrateDown(ctx, pool) }},
				{"version", func() error { _, err := pgstore.SchemaVersion(ctx, pool); return err }},
			} {
				if err := operation.run(); !errors.Is(err, pgstore.ErrSchemaIncompatible) {
					t.Errorf("%s: expected ErrSchemaIncompatible, got %v", operation.name, err)
				}
				if after := migrationCompatibilitySnapshot(t, ctx, pool); after != before {
					t.Fatalf("%s mutated incompatible database\nbefore: %s\nafter: %s", operation.name, before, after)
				}
			}
		})
	}
}

func TestMigrationUpgradePreservesReleasedSchemaData(t *testing.T) {
	e := startEnv(t)
	for prefix := 1; prefix <= 4; prefix++ {
		t.Run(fmt.Sprintf("schema-%d", prefix), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pool := migrationCompatibilityDatabase(t, ctx, e)
			migrationCompatibilityPrefix(t, ctx, pool, prefix)
			migrationCompatibilitySeed(t, ctx, pool)
			before := migrationCompatibilityLegacyData(t, ctx, pool)
			if err := pgstore.MigrateUp(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if after := migrationCompatibilityLegacyData(t, ctx, pool); after != before {
				t.Fatalf("upgrade changed existing payload, lifecycle or deduplication data\nbefore: %s\nafter: %s", before, after)
			}
			version, err := pgstore.SchemaVersion(ctx, pool)
			if err != nil || version != pgstore.CurrentSchemaVersion() {
				t.Fatalf("schema version = %d, %v; want %d", version, err, pgstore.CurrentSchemaVersion())
			}
			var legacyProcessed bool
			if err := pool.QueryRow(ctx, `SELECT status = 'processed' AND attempts = 0
AND source_topic IS NULL AND source_partition IS NULL AND source_offset IS NULL
FROM emitlane.inbox_events WHERE consumer = 'legacy-consumer'`).Scan(&legacyProcessed); err != nil {
				t.Fatal(err)
			}
			if !legacyProcessed {
				t.Fatal("legacy Inbox row did not retain processed lifecycle without fabricated source metadata")
			}
			// The legacy helper must still suppress a duplicate after migration.
			tag, err := pool.Exec(ctx, `INSERT INTO emitlane.inbox_events (consumer, event_id)
SELECT consumer, event_id FROM emitlane.inbox_events WHERE consumer = 'legacy-consumer'
ON CONFLICT (consumer, event_id) DO NOTHING`)
			if err != nil || tag.RowsAffected() != 0 {
				t.Fatalf("legacy duplicate insert affected %d rows: %v", tag.RowsAffected(), err)
			}
			for _, index := range pgstore.RequiredIndexes() {
				exists, err := pgstore.IndexExists(ctx, pool, index)
				if err != nil || !exists {
					t.Fatalf("required index %s missing after upgrade: %v", index, err)
				}
			}
			beforeRepeat := migrationCompatibilitySnapshot(t, ctx, pool)
			if err := pgstore.MigrateUp(ctx, pool); err != nil {
				t.Fatalf("repeat migration: %v", err)
			}
			if after := migrationCompatibilitySnapshot(t, ctx, pool); after != beforeRepeat {
				t.Fatalf("repeat migration was not idempotent\nbefore: %s\nafter: %s", beforeRepeat, after)
			}
		})
	}
}

func TestMigrationDowngradePreservesUnfinishedManagedInbox(t *testing.T) {
	e := startEnv(t)
	for _, status := range []string{"pending", "inflight", "retry_wait", "dead"} {
		t.Run(status, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pool := migrationCompatibilityDatabase(t, ctx, e)
			migrationCompatibilityPrefix(t, ctx, pool, 4)
			migrationCompatibilitySeed(t, ctx, pool)
			if _, err := pool.Exec(ctx, `INSERT INTO emitlane.inbox_events (
consumer, event_id, processed_at, status, attempts, last_error,
source_topic, source_partition, source_offset, lease_owner, lease_token, lease_until
) VALUES ('managed-consumer', $1, NULL, $2, 3, 'preserve failure context',
'orders', 2, 41,
CASE WHEN $2 = 'inflight' THEN 'consumer-worker' END,
CASE WHEN $2 = 'inflight' THEN $3::uuid END,
CASE WHEN $2 = 'inflight' THEN NOW() + INTERVAL '1 hour' END)`, uuid.New(), status, uuid.New()); err != nil {
				t.Fatal(err)
			}
			before := migrationCompatibilitySnapshot(t, ctx, pool)
			var pgErr *pgconn.PgError
			if err := pgstore.MigrateDown(ctx, pool); !errors.As(err, &pgErr) || pgErr.Code != "55000" {
				t.Fatalf("expected refusal to discard managed Inbox state (55000), got %v", err)
			}
			if after := migrationCompatibilitySnapshot(t, ctx, pool); after != before {
				t.Fatalf("refused downgrade changed state\nbefore: %s\nafter: %s", before, after)
			}
			version, err := pgstore.SchemaVersion(ctx, pool)
			if err != nil || version != 4 {
				t.Fatalf("schema version after refused downgrade = %d, %v; want 4", version, err)
			}
		})
	}
}

func TestMigrationDowngradeWaitsForConcurrentManagedInboxWrite(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := migrationCompatibilityDatabase(t, ctx, e)
	migrationCompatibilityPrefix(t, ctx, pool, 4)
	migrationCompatibilitySeed(t, ctx, pool)

	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback(context.Background()) }()
	eventID := uuid.New()
	if _, err := writer.Exec(ctx, `INSERT INTO emitlane.inbox_events (
consumer, event_id, processed_at, status, source_topic, source_partition, source_offset
) VALUES ('concurrent-consumer', $1, NULL, 'pending', 'orders', 0, 99)`, eventID); err != nil {
		t.Fatal(err)
	}

	config := pool.Config()
	applicationName := "migration-downgrade-" + uuid.NewString()
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	migrator, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(migrator.Close)
	done := make(chan error, 1)
	go func() { done <- pgstore.MigrateDown(ctx, migrator) }()

	// Observe the actual database lock wait before committing, so scheduling
	// speed cannot accidentally turn this into a sequential downgrade test.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
WHERE a.application_name = $1 AND l.relation = 'emitlane.inbox_events'::regclass
AND l.mode = 'AccessExclusiveLock' AND NOT l.granted
)`, applicationName).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("downgrade completed before waiting for concurrent Inbox writer: %v", err)
		case <-ctx.Done():
			t.Fatalf("downgrade never waited for Inbox writer: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
			t.Fatalf("downgrade must observe newly committed managed state, got %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("downgrade did not finish after writer commit: %v", ctx.Err())
	}
	version, err := pgstore.SchemaVersion(ctx, pool)
	if err != nil || version != 4 {
		t.Fatalf("schema version after concurrent downgrade = %d, %v; want 4", version, err)
	}
	var preserved bool
	if err := pool.QueryRow(ctx, `SELECT status = 'pending' AND processed_at IS NULL
AND source_topic = 'orders' AND source_partition = 0 AND source_offset = 99
FROM emitlane.inbox_events WHERE consumer = 'concurrent-consumer' AND event_id = $1`, eventID).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if !preserved {
		t.Fatal("downgrade changed concurrently committed managed Inbox state")
	}
}

// Each case gets its own database on the existing test container. In particular,
// deliberately corrupt migration histories never enter the shared test schema.
func migrationCompatibilityDatabase(t *testing.T, ctx context.Context, e *env) *pgxpool.Pool {
	t.Helper()
	name := "migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := e.pool.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := e.pool.Exec(cleanupCtx, "DROP DATABASE "+identifier+" WITH (FORCE)"); err != nil {
			t.Errorf("drop isolated migration database: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(e.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Reconstruct a released schema from the published SQL, independently of the
// current MigrateUp implementation under test.
func migrationCompatibilityPrefix(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prefix int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `CREATE SCHEMA emitlane;
CREATE TABLE emitlane.schema_migrations (
version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		t.Fatal(err)
	}
	names, err := fs.Glob(migrations.SQL, "*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			t.Fatal(err)
		}
		if version > prefix {
			continue
		}
		body, err := migrations.SQL.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply released migration %s: %v", name, err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO emitlane.schema_migrations (version) VALUES ($1)`, version); err != nil {
			t.Fatal(err)
		}
	}
}

func migrationCompatibilitySeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, status := range []string{"pending", "inflight", "delivered", "dead"} {
		if _, err := pool.Exec(ctx, `INSERT INTO emitlane.outbox_events (
id, destination, event_type, message_key, payload, headers, status, attempts,
lease_owner, lease_until, last_error, delivered_at
) VALUES ($1, 'orders', 'order.created', $2, $3, '{"migration":"preserve"}', $4, 2,
CASE WHEN $4 = 'inflight' THEN 'relay-worker' END,
CASE WHEN $4 = 'inflight' THEN NOW() + INTERVAL '1 hour' END,
'preserve publish context', CASE WHEN $4 = 'delivered' THEN NOW() END)`,
			uuid.New(), []byte{0, 1, 255}, []byte{0, 255, 128, 1, 42}, status); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO emitlane.inbox_events (consumer, event_id, processed_at)
VALUES ('legacy-consumer', $1, '2025-01-02T03:04:05Z')`, uuid.New()); err != nil {
		t.Fatal(err)
	}
}

// Compare every v1 outbox column, including opaque bytes, lease, and retry data;
// new additive columns are checked separately by full snapshots after upgrade.
func migrationCompatibilityLegacyData(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
'outbox', (SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM (
SELECT id, destination, event_type, message_key, payload, content_type, headers,
schema_version, correlation_id, causation_id, traceparent, tracestate, status,
attempts, available_at, lease_owner, lease_until, last_error, created_at, delivered_at
FROM emitlane.outbox_events) o),
'inbox', (SELECT jsonb_agg(to_jsonb(i) ORDER BY consumer, event_id) FROM (
SELECT consumer, event_id, processed_at FROM emitlane.inbox_events) i)
)::text`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func migrationCompatibilitySnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
'history', (SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM emitlane.schema_migrations m),
'outbox', (SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM emitlane.outbox_events o),
'inbox', (SELECT jsonb_agg(to_jsonb(i) ORDER BY consumer, event_id) FROM emitlane.inbox_events i),
'columns', (SELECT jsonb_agg(to_jsonb(c) ORDER BY table_name, ordinal_position)
FROM information_schema.columns c WHERE table_schema = 'emitlane'),
'indexes', (SELECT jsonb_agg(to_jsonb(ix) ORDER BY indexname)
FROM pg_indexes ix WHERE schemaname = 'emitlane')
)::text`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
