//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/outbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

// Restore into a new database, never over the source. This is a logical backup
// drill for retained PostgreSQL state, not cross-system point-in-time recovery.
func TestLogicalBackupRestorePreservesDeliveryState(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	source := migrationCompatibilityDatabase(t, ctx, e)
	restored := migrationCompatibilityDatabase(t, ctx, e)
	migrationCompatibilityPrefix(t, ctx, source, 4)
	migrationCompatibilitySeed(t, ctx, source)
	if _, err := source.Exec(ctx, `CREATE TABLE public.restore_orders (id UUID PRIMARY KEY, amount INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	tx, err := source.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `INSERT INTO public.restore_orders VALUES ($1, 42)`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.NewWriter().Enqueue(ctx, tx, outbox.Event{
		ID: id.String(), Destination: "restore.orders", Type: "order.created",
		Payload: []byte{0, 255, 128, 42}, OrderingKey: "restore-order", Sequence: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"pending", "retry_wait", "dead"} {
		if _, err := source.Exec(ctx, `INSERT INTO emitlane.inbox_events
(consumer, event_id, status, processed_at, attempts, source_topic, source_partition, source_offset, last_error)
VALUES ('restore-consumer', $1, $2, NULL, 2, 'restore.orders', 0, $3, 'preserve retry context')`, uuid.New(), status, int64(len(status))); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := migrationCompatibilitySnapshot(t, ctx, source)
	var orderingBefore string
	if err := source.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(s) ORDER BY destination, ordering_key)::text FROM emitlane.ordering_streams s`).Scan(&orderingBefore); err != nil {
		t.Fatal(err)
	}
	dumpPath := "/tmp/emitlane-restore-" + uuid.NewString() + ".dump"
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		code, _, err := e.postgres.Exec(cleanupCtx, []string{"rm", "-f", dumpPath})
		if err != nil || code != 0 {
			t.Errorf("remove test-owned backup: exit=%d err=%v", code, err)
		}
	})
	run := func(args ...string) {
		t.Helper()
		code, output, err := e.postgres.Exec(ctx, args)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(output)
		if err != nil || code != 0 {
			t.Fatalf("%s: exit=%d err=%v output=%s", args[0], code, err, body)
		}
	}
	run("pg_dump", "--username=emitlane", "--dbname="+source.Config().ConnConfig.Database,
		"--format=custom", "--no-owner", "--no-acl", "--file="+dumpPath)
	run("pg_restore", "--username=emitlane", "--dbname="+restored.Config().ConnConfig.Database,
		"--exit-on-error", "--single-transaction", "--no-owner", "--no-acl", dumpPath)
	if after := migrationCompatibilitySnapshot(t, ctx, restored); after != snapshot {
		t.Fatalf("restore changed retained Outbox/Inbox/history/constraints\nbefore: %s\nafter: %s", snapshot, after)
	}
	var orderingAfter string
	if err := restored.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(s) ORDER BY destination, ordering_key)::text FROM emitlane.ordering_streams s`).Scan(&orderingAfter); err != nil {
		t.Fatal(err)
	}
	if orderingBefore != orderingAfter {
		t.Fatal("restore changed ordered stream cursors")
	}
	var atomicPair bool
	if err := restored.QueryRow(ctx, `SELECT b.amount=42 AND o.status='pending' AND o.payload=$2
FROM public.restore_orders b JOIN emitlane.outbox_events o USING (id) WHERE b.id=$1`, id, []byte{0, 255, 128, 42}).Scan(&atomicPair); err != nil || !atomicPair {
		t.Fatalf("restore lost business/outbox atomic pair: %v", err)
	}
	if err := pgstore.MigrateUp(ctx, restored); err != nil {
		t.Fatal(fmt.Errorf("migrate restored current schema: %w", err))
	}
	if after := migrationCompatibilitySnapshot(t, ctx, restored); after != snapshot {
		t.Fatal("repeated migration changed restored retained state")
	}
}
