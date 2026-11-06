package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/emitlane/emitlane/outbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestReconcileCommitPostgreSQL(t *testing.T) {
	runID := os.Getenv("EMITLANE_SOAK_RECONCILE_RUN_ID")
	if runID == "" {
		t.Skip("set EMITLANE_SOAK_RECONCILE_RUN_ID to opt into a private PostgreSQL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pg, err := startSoakPostgres(ctx, map[string]string{
		"com.emitlane.soak.run_id": runID, "com.emitlane.soak.owned": "true",
	})
	if pg != nil {
		t.Cleanup(func() {
			if err := testcontainers.TerminateContainer(pg); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pgstore.MigrateUp(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var version string
	if err := pool.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("PostgreSQL %s", version)

	for _, name := range []string{"commit_response_lost", "commit_canceled"} {
		t.Run(name, func(t *testing.T) {
			r := &soakRuntime{env: &soakEnvironment{pool: pool}, verifier: newVerifier()}
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			id := uuid.New()
			meta := expectedEvent{topic: "soak-reconcile", committedAt: time.Now()}
			if _, err := outbox.NewWriter().Enqueue(ctx, tx, outbox.Event{
				ID: id.String(), Destination: meta.topic, Type: "soak.test", Payload: []byte("{}"),
			}); err != nil {
				t.Fatal(err)
			}

			wantCommitted := name == "commit_response_lost"
			var commitErr error
			if wantCommitted {
				waitCtx, stop := context.WithTimeout(ctx, time.Second)
				pending := &uncertainCommit{id: id, backendPID: tx.Conn().PgConn().PID(), event: meta}
				err := r.reconcileCommit(waitCtx, pending)
				stop()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("unsettled PostgreSQL transaction: %v", err)
				}
				commitErr = r.commitEvent(ctx, commitWithoutResponse{tx}, id, meta)
			} else {
				commitCtx, stop := context.WithCancel(ctx)
				stop()
				commitErr = r.commitEvent(commitCtx, tx, id, meta)
			}

			var pending *uncertainCommit
			if !errors.As(commitErr, &pending) || pending.id != id {
				t.Fatalf("expected an uncertain commit for %s: %v", id, commitErr)
			}
			if _, found := r.verifier.expected[id]; found {
				t.Fatal("registered before reconciliation")
			}
			if err := r.reconcileCommit(ctx, pending); err != nil {
				t.Fatal(err)
			}
			got, found := r.verifier.expected[id]
			if found != wantCommitted || found && got != meta {
				t.Fatalf("event registered=%t, want %t with original metadata", found, wantCommitted)
			}
		})
	}
}

// Perform the real commit, then simulate losing the successful response.
type commitWithoutResponse struct{ pgx.Tx }

func (tx commitWithoutResponse) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	return context.DeadlineExceeded
}
