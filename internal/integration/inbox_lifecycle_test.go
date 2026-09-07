//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/emitlane/emitlane/inbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestInboxLifecycleClaimProcessAndDuplicate(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.New()
	req := inbox.ClaimRequest{
		Consumer: "billing-v1", EventID: eventID,
		Source:     inbox.Source{Topic: "orders", Partition: 1, Offset: 7, Timestamp: time.Now()},
		LeaseOwner: "consumer-a", LeaseDuration: time.Second,
	}
	claim, err := store.Claim(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Disposition != inbox.Claimed || claim.Event.Attempts != 1 || claim.Event.LeaseToken == uuid.Nil {
		t.Fatalf("unexpected claim: %+v", claim)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 42)`, eventID.String()); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := store.MarkProcessed(ctx, tx, req.Consumer, eventID, claim.Event.LeaseToken); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	req.Source.Offset++
	duplicate, err := store.Claim(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Disposition != inbox.AlreadyProcessed || duplicate.Event.Attempts != 1 {
		t.Fatalf("duplicate claim: %+v", duplicate)
	}
}

func TestInboxLifecycleStaleTokenRollsBackBusinessTransaction(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.New()
	req := inbox.ClaimRequest{
		Consumer: "fenced-v1", EventID: eventID,
		Source:     inbox.Source{Topic: "orders", Partition: 0, Offset: 19},
		LeaseOwner: "attempt-a", LeaseDuration: 40 * time.Millisecond,
	}
	first, err := store.Claim(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	txA, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txA.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, eventID.String()+"-a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	req.LeaseOwner = "attempt-b"
	second, err := store.Claim(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Disposition != inbox.Claimed || second.Event.Attempts != 2 || second.Event.LeaseToken == first.Event.LeaseToken {
		t.Fatalf("reclaim: first=%+v second=%+v", first, second)
	}
	if err := store.MarkProcessed(ctx, txA, req.Consumer, eventID, first.Event.LeaseToken); !errors.Is(err, inbox.ErrLeaseLost) {
		t.Fatalf("stale mark error = %v", err)
	}
	if err := txA.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	txB, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txB.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 2)`, eventID.String()+"-b"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProcessed(ctx, txB, req.Consumer, eventID, second.Event.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_payments WHERE order_id LIKE $1`, eventID.String()+"-%").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("protected effects = %d, want 1", count)
	}
}

func TestInboxLifecycleRetryDeadAndSourceConflict(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	req := inbox.ClaimRequest{
		Consumer: "retry-v1", EventID: uuid.New(),
		Source:     inbox.Source{Topic: "orders", Partition: 2, Offset: 3},
		LeaseOwner: "attempt-a", LeaseDuration: time.Second,
	}
	first, err := store.Claim(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRetry(ctx, req.Consumer, req.EventID, first.Event.LeaseToken, 50*time.Millisecond, "temporary"); err != nil {
		t.Fatal(err)
	}
	waiting, err := store.Claim(ctx, req)
	if err != nil || waiting.Disposition != inbox.ClaimWaiting {
		t.Fatalf("retry wait claim=%+v err=%v", waiting, err)
	}
	time.Sleep(70 * time.Millisecond)
	second, err := store.Claim(ctx, req)
	if err != nil || second.Disposition != inbox.Claimed || second.Event.Attempts != 2 {
		t.Fatalf("retry claim=%+v err=%v", second, err)
	}
	if err := store.MarkDead(ctx, req.Consumer, req.EventID, second.Event.LeaseToken, "permanent"); err != nil {
		t.Fatal(err)
	}
	dead, err := store.Claim(ctx, req)
	if err != nil || dead.Disposition != inbox.ClaimDead {
		t.Fatalf("dead claim=%+v err=%v", dead, err)
	}
	conflict := req
	conflict.EventID = uuid.New()
	if _, err := store.Claim(ctx, conflict); !errors.Is(err, inbox.ErrLifecycleConflict) {
		t.Fatalf("source conflict error = %v", err)
	}
}

func TestLegacyInboxOnSchemaV4AndManagedConflict(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	legacyID := uuid.New()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := inbox.Process(ctx, tx, "legacy-v1", legacyID.String(), func(context.Context, pgx.Tx) error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("legacy callback did not run")
	}
	var status string
	if err := e.pool.QueryRow(ctx, `SELECT status FROM emitlane.inbox_events WHERE consumer='legacy-v1' AND event_id=$1`, legacyID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "processed" {
		t.Fatalf("legacy marker status = %s", status)
	}

	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	managedID := uuid.New()
	_, err = store.Claim(ctx, inbox.ClaimRequest{
		Consumer: "managed-v1", EventID: managedID,
		Source:     inbox.Source{Topic: "orders", Partition: 0, Offset: 4},
		LeaseOwner: "managed", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = inbox.Process(ctx, tx, "managed-v1", managedID.String(), func(context.Context, pgx.Tx) error {
		t.Fatal("conflicting legacy callback must not run")
		return nil
	})
	_ = tx.Rollback(ctx)
	if !errors.Is(err, inbox.ErrLifecycleConflict) {
		t.Fatalf("legacy managed conflict = %v", err)
	}
}
