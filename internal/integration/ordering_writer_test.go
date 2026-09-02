//go:build integration

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/emitlane/emitlane/outbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestOrderedWriterInitializesStreamAndKeyAffinity(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.writer.Enqueue(ctx, tx, outbox.Event{
		Destination:           "orders.ordered",
		Type:                  "order.adopted",
		OrderingKey:           "order:writer",
		Sequence:              51,
		OrderingStartSequence: 51,
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var key []byte
	var sequence int64
	var partition int16
	if err := e.pool.QueryRow(ctx, `
SELECT message_key, ordering_sequence, ordering_partition
FROM emitlane.outbox_events WHERE id = $1`, id).Scan(&key, &sequence, &partition); err != nil {
		t.Fatal(err)
	}
	if string(key) != "order:writer" || sequence != 51 || partition < 0 || partition >= 64 {
		t.Fatalf("ordered row key=%q sequence=%d partition=%d", key, sequence, partition)
	}
	var start, next int64
	if err := e.pool.QueryRow(ctx, `
SELECT start_sequence, next_sequence
FROM emitlane.ordering_streams
WHERE destination='orders.ordered' AND ordering_key='order:writer'`).Scan(&start, &next); err != nil {
		t.Fatal(err)
	}
	if start != 51 || next != 51 {
		t.Fatalf("stream start=%d next=%d", start, next)
	}
}

func TestOrderedWriterRollbackIsAtomic(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.Enqueue(ctx, tx, outbox.Event{
		Destination: "orders.rollback", Type: "order.created",
		OrderingKey: "order:rollback", Sequence: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := e.pool.QueryRow(ctx, `
SELECT COUNT(*) FROM emitlane.ordering_streams
WHERE destination='orders.rollback' AND ordering_key='order:rollback'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("rolled-back enqueue left stream state")
	}
}

func TestOrderedWriterRejectsDuplicateAndPassedSequence(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	enqueue := func() error {
		tx, err := e.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		_, err = e.writer.Enqueue(ctx, tx, outbox.Event{
			Destination: "orders.duplicate", Type: "order.changed",
			OrderingKey: "order:duplicate", Sequence: 1,
		})
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- enqueue()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	var success, duplicate int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, outbox.ErrDuplicateSequence):
			duplicate++
		default:
			t.Fatalf("unexpected concurrent enqueue error: %v", err)
		}
	}
	if success != 1 || duplicate != 1 {
		t.Fatalf("success=%d duplicate=%d", success, duplicate)
	}

	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.ordering_streams SET next_sequence=2
WHERE destination='orders.duplicate' AND ordering_key='order:duplicate'`); err != nil {
		t.Fatal(err)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, err = e.writer.Enqueue(ctx, tx, outbox.Event{
		Destination: "orders.duplicate", Type: "order.old",
		OrderingKey: "order:duplicate", Sequence: 1,
	})
	if !errors.Is(err, outbox.ErrSequenceAlreadyPassed) {
		t.Fatalf("old sequence error = %v", err)
	}
}

func TestSchemaV3SeedsPartitionsAndRefusesUnsafeDown(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var partitions int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.ordering_partitions`).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	if partitions != 64 {
		t.Fatalf("partition seed rows = %d, want 64", partitions)
	}

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.Enqueue(ctx, tx, outbox.Event{
		Destination: "orders.down", Type: "order.created",
		OrderingKey: "order:down", Sequence: 1,
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.MigrateDown(ctx, e.pool); err == nil {
		t.Fatal("unsafe v3 down migration succeeded")
	}
	version, err := pgstore.SchemaVersion(ctx, e.pool)
	if err != nil || version != 3 {
		t.Fatalf("schema version after refused down=%d err=%v", version, err)
	}
}
