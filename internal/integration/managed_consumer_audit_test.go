//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	managed "github.com/emitlane/emitlane/consumer"
)

type soakPayload struct {
	Seed     string `json:"seed"`
	Sequence int64  `json:"sequence"`
}

// The producer's business transaction is an independent source of expected IDs
// and amounts. Equal counts alone cannot detect substituted IDs or wrong effects.
func auditManagedSoakDatabase(ctx context.Context, pool *pgxpool.Pool, topic, consumer string) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `
WITH expected AS (
    SELECT substring(id FROM 6) AS id, amount FROM public.business_orders
), published AS (
    SELECT id::text AS id, status FROM emitlane.outbox_events WHERE destination=$1
), processed AS (
    SELECT event_id::text AS id, status FROM emitlane.inbox_events WHERE consumer=$2
)
SELECT COALESCE(e.id, o.id, i.id, b.order_id), e.amount, o.status, i.status, b.amount
FROM expected e
FULL JOIN published o USING (id)
FULL JOIN processed i USING (id)
FULL JOIN public.business_payments b ON b.order_id=COALESCE(e.id, o.id, i.id)`, topic, consumer)
	if err != nil {
		return nil, fmt.Errorf("soak database audit: %w", err)
	}
	defer rows.Close()
	expected := make(map[string]int64)
	for rows.Next() {
		var id string
		var amount, effect *int64
		var outboxStatus, inboxStatus *string
		if err := rows.Scan(&id, &amount, &outboxStatus, &inboxStatus, &effect); err != nil {
			return nil, err
		}
		if amount == nil || effect == nil || *amount != *effect || outboxStatus == nil || *outboxStatus != "delivered" || inboxStatus == nil || *inboxStatus != "processed" {
			return nil, fmt.Errorf("soak database identity/effect mismatch for event %s", id)
		}
		expected[id] = *amount
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("soak database audit has no committed events")
	}
	return expected, nil
}

func auditManagedSoakRecords(records []*kgo.Record, expected map[string]int64, seed string) (int, error) {
	seen := make(map[string]bool, len(expected))
	duplicates := 0
	for _, record := range records {
		message := managed.Message{}
		for _, header := range record.Headers {
			message.Headers = append(message.Headers, managed.Header{Key: header.Key, Value: header.Value})
		}
		id, err := managed.ResolveEventID(message)
		if err != nil {
			return 0, fmt.Errorf("soak Kafka identity: %w", err)
		}
		amount, ok := expected[id.String()]
		if !ok {
			return 0, fmt.Errorf("soak Kafka audit observed unexpected event %s", id)
		}
		var payload soakPayload
		if err := json.Unmarshal(record.Value, &payload); err != nil {
			return 0, fmt.Errorf("soak Kafka payload: %w", err)
		}
		if string(record.Key) != id.String() || payload.Seed != seed || payload.Sequence != amount {
			return 0, fmt.Errorf("soak Kafka key/payload mismatch for event %s", id)
		}
		if seen[id.String()] {
			duplicates++
		}
		seen[id.String()] = true
	}
	if len(seen) != len(expected) {
		return 0, fmt.Errorf("soak Kafka audit: observed %d of %d committed event IDs", len(seen), len(expected))
	}
	return duplicates, nil
}

func TestManagedSoakAuditRejectsEqualCountsWithWrongIdentity(t *testing.T) {
	const id = "10000000-0000-0000-0000-000000000001"
	const wrongID = "10000000-0000-0000-0000-000000000002"
	makeRecord := func(eventID string, sequence int64) *kgo.Record {
		payload, err := json.Marshal(soakPayload{Seed: "test", Sequence: sequence})
		if err != nil {
			t.Fatal(err)
		}
		return &kgo.Record{Key: []byte(eventID), Value: payload, Headers: []kgo.RecordHeader{{Key: managed.EventIDHeader, Value: []byte(eventID)}}}
	}
	expected := map[string]int64{id: 42}
	if _, err := auditManagedSoakRecords([]*kgo.Record{makeRecord(wrongID, 42)}, expected, "test"); err == nil {
		t.Fatal("equal record counts concealed a substituted identity")
	}
	if _, err := auditManagedSoakRecords([]*kgo.Record{makeRecord(id, 43)}, expected, "test"); err == nil {
		t.Fatal("matching identity concealed corrupted payload")
	}
	if _, err := auditManagedSoakRecords(nil, expected, "test"); err == nil {
		t.Fatal("missing committed identity was accepted")
	}
	record := makeRecord(id, 42)
	if duplicates, err := auditManagedSoakRecords([]*kgo.Record{record, record}, expected, "test"); err != nil || duplicates != 1 {
		t.Fatalf("expected at-least-once duplicate to be reported: duplicates=%d err=%v", duplicates, err)
	}
}

func TestManagedSoakDatabaseAuditRejectsSubstitutedEffects(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := uuid.NewString()
	if _, err := e.pool.Exec(ctx, `
INSERT INTO public.business_orders (id, amount) VALUES ('soak-' || $1, 42);
`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.outbox_events
(id, destination, event_type, payload, status, delivered_at)
VALUES ($1, 'audit-test', 'audit.input', ''::bytea, 'delivered', NOW())`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events
(consumer, event_id, processed_at) VALUES ('audit-test', $1, NOW())`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO public.business_payments VALUES ($1, 42)`, id); err != nil {
		t.Fatal(err)
	}
	if expected, err := auditManagedSoakDatabase(ctx, e.pool, "audit-test", "audit-test"); err != nil || expected[id] != 42 {
		t.Fatalf("valid committed state rejected: expected=%v err=%v", expected, err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE public.business_payments SET amount=43 WHERE order_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := auditManagedSoakDatabase(ctx, e.pool, "audit-test", "audit-test"); err == nil {
		t.Fatal("equal row counts concealed the wrong business effect")
	}
	if _, err := e.pool.Exec(ctx, `UPDATE public.business_payments SET amount=42, order_id=$2 WHERE order_id=$1`, id, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := auditManagedSoakDatabase(ctx, e.pool, "audit-test", "audit-test"); err == nil {
		t.Fatal("equal row counts concealed a substituted business identity")
	}
}
