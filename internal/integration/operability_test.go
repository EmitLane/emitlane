//go:build integration

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/emitlane/emitlane/broker"
	"github.com/emitlane/emitlane/inbox"
	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func adminService(t *testing.T, e *env, staleAfter time.Duration) *adminapi.Service {
	t.Helper()
	service, err := adminapi.NewService(e.store, staleAfter, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestMigrationV1ToV3PreservesReleasedData(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := pgstore.MigrateUp(cleanupCtx, e.pool); err != nil {
			t.Errorf("restore v3 schema: %v", err)
		}
	})

	if err := pgstore.MigrateDown(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.MigrateDown(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	version, err := pgstore.SchemaVersion(ctx, e.pool)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("schema version = %d, want released v1", version)
	}

	pendingID := uuid.New()
	deliveredID := uuid.New()
	deadID := uuid.New()
	for _, row := range []struct {
		id     uuid.UUID
		status string
	}{
		{pendingID, "pending"}, {deliveredID, "delivered"}, {deadID, "dead"},
	} {
		_, err := e.pool.Exec(ctx, `
INSERT INTO emitlane.outbox_events
    (id, destination, event_type, payload, status, delivered_at, last_error)
VALUES ($1, 'migration.events', 'migration.test', $2, $3,
        CASE WHEN $3 = 'delivered' THEN NOW() ELSE NULL END,
        CASE WHEN $3 = 'dead' THEN 'old failure' ELSE NULL END)`, row.id, []byte("v1-payload"), row.status)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events (consumer, event_id) VALUES ('migration-test', $1)`, deliveredID); err != nil {
		t.Fatal(err)
	}

	if err := pgstore.MigrateUp(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	version, err = pgstore.SchemaVersion(ctx, e.pool)
	if err != nil || version != pgstore.CurrentSchemaVersion() {
		t.Fatalf("schema version after upgrade = %d, err=%v", version, err)
	}
	var outboxCount, inboxCount int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.inbox_events`).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 3 || inboxCount != 1 {
		t.Fatalf("upgrade lost data: outbox=%d inbox=%d", outboxCount, inboxCount)
	}
	for _, id := range []uuid.UUID{pendingID, deliveredID, deadID} {
		var from, batch *uuid.UUID
		if err := e.pool.QueryRow(ctx, `SELECT replayed_from_event_id, replay_batch_id FROM emitlane.outbox_events WHERE id = $1`, id).Scan(&from, &batch); err != nil {
			t.Fatal(err)
		}
		if from != nil || batch != nil {
			t.Fatalf("v1 row %s acquired replay provenance", id)
		}
	}
	for _, table := range pgstore.RequiredTables() {
		exists, err := pgstore.TableExists(ctx, e.pool, table)
		if err != nil || !exists {
			t.Fatalf("required v3 table %s: exists=%t err=%v", table, exists, err)
		}
	}
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "migration-relay"}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, pendingID.String(), "delivered", 20*time.Second)
	if status := e.eventStatus(t, deadID.String()); status != "dead" {
		t.Fatalf("pre-existing dead status changed to %s", status)
	}
}

func TestDurablePauseStopsClaimsAcrossRelaysAndResumeRecovers(t *testing.T) {
	e := startEnv(t)
	service := adminService(t, e, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration", Reason: "maintenance"}); err != nil {
		t.Fatal(err)
	}
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-paused", topic, 10, true)
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "paused-a"}, e.publisher(t), relay.FailureHooks{}))
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "paused-b"}, e.publisher(t), relay.FailureHooks{}))
	time.Sleep(500 * time.Millisecond)
	if status := e.eventStatus(t, id); status != "pending" {
		t.Fatalf("event claimed while cluster paused: %s", status)
	}
	if attempts := e.getEvent(t, id).Attempts; attempts != 0 {
		t.Fatalf("paused event started %d publish attempts", attempts)
	}
	claimed, err := e.store.Claim(ctx, "direct-claimer", 1, 5*time.Second)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("atomic claim gate while paused: claimed=%d err=%v", len(claimed), err)
	}
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "integration", Reason: "maintenance complete"}); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(t, id, "delivered", 20*time.Second)
}

type blockingPublisher struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingPublisher) Publish(ctx context.Context, _ broker.Message) error {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*blockingPublisher) Close() error { return nil }

func TestPauseDoesNotBlockInflightCompletion(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := topicName(t, e)
	firstID := enqueueOrder(t, e, "ord-inflight-pause", topic, 11, true)
	publisher := &blockingPublisher{started: make(chan struct{}), release: make(chan struct{})}
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "inflight-owner", Concurrency: 1}, publisher, relay.FailureHooks{}))
	select {
	case <-publisher.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first publish did not start")
	}
	service := adminService(t, e, time.Second)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration"}); err != nil {
		t.Fatal(err)
	}
	secondID := enqueueOrder(t, e, "ord-after-pause", topic, 12, true)
	close(publisher.release)
	e.waitStatus(t, firstID, "delivered", 10*time.Second)
	time.Sleep(250 * time.Millisecond)
	second := e.getEvent(t, secondID)
	if second.Status != "pending" || second.Attempts != 0 {
		t.Fatalf("new work was claimed after pause: status=%s attempts=%d", second.Status, second.Attempts)
	}
}

func TestRelayPresenceActiveStaleAndStopped(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	presence := relay.RelayPresence{InstanceID: "presence-manual", Hostname: "test-host", Version: "v0.2.0", StartedAt: time.Now()}
	if err := e.store.RegisterRelay(ctx, presence); err != nil {
		t.Fatal(err)
	}
	service := adminService(t, e, 150*time.Millisecond)
	instance, err := service.GetRelay(ctx, presence.InstanceID)
	if err != nil || instance.State != "active" {
		t.Fatalf("active presence: state=%s err=%v", instance.State, err)
	}
	time.Sleep(250 * time.Millisecond)
	instance, err = service.GetRelay(ctx, presence.InstanceID)
	if err != nil || instance.State != "stale" {
		t.Fatalf("stale presence: state=%s err=%v", instance.State, err)
	}
	if err := e.store.HeartbeatRelay(ctx, presence.InstanceID); err != nil {
		t.Fatal(err)
	}
	instance, err = service.GetRelay(ctx, presence.InstanceID)
	if err != nil || instance.State != "active" {
		t.Fatalf("heartbeat recovery: state=%s err=%v", instance.State, err)
	}
	if err := e.store.MarkRelayStopped(ctx, presence.InstanceID); err != nil {
		t.Fatal(err)
	}
	instance, err = service.GetRelay(ctx, presence.InstanceID)
	if err != nil || instance.State != "stopped" {
		t.Fatalf("stopped presence: state=%s err=%v", instance.State, err)
	}
}

func TestRelayRunRegistersHeartbeatAndCleanStop(t *testing.T) {
	e := startEnv(t)
	cfg := relay.Config{InstanceID: "presence-running"}
	rly := e.newRelay(t, cfg, &failPublisher{err: errors.New("unused")}, relay.FailureHooks{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rly.Run(ctx) }()
	service := adminService(t, e, 500*time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		instance, err := service.GetRelay(context.Background(), cfg.InstanceID)
		if err == nil && instance.State == "active" {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("relay did not register active presence: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	beforeStop, err := service.GetRelay(context.Background(), cfg.InstanceID)
	if err != nil || time.Since(beforeStop.LastHeartbeatAt) > 300*time.Millisecond {
		cancel()
		t.Fatalf("heartbeat was not refreshed: %+v err=%v", beforeStop, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not stop")
	}
	afterStop, err := service.GetRelay(context.Background(), cfg.InstanceID)
	if err != nil || afterStop.State != "stopped" {
		t.Fatalf("clean stop presence: %+v err=%v", afterStop, err)
	}
}

func TestEventInspectionRedactionAndKeysetPagination(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	ids := make(map[uuid.UUID]struct{})
	for i := 0; i < 5; i++ {
		id := enqueueOrder(t, e, fmt.Sprintf("ord-page-%d", i), topic, i, true)
		ids[uuid.MustParse(id)] = struct{}{}
	}
	service := adminService(t, e, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seen := make(map[uuid.UUID]struct{})
	var cursor *adminapi.Cursor
	for {
		page, err := service.ListEvents(ctx, adminapi.EventFilter{Destination: topic, Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Events {
			if _, exists := seen[event.ID]; exists {
				t.Fatalf("duplicate event across keyset pages: %s", event.ID)
			}
			seen[event.ID] = struct{}{}
			if event.PayloadBase64 != nil || event.Headers != nil || event.KeyBase64 != nil {
				t.Fatal("list exposed sensitive event data")
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor, err = adminapi.DecodeCursor(page.NextCursor)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != len(ids) {
		t.Fatalf("paginated events = %d, want %d", len(seen), len(ids))
	}
	for id := range ids {
		redacted, err := service.InspectEvent(ctx, id, false)
		if err != nil {
			t.Fatal(err)
		}
		if redacted.PayloadBase64 != nil || redacted.Headers != nil {
			t.Fatal("default inspect exposed sensitive data")
		}
		exposed, err := service.InspectEvent(ctx, id, true)
		if err != nil {
			t.Fatal(err)
		}
		if exposed.PayloadBase64 == nil || exposed.Headers["source"] != "test" {
			t.Fatal("explicit inspect did not return base64 payload and headers")
		}
		break
	}
}

func TestRetryDeadIsSameIDAndAudited(t *testing.T) {
	e := startEnv(t)
	id := uuid.MustParse(enqueueOrder(t, e, "ord-retry-audit", topicName(t, e), 12, true))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET status = 'dead', last_error = 'poison' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	service := adminService(t, e, time.Second)
	if err := service.RetryDead(ctx, id, adminapi.Mutation{Actor: "integration", Reason: "fixed"}); err != nil {
		t.Fatal(err)
	}
	event, err := service.InspectEvent(ctx, id, false)
	if err != nil {
		t.Fatal(err)
	}
	if event.ID != id || event.Status != "pending" || event.Attempts != 0 || event.LastError != "" {
		t.Fatalf("retry result: %+v", event)
	}
	page, err := service.ListAudit(ctx, adminapi.AuditFilter{Action: "event.retry", Limit: 10})
	if err != nil || len(page.Records) != 1 || page.Records[0].TargetEventID == nil || *page.Records[0].TargetEventID != id {
		t.Fatalf("retry audit: %+v err=%v", page, err)
	}
	if err := service.RetryDead(ctx, id, adminapi.Mutation{Actor: "integration"}); !errors.Is(err, adminapi.ErrConflict) {
		t.Fatalf("retry pending error = %v, want conflict", err)
	}
}

func TestReplayCreatesUUIDv7CloneWithProvenanceAndHeaders(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	sourceID := uuid.MustParse(enqueueOrder(t, e, "ord-replay", topic, 13, true))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET headers = headers || $2::jsonb WHERE id = $1`, sourceID,
		`{"emitlane-original-event-id":"spoofed","emitlane-replay-batch-id":"spoofed"}`); err != nil {
		t.Fatal(err)
	}
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "replay-relay"}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, sourceID.String(), "delivered", 20*time.Second)
	service := adminService(t, e, time.Second)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration"}); err != nil {
		t.Fatal(err)
	}
	result, err := service.ReplayEvent(ctx, sourceID, adminapi.Mutation{Actor: "integration", Reason: "consumer bug fixed"})
	if err != nil {
		t.Fatal(err)
	}
	cloneID := result.EventIDs[0]
	if cloneID.Version() != 7 {
		t.Fatalf("replay id version = %d, want 7", cloneID.Version())
	}
	clone, err := service.InspectEvent(ctx, cloneID, true)
	if err != nil {
		t.Fatal(err)
	}
	if clone.Status != "pending" || clone.Attempts != 0 || clone.ReplayedFromEventID == nil || *clone.ReplayedFromEventID != sourceID ||
		clone.ReplayBatchID == nil || *clone.ReplayBatchID != result.ReplayBatchID {
		t.Fatalf("replay clone lifecycle/provenance: %+v", clone)
	}
	if clone.PayloadBase64 == nil {
		t.Fatal("clone payload was not exposed")
	}
	if payload, err := base64.StdEncoding.DecodeString(*clone.PayloadBase64); err != nil || len(payload) == 0 {
		t.Fatalf("clone payload: len=%d err=%v", len(payload), err)
	}
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "integration"}); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(t, cloneID.String(), "delivered", 20*time.Second)
	records := consumeRecords(t, e.brokers, topic, 2, 20*time.Second)
	var replayRecordFound bool
	for _, record := range records {
		if headerValue(record, broker.HeaderEventID) == cloneID.String() {
			replayRecordFound = true
			if got := headerValue(record, broker.HeaderOriginalEvent); got != sourceID.String() {
				t.Fatalf("replayed-from header = %q", got)
			}
			if got := headerValue(record, broker.HeaderReplayBatch); got != result.ReplayBatchID.String() {
				t.Fatalf("replay-batch header = %q", got)
			}
		}
	}
	if !replayRecordFound {
		t.Fatal("replayed Kafka record not found")
	}
}

func TestInboxTreatsReplayAsNewIdentity(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sourceID := uuid.MustParse(enqueueOrder(t, e, "ord-inbox-replay", topicName(t, e), 15, true))
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.outbox_events SET status='delivered', delivered_at=NOW() WHERE id=$1`, sourceID); err != nil {
		t.Fatal(err)
	}
	service := adminService(t, e, time.Second)
	replayResult, err := service.ReplayEvent(ctx, sourceID, adminapi.Mutation{Actor: "integration", Reason: "inbox test"})
	if err != nil {
		t.Fatal(err)
	}
	cloneID := replayResult.EventIDs[0]
	callbackCount := 0
	for _, eventID := range []uuid.UUID{sourceID, cloneID} {
		tx, err := e.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = inbox.Process(ctx, tx, "replay-consumer", eventID.String(), func(ctx context.Context, tx pgx.Tx) error {
			callbackCount++
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, eventID.String())
			return err
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if callbackCount != 2 {
		t.Fatalf("Inbox callback count=%d, want original and replay", callbackCount)
	}
}

func TestBatchReplayFiltersAndPreviewDoNotMutate(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Second)
	targetID := uuid.New()
	rows := []struct {
		id          uuid.UUID
		destination string
		eventType   string
		status      string
		createdAt   time.Time
	}{
		{targetID, "selected.events", "selected.type", "delivered", now},
		{uuid.New(), "other.events", "selected.type", "delivered", now},
		{uuid.New(), "selected.events", "other.type", "delivered", now},
		{uuid.New(), "selected.events", "selected.type", "dead", now},
		{uuid.New(), "selected.events", "selected.type", "delivered", now.Add(-2 * time.Hour)},
	}
	for _, row := range rows {
		_, err := e.pool.Exec(ctx, `
INSERT INTO emitlane.outbox_events
    (id, destination, event_type, payload, status, created_at, delivered_at, last_error)
VALUES ($1, $2, $3, $4, $5, $6,
        CASE WHEN $5='delivered' THEN $6::timestamptz ELSE NULL END,
        CASE WHEN $5='dead' THEN 'poison' ELSE NULL END)`,
			row.id, row.destination, row.eventType, []byte("filter-payload"), row.status, row.createdAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	from, to := now.Add(-time.Minute), now.Add(time.Minute)
	filter := adminapi.EventFilter{Destination: "selected.events", EventType: "selected.type", CreatedFrom: &from, CreatedTo: &to}
	service := adminService(t, e, time.Second)
	preview, err := service.PreviewReplay(ctx, filter)
	if err != nil || preview.Count != 1 || len(preview.Sample) != 1 || preview.Sample[0].ID != targetID {
		t.Fatalf("filtered preview=%+v err=%v", preview, err)
	}
	var clones, audits int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE replayed_from_event_id IS NOT NULL`).Scan(&clones); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.admin_audit_log`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if clones != 0 || audits != 0 {
		t.Fatalf("preview mutated state: clones=%d audits=%d", clones, audits)
	}
	result, err := service.ReplayBatch(ctx, filter, adminapi.Mutation{Actor: "integration", Reason: "filter test"})
	if err != nil || result.Created != 1 {
		t.Fatalf("filtered replay=%+v err=%v", result, err)
	}
	clone, err := service.InspectEvent(ctx, result.EventIDs[0], false)
	if err != nil || clone.ReplayedFromEventID == nil || *clone.ReplayedFromEventID != targetID {
		t.Fatalf("filtered clone=%+v err=%v", clone, err)
	}
}

func TestReplayRejectsPendingAndBatchCapIsAtomic(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service := adminService(t, e, time.Second)
	pendingID := uuid.MustParse(enqueueOrder(t, e, "ord-no-replay", topicName(t, e), 14, true))
	if _, err := service.ReplayEvent(ctx, pendingID, adminapi.Mutation{Actor: "integration", Reason: "no"}); !errors.Is(err, adminapi.ErrConflict) {
		t.Fatalf("pending replay error = %v, want conflict", err)
	}

	destination := "batch-cap-" + uuid.NewString()
	batch := &pgx.Batch{}
	for i := 0; i < adminapi.MaxReplayBatch+1; i++ {
		batch.Queue(`
INSERT INTO emitlane.outbox_events
    (id, destination, event_type, payload, status, delivered_at)
VALUES ($1, $2, 'batch.test', '{}'::bytea, 'delivered', NOW())`, uuid.New(), destination)
	}
	results := e.pool.SendBatch(ctx, batch)
	for i := 0; i < adminapi.MaxReplayBatch+1; i++ {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			t.Fatal(err)
		}
	}
	if err := results.Close(); err != nil {
		t.Fatal(err)
	}
	filter := adminapi.EventFilter{Destination: destination}
	preview, err := service.PreviewReplay(ctx, filter)
	if err != nil || preview.Count != adminapi.MaxReplayBatch+1 {
		t.Fatalf("preview count=%d err=%v", preview.Count, err)
	}
	if _, err := service.ReplayBatch(ctx, filter, adminapi.Mutation{Actor: "integration", Reason: "cap test"}); !errors.Is(err, adminapi.ErrConflict) {
		t.Fatalf("oversize replay error = %v, want conflict", err)
	}
	var clones, audits int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE replayed_from_event_id IS NOT NULL`).Scan(&clones); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.admin_audit_log WHERE action = 'replay.batch'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if clones != 0 || audits != 0 {
		t.Fatalf("oversize replay partially committed: clones=%d audits=%d", clones, audits)
	}
}

func TestBatchReplayDefaultsDeliveredAndAllowsExplicitDead(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := "batch-success-" + uuid.NewString()
	delivered := []uuid.UUID{uuid.New(), uuid.New()}
	dead := uuid.New()
	for _, id := range delivered {
		if _, err := e.pool.Exec(ctx, `
INSERT INTO emitlane.outbox_events (id, destination, event_type, payload, status, delivered_at)
VALUES ($1, $2, 'batch.success', $3, 'delivered', NOW())`, id, destination, []byte("raw")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO emitlane.outbox_events (id, destination, event_type, payload, status, last_error)
VALUES ($1, $2, 'batch.success', $3, 'dead', 'poison')`, dead, destination, []byte("dead-raw")); err != nil {
		t.Fatal(err)
	}
	service := adminService(t, e, time.Second)
	filter := adminapi.EventFilter{Destination: destination}
	preview, err := service.PreviewReplay(ctx, filter)
	if err != nil || preview.Count != 2 {
		t.Fatalf("default delivered preview count=%d err=%v", preview.Count, err)
	}
	result, err := service.ReplayBatch(ctx, filter, adminapi.Mutation{Actor: "integration", Reason: "delivered replay"})
	if err != nil || result.Created != 2 {
		t.Fatalf("delivered replay result=%+v err=%v", result, err)
	}
	for _, id := range result.EventIDs {
		event, err := service.InspectEvent(ctx, id, false)
		if err != nil {
			t.Fatal(err)
		}
		if event.ReplayBatchID == nil || *event.ReplayBatchID != result.ReplayBatchID || event.Status != "pending" {
			t.Fatalf("batch clone: %+v", event)
		}
	}
	deadFilter := adminapi.EventFilter{Destination: destination, Statuses: []string{"dead"}}
	preview, err = service.PreviewReplay(ctx, deadFilter)
	if err != nil || preview.Count != 1 {
		t.Fatalf("explicit dead preview count=%d err=%v", preview.Count, err)
	}
	deadResult, err := service.ReplayBatch(ctx, deadFilter, adminapi.Mutation{Actor: "integration", Reason: "dead replay"})
	if err != nil || deadResult.Created != 1 {
		t.Fatalf("dead replay result=%+v err=%v", deadResult, err)
	}
	var sourceStatus string
	if err := e.pool.QueryRow(ctx, `SELECT status FROM emitlane.outbox_events WHERE id = $1`, dead).Scan(&sourceStatus); err != nil {
		t.Fatal(err)
	}
	if sourceStatus != "dead" {
		t.Fatalf("dead source changed to %s", sourceStatus)
	}
	audit, err := service.ListAudit(ctx, adminapi.AuditFilter{Action: "replay.batch", Limit: 10})
	if err != nil || len(audit.Records) != 2 {
		t.Fatalf("batch audit records=%d err=%v", len(audit.Records), err)
	}
}

func TestAuditCoversMutationsWithoutSensitiveData(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service := adminService(t, e, time.Second)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "audit-operator", Reason: "audit pause", RequestID: "audit-request-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "audit-operator", Reason: "audit resume", RequestID: "audit-request-2"}); err != nil {
		t.Fatal(err)
	}
	retryID := uuid.MustParse(enqueueOrder(t, e, "recognizable-secret-payload", topicName(t, e), 16, true))
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET status='dead', last_error='safe error' WHERE id=$1`, retryID); err != nil {
		t.Fatal(err)
	}
	if err := service.RetryDead(ctx, retryID, adminapi.Mutation{Actor: "audit-operator", Reason: "audit retry", RequestID: "audit-request-3"}); err != nil {
		t.Fatal(err)
	}
	replayID := uuid.New()
	if _, err := e.pool.Exec(ctx, `
INSERT INTO emitlane.outbox_events (id, destination, event_type, payload, status, delivered_at)
VALUES ($1, 'audit.events', 'audit.replay', $2, 'delivered', NOW())`, replayID, []byte("recognizable-secret-payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReplayEvent(ctx, replayID, adminapi.Mutation{Actor: "audit-operator", Reason: "audit replay", RequestID: "audit-request-4"}); err != nil {
		t.Fatal(err)
	}
	page, err := service.ListAudit(ctx, adminapi.AuditFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	actions := map[string]bool{}
	for _, record := range page.Records {
		actions[record.Action] = true
		if record.Actor != "audit-operator" || record.Reason == "" || record.RequestID == "" || record.CreatedAt.IsZero() {
			t.Fatalf("incomplete audit record: %+v", record)
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "recognizable-secret-payload") || strings.Contains(string(raw), "super-secret-token") {
			t.Fatalf("audit exposed sensitive data: %s", raw)
		}
	}
	for _, action := range []string{"relay.pause", "relay.resume", "event.retry", "event.replay"} {
		if !actions[action] {
			t.Errorf("missing audit action %s", action)
		}
	}
}
