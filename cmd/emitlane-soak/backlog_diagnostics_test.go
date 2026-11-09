package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

// This diagnostic uses only a new private PostgreSQL fixture. Its completion
// describes query measurements, not the release soak's acceptance.
func TestPostgreSQLBacklogDiagnostics(t *testing.T) {
	runID := os.Getenv("EMITLANE_SOAK_BACKLOG_RUN_ID")
	if runID == "" {
		t.Skip("set EMITLANE_SOAK_BACKLOG_RUN_ID to opt into backlog diagnostics")
	}
	output := os.Getenv("EMITLANE_SOAK_BACKLOG_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("EMITLANE_SOAK_BACKLOG_OUTPUT must be an absolute fresh file path")
	}
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	write := func(value any) {
		t.Helper()
		if err := json.NewEncoder(file).Encode(value); err != nil {
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	complete := false
	defer func() {
		write(map[string]any{"kind": "finish", "complete": complete && !t.Failed(), "time": time.Now().UTC()})
	}()
	source, err := gitProvenance("../..")
	if err != nil {
		t.Fatal(err)
	}
	write(map[string]any{
		"kind": "start", "run_id": runID, "source": source, "time": time.Now().UTC(),
		"scope": "Synthetic 670175-row fixture with 442365 pending and 3500 streams. No replayed WAL, historical churn, Kafka or delivery workload.",
		"cache": "Seeding and prior operations populate caches. Repetitions are not cold-cache trials.",
	})
	cfg, err := profile("release")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RecoveryTimeout)
	defer cancel()
	pg, err := startSoakPostgres(ctx, map[string]string{
		"com.emitlane.soak.run_id": runID, "com.emitlane.soak.owned": "true",
	})
	if pg != nil {
		defer func() {
			if err := testcontainers.TerminateContainer(pg); err != nil {
				t.Error(err)
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	control, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := pgstore.MigrateUp(ctx, control); err != nil {
		t.Fatal(err)
	}
	var settings json.RawMessage
	err = control.QueryRow(ctx, `
SELECT json_object_agg(name, setting) FROM pg_settings
WHERE name IN ('server_version','fsync','synchronous_commit','full_page_writes',
 'shared_buffers','work_mem','effective_cache_size','max_parallel_workers_per_gather',
 'random_page_cost','statement_timeout','lock_timeout')`).Scan(&settings)
	if err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"kind": "fixture", "postgres_container": pg.GetContainerID(), "settings": settings})
	var values map[string]string
	if err := json.Unmarshal(settings, &values); err != nil {
		t.Fatal(err)
	}
	if values["fsync"] != "on" || values["synchronous_commit"] != "on" || values["full_page_writes"] != "on" {
		t.Fatal("diagnostic PostgreSQL requires durable writes")
	}
	owners := []string{"backlog-0", "backlog-1", "backlog-2", "backlog-3"}
	seedBacklogDiagnostic(t, ctx, control, owners)
	var total, pending, delivered, orderedPending int64
	err = control.QueryRow(ctx, `
SELECT COUNT(*),COUNT(*) FILTER(WHERE status='pending'),COUNT(*) FILTER(WHERE status='delivered'),
 COUNT(*) FILTER(WHERE status='pending' AND ordering_key IS NOT NULL)
FROM emitlane.outbox_events`).Scan(&total, &pending, &delivered, &orderedPending)
	if err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"kind": "initial_counts", "rows": total, "pending": pending,
		"delivered": delivered, "ordered_pending": orderedPending})
	if total != 670175 || pending != 442365 || delivered != 227810 || orderedPending != 412357 {
		t.Fatal("synthetic backlog counts differ from the selected fixture")
	}
	var catalog json.RawMessage
	err = control.QueryRow(ctx, `
SELECT json_build_object(
 'indexes',(SELECT json_agg(i) FROM (SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='emitlane' ORDER BY indexname)i),
 'tables',(SELECT json_agg(t) FROM (SELECT relname,n_live_tup,n_dead_tup,
  pg_relation_size(relid) AS heap_bytes,pg_indexes_size(relid) AS index_bytes
  FROM pg_stat_user_tables WHERE schemaname='emitlane' ORDER BY relname)t))`).Scan(&catalog)
	if err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"kind": "catalog", "data": catalog})
	capture := &backlogQueryCapture{}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.Tracer = capture
	poolConfig.ConnConfig.RuntimeParams["track_io_timing"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	// Lease durations match the standalone Relay. Renewal continues while a
	// measured query runs, with a separate pool so it cannot wait behind it.
	renew := func() error {
		_, err := control.Exec(ctx, `
UPDATE emitlane.ordering_partitions SET lease_until=clock_timestamp()+INTERVAL '4 seconds';
UPDATE emitlane.relay_instances SET last_heartbeat_at=clock_timestamp()`)
		return err
	}
	if err := renew(); err != nil {
		t.Fatal(err)
	}
	stopRenewal := make(chan struct{})
	renewalDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				renewalDone <- nil
				return
			case <-ctx.Done():
				renewalDone <- ctx.Err()
				return
			case <-ticker.C:
				if err := renew(); err != nil {
					renewalDone <- err
					return
				}
			}
		}
	}()
	defer func() {
		close(stopRenewal)
		if err := <-renewalDone; err != nil {
			t.Error(err)
		}
	}()

	for round, owner := range owners {
		for _, operation := range []string{"claim_ordered", "claim_unordered", "recovery_work_probe", "recovery_queue_state", "relay_stats", "relay_stats_poll"} {
			capture.take()
			started := time.Now()
			var events []relay.Event
			var statsCapturedAt time.Time
			switch operation {
			case "claim_ordered":
				events, err = store.ClaimOrdered(ctx, owner, 8, 4*time.Second, 1250*time.Millisecond)
			case "claim_unordered":
				events, err = store.Claim(ctx, owner, 8, 4*time.Second)
			case "recovery_work_probe":
				var unfinished bool
				unfinished, err = queueHasUnfinishedWork(ctx, pool)
				if err == nil && !unfinished {
					t.Fatal("backlog probe missed unfinished work")
				}
			case "recovery_queue_state":
				_, _, _, _, _, err = queueState(ctx, pool)
			case "relay_stats":
				_, err = store.StatsSnapshotWithPresence(ctx, time.Second)
			case "relay_stats_poll":
				_, statsCapturedAt, err = store.CachedStatsSnapshotWithPresence(ctx, time.Second)
			}
			elapsed := time.Since(started)
			queries := capture.take()
			sample := map[string]any{
				"kind": "operation", "round": round + 1, "operation": operation, "owner": owner,
				"client_seconds": elapsed.Seconds(), "queries": queries, "claimed_rows": nil,
			}
			if err != nil {
				sample["error"] = err.Error()
			} else if strings.HasPrefix(operation, "claim_") {
				sample["claimed_rows"] = len(events)
			}
			if operation == "relay_stats_poll" {
				sample["stats_captured_at"] = statsCapturedAt
				sample["snapshot_refreshed"] = len(queries) == 3
			}
			write(sample)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(operation, "claim_") && len(events) != 8 {
				t.Fatalf("%s returned %d rows, want 8", operation, len(events))
			}
			ids := make([]uuid.UUID, len(events))
			for index, event := range events {
				ids[index] = event.ID
			}
			if len(ids) > 0 {
				// Only the diagnostic's eight claimed rows are rearmed. No
				// publisher, attempts or stream-cursor transitions run here.
				tag, err := control.Exec(ctx, `
UPDATE emitlane.outbox_events SET status='pending',lease_owner=NULL,lease_until=NULL
WHERE id=ANY($1::uuid[]) AND status='inflight' AND lease_owner=$2`, ids, owner)
				if err != nil {
					t.Fatal(err)
				}
				if tag.RowsAffected() != int64(len(ids)) {
					t.Fatal("could not restore diagnostic claim rows")
				}
			}
			wantQueries := 1
			if operation == "relay_stats" {
				wantQueries = 2
			}
			if operation == "relay_stats_poll" && len(queries) == 3 {
				wantQueries = 3
			}
			if len(queries) != wantQueries {
				t.Fatalf("%s captured %d queries, want %d", operation, len(queries), wantQueries)
			}
			for index, query := range queries {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var plan json.RawMessage
				started := time.Now()
				err = tx.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) "+query.SQL, query.Args...).Scan(&plan)
				explainElapsed := time.Since(started)
				rollbackErr := tx.Rollback(context.Background())
				record := map[string]any{
					"kind": "plan", "round": round + 1, "operation": operation, "query_index": index,
					"sql": query.SQL, "args": query.Args, "plan": plan, "client_seconds": explainElapsed.Seconds(),
				}
				if err != nil {
					record["error"] = err.Error()
				}
				if rollbackErr != nil {
					record["rollback_error"] = rollbackErr.Error()
				}
				write(record)
				if err != nil || rollbackErr != nil {
					t.Fatalf("explain=%v rollback=%v", err, rollbackErr)
				}
				if strings.HasPrefix(operation, "claim_") {
					var decoded []struct {
						Plan struct {
							Rows int `json:"Actual Rows"`
						}
					}
					if err := json.Unmarshal(plan, &decoded); err != nil {
						t.Fatal(err)
					}
					if len(decoded) != 1 || decoded[0].Plan.Rows != 8 {
						t.Fatal("claim EXPLAIN did not update eight diagnostic rows")
					}
				}
			}
		}
	}
	var totals json.RawMessage
	err = control.QueryRow(ctx, `
SELECT json_build_object('rows',COUNT(*),'pending',COUNT(*) FILTER(WHERE status='pending'),
 'inflight',COUNT(*) FILTER(WHERE status='inflight'),'delivered',COUNT(*) FILTER(WHERE status='delivered'),
 'dead',COUNT(*) FILTER(WHERE status='dead')) FROM emitlane.outbox_events`).Scan(&totals)
	if err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"kind": "final_counts", "counts": totals})
	var counts struct {
		Rows, Pending, Inflight, Delivered, Dead int64
	}
	if err := json.Unmarshal(totals, &counts); err != nil {
		t.Fatal(err)
	}
	if counts.Rows != 670175 || counts.Pending != 442365 || counts.Delivered != 227810 || counts.Inflight != 0 || counts.Dead != 0 {
		t.Fatal("diagnostic operations changed the logical queue contents")
	}
	// Run after the comparable four-round measurement. All sparse mutations
	// remain in one rolled-back transaction, with pre-drain planner statistics.
	diagnoseSparseRecoveryProbe(t, ctx, pool, capture, write)
	complete = true
}

// This reference is the previously measured bare EXISTS probe. Keep it only
// in diagnostics, so the same sparse fixture compares both query shapes.
const legacyRecoveryProbeSQL = `
SELECT CASE WHEN
 EXISTS (SELECT 1 FROM emitlane.outbox_events WHERE status='pending')
 OR EXISTS (SELECT 1 FROM emitlane.outbox_events WHERE status='inflight')
 OR EXISTS (SELECT 1 FROM emitlane.outbox_events WHERE status='dead')
THEN TRUE ELSE EXISTS (
 SELECT 1 FROM emitlane.ordering_streams s
 WHERE NOT EXISTS (
  SELECT 1 FROM emitlane.outbox_events e
  WHERE e.destination=s.destination AND e.ordering_key=s.ordering_key
    AND e.ordering_key IS NOT NULL AND e.ordering_sequence=s.next_sequence)
 AND EXISTS (
  SELECT 1 FROM emitlane.outbox_events e
  WHERE e.destination=s.destination AND e.ordering_key=s.ordering_key
    AND e.ordering_key IS NOT NULL AND e.ordering_sequence>s.next_sequence)
) END`

func diagnoseSparseRecoveryProbe(t *testing.T, ctx context.Context, pool *pgxpool.Pool, capture *backlogQueryCapture, write func(any)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// Do not ANALYZE after the simulated drain: stale estimates are part of
	// this case. No session planner settings or query deadlines are changed.
	tag, err := tx.Exec(ctx, `UPDATE emitlane.outbox_events
SET status='delivered',delivered_at=NOW(),lease_owner=NULL,lease_until=NULL
WHERE status='pending'`)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 442365 {
		t.Fatalf("sparse fixture drained %d rows, want 442365", tag.RowsAffected())
	}
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM emitlane.outbox_events
WHERE ordering_key IS NULL ORDER BY created_at DESC,id DESC LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"kind": "probe_diagnostics_begin", "rows": 670175,
		"scope": "Uncommitted synthetic drain, retained delivered history, pre-drain planner statistics; all changes rolled back. Not a vacuumed aged heap or concurrent recovery workload."})
	for _, status := range []string{"pending", "inflight", "dead", "delivered", "gap"} {
		eventStatus := status
		if status == "gap" {
			eventStatus = "delivered"
		}
		_, err := tx.Exec(ctx, `UPDATE emitlane.outbox_events SET
status=$2,delivered_at=CASE WHEN $2='delivered' THEN NOW() END,
lease_owner=CASE WHEN $2='inflight' THEN 'diagnostic' END,
lease_until=CASE WHEN $2='inflight' THEN NOW()+INTERVAL '4 seconds' END
WHERE id=$1`, id, eventStatus)
		if err != nil {
			t.Fatal(err)
		}
		if status == "gap" {
			// Remove exactly one delivered expected head while retaining its
			// delivered future. A gap must remain observable with no work rows.
			tag, err := tx.Exec(ctx, `DELETE FROM emitlane.outbox_events
WHERE id=(SELECT e.id FROM emitlane.ordering_streams s
JOIN emitlane.outbox_events e ON e.destination=s.destination
 AND e.ordering_key=s.ordering_key AND e.ordering_sequence=s.next_sequence
ORDER BY s.destination,s.ordering_key LIMIT 1)`)
			if err != nil || tag.RowsAffected() != 1 {
				t.Fatalf("prepare sparse gap: %v rows=%d", err, tag.RowsAffected())
			}
		}
		var pending, inflight, dead int64
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FILTER(WHERE status='pending'),
COUNT(*) FILTER(WHERE status='inflight'),COUNT(*) FILTER(WHERE status='dead')
FROM emitlane.outbox_events`).Scan(&pending, &inflight, &dead); err != nil {
			t.Fatal(err)
		}
		want := status != "delivered"
		wantActive := int64(1)
		if status == "delivered" || status == "gap" {
			wantActive = 0
		}
		if pending+inflight+dead != wantActive {
			t.Fatalf("sparse active counts %d/%d/%d, want total %d", pending, inflight, dead, wantActive)
		}
		for _, variant := range []string{"legacy_exists", "limited_status_probe"} {
			capture.take()
			started := time.Now()
			var got bool
			var err error
			if variant == "legacy_exists" {
				err = tx.QueryRow(ctx, legacyRecoveryProbeSQL).Scan(&got)
			} else {
				got, err = queueHasUnfinishedWork(ctx, tx)
			}
			elapsed := time.Since(started)
			queries := capture.take()
			var queryError string
			if err != nil {
				queryError = err.Error()
			}
			write(map[string]any{"kind": "probe_case_operation", "case": status,
				"variant": variant, "client_seconds": elapsed.Seconds(), "unfinished": got,
				"want": want, "pending": pending, "inflight": inflight, "dead": dead, "queries": queries, "error": queryError})
			if err != nil || got != want || len(queries) != 1 {
				t.Fatalf("sparse %s/%s: got=%t want=%t queries=%d err=%v", status, variant, got, want, len(queries), err)
			}
			var plan json.RawMessage
			started = time.Now()
			err = tx.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) "+queries[0].SQL).Scan(&plan)
			queryError = ""
			if err != nil {
				queryError = err.Error()
			}
			write(map[string]any{"kind": "probe_case_plan", "case": status,
				"variant": variant, "plan": plan, "client_seconds": time.Since(started).Seconds(), "error": queryError})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var rows, pending, delivered int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*),COUNT(*) FILTER(WHERE status='pending'),
COUNT(*) FILTER(WHERE status='delivered') FROM emitlane.outbox_events`).Scan(&rows, &pending, &delivered); err != nil {
		t.Fatal(err)
	}
	if rows != 670175 || pending != 442365 || delivered != 227810 {
		t.Fatal("sparse diagnostic rollback did not preserve original fixture")
	}
	write(map[string]any{"kind": "probe_diagnostics_end", "rollback_verified": true,
		"rows": rows, "pending": pending, "delivered": delivered})
}

func seedBacklogDiagnostic(t *testing.T, ctx context.Context, pool *pgxpool.Pool, owners []string) {
	t.Helper()
	streams := make([][]any, 3500)
	for index := range streams {
		key := fmt.Sprintf("stream:%d", index)
		delivered := 138245 / len(streams)
		if index < 138245%len(streams) {
			delivered++
		}
		streams[index] = []any{"backlog-ordered", key, internalordering.Partition("backlog-ordered", key), 1, delivered + 1}
	}
	_, err := pool.CopyFrom(ctx, pgx.Identifier{"emitlane", "ordering_streams"},
		[]string{"destination", "ordering_key", "partition_id", "start_sequence", "next_sequence"}, pgx.CopyFromRows(streams))
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Now().Add(-time.Hour)
	_, err = pool.Exec(ctx, `
WITH events AS (
 SELECT md5(s.ordering_key||':'||n)::uuid AS id,s.destination,'soak.ordered' AS event_type,
  convert_to(s.ordering_key,'UTF8') AS message_key,
  CASE WHEN n<s.next_sequence THEN 'delivered' ELSE 'pending' END AS status,
  $1::timestamptz+(((n-1)*3500+split_part(s.ordering_key,':',2)::int)::double precision/550602)*INTERVAL '1 hour' AS created_at,
  CASE WHEN n<s.next_sequence THEN $1::timestamptz+INTERVAL '1 hour' END AS delivered_at,
  s.ordering_key,n::bigint AS ordering_sequence,s.partition_id AS ordering_partition
 FROM emitlane.ordering_streams s
 CROSS JOIN LATERAL generate_series(1,550602/3500+
  CASE WHEN split_part(s.ordering_key,':',2)::int<550602%3500 THEN 1 ELSE 0 END)n
 UNION ALL
 SELECT md5('unordered:'||g)::uuid,'backlog-unordered','soak.unordered',NULL::bytea,
  CASE WHEN g<=89565 THEN 'delivered' ELSE 'pending' END,
  $1::timestamptz+(g::double precision/119573)*INTERVAL '1 hour',
  CASE WHEN g<=89565 THEN $1::timestamptz+INTERVAL '1 hour' END,
  NULL::text,NULL::bigint,NULL::smallint
 FROM generate_series(1,119573)g
)
INSERT INTO emitlane.outbox_events(id,destination,event_type,message_key,payload,status,
 created_at,available_at,delivered_at,ordering_key,ordering_sequence,ordering_partition)
SELECT id,destination,event_type,message_key,decode(repeat('61',512),'hex'),status,
 created_at,created_at,delivered_at,ordering_key,ordering_sequence,ordering_partition
FROM events ORDER BY created_at,id`, anchor)
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range owners {
		_, err := pool.Exec(ctx, `
INSERT INTO emitlane.relay_instances(instance_id,hostname,version,started_at,last_heartbeat_at,ordering_capable)
VALUES($1,'diagnostic','diagnostic',NOW(),NOW(),TRUE)`, owner)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = pool.Exec(ctx, `
UPDATE emitlane.ordering_partitions
SET lease_owner=($1::text[])[partition_id%4+1],lease_until=NOW()+INTERVAL '4 seconds',epoch=1`, owners)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
ANALYZE emitlane.outbox_events;
ANALYZE emitlane.ordering_streams;
ANALYZE emitlane.ordering_partitions`)
	if err != nil {
		t.Fatal(err)
	}
}

type backlogQuery struct {
	SQL           string  `json:"sql"`
	Args          []any   `json:"args"`
	ClientSeconds float64 `json:"client_seconds"`
	Rows          *int64  `json:"rows"`
	Error         string  `json:"error,omitempty"`
	started       time.Time
}

type backlogQueryCapture struct {
	mu      sync.Mutex
	queries []backlogQuery
}

type backlogQueryKey struct{}

func (*backlogQueryCapture) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, backlogQueryKey{}, backlogQuery{
		SQL: data.SQL, Args: append([]any(nil), data.Args...), started: time.Now(),
	})
}

func (c *backlogQueryCapture) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	query := ctx.Value(backlogQueryKey{}).(backlogQuery)
	sql := strings.ToUpper(strings.TrimSpace(query.SQL))
	if !strings.HasPrefix(sql, "SELECT") && !strings.HasPrefix(sql, "WITH") {
		return
	}
	query.ClientSeconds = time.Since(query.started).Seconds()
	if data.Err != nil {
		query.Error = data.Err.Error()
	} else {
		rows := data.CommandTag.RowsAffected()
		query.Rows = &rows
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
}

func (c *backlogQueryCapture) take() []backlogQuery {
	c.mu.Lock()
	defer c.mu.Unlock()
	queries := c.queries
	c.queries = nil
	return queries
}
