//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This accounting belongs only to the qualification fixture. Every delta is
// committed (or rolled back) with its Inbox row. Fixed shards avoid one hot
// counter for the consumer; all five status totals share the query's MVCC view.
// It changes the measured fixture's write overhead, not a released migration.
const qualificationInboxAccountingDDL = `
CREATE TABLE public.qualification_inbox_counts (
 consumer text NOT NULL, shard smallint NOT NULL CHECK(shard BETWEEN 0 AND 255),
 pending bigint NOT NULL CHECK(pending>=0), inflight bigint NOT NULL CHECK(inflight>=0),
 retry_wait bigint NOT NULL CHECK(retry_wait>=0), dead bigint NOT NULL CHECK(dead>=0),
 processed bigint NOT NULL CHECK(processed>=0), PRIMARY KEY(consumer,shard)
);
CREATE FUNCTION public.qualification_inbox_count_change() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $body$
DECLARE
 old_consumer text; new_consumer text; old_id uuid; new_id uuid;
 old_status text; new_status text; change record;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF OLD.consumer=NEW.consumer AND OLD.event_id=NEW.event_id AND OLD.status=NEW.status THEN
   RETURN NULL;
  END IF;
 END IF;
 IF TG_OP<>'INSERT' THEN
  old_consumer:=OLD.consumer; old_id:=OLD.event_id; old_status:=OLD.status;
 END IF;
 IF TG_OP<>'DELETE' THEN
  new_consumer:=NEW.consumer; new_id:=NEW.event_id; new_status:=NEW.status;
 END IF;
 -- A status transition normally touches one shard, with its entire vector in
 -- one update. Identity changes lock both shards in a stable order. No network
 -- call, asynchronous observer, estimate or periodically rebuilt cache is used.
 FOR change IN
  SELECT consumer,get_byte(uuid_send(id),15)::smallint AS shard,
   SUM(delta*(status='pending')::int)::bigint AS pending,
   SUM(delta*(status='inflight')::int)::bigint AS inflight,
   SUM(delta*(status='retry_wait')::int)::bigint AS retry_wait,
   SUM(delta*(status='dead')::int)::bigint AS dead,
   SUM(delta*(status='processed')::int)::bigint AS processed
  FROM (VALUES(old_consumer,old_id,old_status,-1),(new_consumer,new_id,new_status,1))v(consumer,id,status,delta)
  WHERE consumer IS NOT NULL GROUP BY consumer,shard ORDER BY consumer,shard
 LOOP
  UPDATE public.qualification_inbox_counts AS counters SET
   pending=counters.pending+change.pending, inflight=counters.inflight+change.inflight,
   retry_wait=counters.retry_wait+change.retry_wait, dead=counters.dead+change.dead,
   processed=counters.processed+change.processed
  WHERE consumer=change.consumer AND shard=change.shard;
  IF NOT FOUND THEN
   IF LEAST(change.pending,change.inflight,change.retry_wait,change.dead,change.processed)<0 THEN
    RAISE EXCEPTION 'qualification Inbox accounting shard is missing';
   END IF;
   -- Only nonnegative new-row values reach INSERT; CHECK constraints are
   -- evaluated before ON CONFLICT and must not reject legitimate decrements.
   INSERT INTO public.qualification_inbox_counts AS counters
    (consumer,shard,pending,inflight,retry_wait,dead,processed)
   VALUES(change.consumer,change.shard,change.pending,change.inflight,change.retry_wait,change.dead,change.processed)
   ON CONFLICT(consumer,shard) DO UPDATE SET
    pending=counters.pending+EXCLUDED.pending, inflight=counters.inflight+EXCLUDED.inflight,
    retry_wait=counters.retry_wait+EXCLUDED.retry_wait, dead=counters.dead+EXCLUDED.dead,
    processed=counters.processed+EXCLUDED.processed;
  END IF;
 END LOOP;
 RETURN NULL;
END
$body$;
CREATE TRIGGER qualification_inbox_count_change AFTER INSERT OR UPDATE OR DELETE
 ON emitlane.inbox_events FOR EACH ROW EXECUTE FUNCTION public.qualification_inbox_count_change();
CREATE FUNCTION public.qualification_inbox_count_no_truncate() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
 RAISE EXCEPTION 'qualification Inbox accounting requires transactional DELETE, not TRUNCATE';
END
$body$;
CREATE TRIGGER qualification_inbox_count_no_truncate BEFORE TRUNCATE
 ON emitlane.inbox_events EXECUTE FUNCTION public.qualification_inbox_count_no_truncate();
`

const qualificationInboxAccountingSeedSQL = `INSERT INTO public.qualification_inbox_counts
 (consumer,shard,pending,inflight,retry_wait,dead,processed)
 SELECT consumer,get_byte(uuid_send(event_id),15)::smallint,
 COUNT(*) FILTER(WHERE status='pending'),COUNT(*) FILTER(WHERE status='inflight'),
 COUNT(*) FILTER(WHERE status='retry_wait'),COUNT(*) FILTER(WHERE status='dead'),
 COUNT(*) FILTER(WHERE status='processed')
 FROM emitlane.inbox_events GROUP BY consumer,get_byte(uuid_send(event_id),15)::smallint`

func installQualificationInboxAccounting(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// Block writers throughout initial seeding and trigger installation. Thus a
	// concurrent commit cannot fall between the seed snapshot and the first delta.
	if _, err = tx.Exec(ctx, `LOCK TABLE emitlane.inbox_events IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, qualificationInboxAccountingDDL); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, qualificationInboxAccountingSeedSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func removeQualificationInboxAccounting(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `DROP TRIGGER IF EXISTS qualification_inbox_count_change ON emitlane.inbox_events;
 DROP TRIGGER IF EXISTS qualification_inbox_count_no_truncate ON emitlane.inbox_events;
 DROP FUNCTION IF EXISTS public.qualification_inbox_count_change();
 DROP FUNCTION IF EXISTS public.qualification_inbox_count_no_truncate();
 DROP TABLE IF EXISTS public.qualification_inbox_counts`)
	return err
}

// This separate full scan never supplies diagnostic progress. After workload
// quiescence it reconciles the accounting against the real Inbox in one MVCC
// statement, within the existing final-audit context. The identity/effect/order/
// operator/integrity audits still read original data and remain mandatory.
func reconcileQualificationInboxAccounting(ctx context.Context, pool *pgxpool.Pool) error {
	var mismatch bool
	err := pool.QueryRow(ctx, `WITH actual AS (
 SELECT consumer,COUNT(*) FILTER(WHERE status='pending') AS pending,
 COUNT(*) FILTER(WHERE status='inflight') AS inflight,COUNT(*) FILTER(WHERE status='retry_wait') AS retry_wait,
 COUNT(*) FILTER(WHERE status='dead') AS dead,COUNT(*) FILTER(WHERE status='processed') AS processed
 FROM emitlane.inbox_events GROUP BY consumer
 ), tracked AS (
 SELECT consumer,SUM(pending) AS pending,SUM(inflight) AS inflight,SUM(retry_wait) AS retry_wait,
 SUM(dead) AS dead,SUM(processed) AS processed FROM public.qualification_inbox_counts GROUP BY consumer
 ) SELECT EXISTS(SELECT 1 FROM actual FULL JOIN tracked USING(consumer)
 WHERE ROW(COALESCE(actual.pending,0),COALESCE(actual.inflight,0),COALESCE(actual.retry_wait,0),COALESCE(actual.dead,0),COALESCE(actual.processed,0))
 IS DISTINCT FROM ROW(COALESCE(tracked.pending,0),COALESCE(tracked.inflight,0),COALESCE(tracked.retry_wait,0),COALESCE(tracked.dead,0),COALESCE(tracked.processed,0)))`).Scan(&mismatch)
	if err != nil {
		return err
	}
	if mismatch {
		return fmt.Errorf("qualification Inbox accounting differs from full independent scan")
	}
	return nil
}

func setupQualificationInboxAccounting(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), qualificationSnapshotTimeout)
	defer cancel()
	if err := installQualificationInboxAccounting(ctx, pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := removeQualificationInboxAccounting(cleanup, pool); err != nil {
			t.Error(err)
		}
	})
}

func TestQualificationInboxSnapshotAccountingAtomicity(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// The initial seed must include rows committed before installation.
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id) SELECT 'accounting-seed',md5(g::text)::uuid FROM generate_series(1,32)g`); err != nil {
		t.Fatal(err)
	}
	setupQualificationInboxAccounting(t, e.pool)
	if err := reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id) VALUES ('accounting-rollback',md5('rollback')::uuid)`); err != nil {
		t.Fatal(err)
	}
	before, err := readQualificationInboxSnapshot(ctx, e.pool, "accounting-rollback")
	if err != nil || before != [5]int64{} {
		t.Fatalf("uncommitted delta became visible: %v %v", before, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	// Terminate a real DB session after row+counter writes but before COMMIT.
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	killed, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		t.Fatal(err)
	}
	var pid int
	if err = killed.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err = killed.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id) VALUES ('accounting-crash',md5('crash')::uuid)`); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err = e.pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate backend: %v %v", terminated, err)
	}
	if err = killed.Commit(ctx); err == nil {
		t.Fatal("terminated transaction committed")
	}
	if err = reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	after, err := readQualificationInboxSnapshot(ctx, e.pool, "accounting-crash")
	if err != nil || after != [5]int64{} {
		t.Fatalf("killed transaction left count: %v %v", after, err)
	}
	// Exercise deletion, consumer and shard-key changes, and statement rollback.
	if _, err = e.pool.Exec(ctx, `UPDATE emitlane.inbox_events SET consumer='accounting-moved',event_id=md5('moved-'||event_id::text)::uuid WHERE consumer='accounting-seed'; DELETE FROM emitlane.inbox_events WHERE consumer='accounting-moved'`); err != nil {
		t.Fatal(err)
	}
	if err = reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	if _, err = e.pool.Exec(ctx, `TRUNCATE emitlane.inbox_events`); err == nil {
		t.Fatal("untracked truncate accepted")
	}
	if _, err = e.pool.Exec(ctx, `UPDATE public.qualification_inbox_counts SET processed=processed+1 WHERE consumer='accounting-moved' AND shard=(SELECT min(shard) FROM public.qualification_inbox_counts WHERE consumer='accounting-moved')`); err != nil {
		t.Fatal(err)
	}
	if err = reconcileQualificationInboxAccounting(ctx, e.pool); err == nil {
		t.Fatal("independent full scan accepted corrupted accounting")
	}
	if _, err = e.pool.Exec(ctx, `UPDATE public.qualification_inbox_counts SET processed=processed-1 WHERE consumer='accounting-moved' AND shard=(SELECT min(shard) FROM public.qualification_inbox_counts WHERE consumer='accounting-moved')`); err != nil {
		t.Fatal(err)
	}
	if _, err = e.pool.Exec(ctx, `ALTER TABLE emitlane.inbox_events DISABLE TRIGGER qualification_inbox_count_change`); err != nil {
		t.Fatal(err)
	}
	if _, err = readQualificationInboxSnapshot(ctx, e.pool, "accounting-moved"); err == nil {
		t.Fatal("disabled accounting trigger accepted")
	}
	if _, err = e.pool.Exec(ctx, `ALTER TABLE emitlane.inbox_events ENABLE TRIGGER qualification_inbox_count_change`); err != nil {
		t.Fatal(err)
	}
	if err = reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	t.Log("ACCOUNTING_ATOMICITY seed/rollback/backend-death/identity-move/delete/corruption/disabled-trigger/truncate proof passed")
}

func TestQualificationInboxSnapshotAccountingConcurrent(t *testing.T) {
	e := startEnv(t)
	setupQualificationInboxAccounting(t, e.pool)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	results := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		go func(worker int) {
			for i := 0; i < 80; i++ {
				id := fmt.Sprintf("accounting-concurrent-%d-%d", worker, i)
				tx, err := e.pool.Begin(ctx)
				if err != nil {
					results <- err
					return
				}
				_, err = tx.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id,status,processed_at,source_topic,source_partition,source_offset) VALUES ('accounting-concurrent',md5($1)::uuid,'pending',NULL,'accounting-concurrent',$2,$3)`, id, worker, i)
				if err == nil {
					_, err = tx.Exec(ctx, `UPDATE emitlane.inbox_events SET status='retry_wait' WHERE consumer='accounting-concurrent' AND event_id=md5($1)::uuid`, id)
				}
				if err == nil {
					_, err = tx.Exec(ctx, `UPDATE emitlane.inbox_events SET status='processed',processed_at=NOW() WHERE consumer='accounting-concurrent' AND event_id=md5($1)::uuid`, id)
				}
				if err == nil {
					err = tx.Commit(ctx)
				} else {
					_ = tx.Rollback(ctx)
				}
				if err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(worker)
	}
	// Each comparison reads source rows and shards in one statement snapshot;
	// comparing two separate READ COMMITTED statements would itself be racy.
	for i := 0; i < 40; i++ {
		if err := reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	got, err := readQualificationInboxSnapshot(ctx, e.pool, "accounting-concurrent")
	if err != nil || got != [5]int64{0, 0, 0, 0, 320} {
		t.Fatalf("concurrent counts: %v %v", got, err)
	}
	if err = reconcileQualificationInboxAccounting(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	t.Log("ACCOUNTING_CONCURRENCY four real writers/320 commits/40 single-snapshot independent comparisons passed")
}
