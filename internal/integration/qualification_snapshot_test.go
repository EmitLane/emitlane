//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const qualificationSnapshotTimeout = 10 * time.Second

// Keep all five exact counts in one MVCC statement. Explicit, literal status
// predicates make the released partial indexes eligible. A FILTER-only status
// condition forces every retained row's status to be fetched from the heap.
// Processed history is still counted in full: this is not an estimate, cached
// counter, planner override or replacement for the independent final audits.
const qualificationInboxSnapshotSQL = `SELECT
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='pending'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='inflight'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='retry_wait'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='dead'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='processed')`

func readQualificationInboxSnapshot(ctx context.Context, pool *pgxpool.Pool, name string) ([5]int64, error) {
	var counts [5]int64
	err := pool.QueryRow(ctx, qualificationInboxSnapshotSQL, name).Scan(&counts[0], &counts[1], &counts[2], &counts[3], &counts[4])
	return counts, err
}

func qualificationInboxSnapshotPlan(ctx context.Context, pool *pgxpool.Pool, name string) (json.RawMessage, error) {
	var plan json.RawMessage
	err := pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+qualificationInboxSnapshotSQL, name).Scan(&plan)
	return plan, err
}

func recordQualificationSnapshotFailure(output, phase string, elapsed time.Duration, contextErr, cause error) error {
	if output == "" {
		return nil
	}
	proof := map[string]any{"time": time.Now().UTC(), "phase": phase, "elapsed_seconds": elapsed.Seconds(),
		"shared_deadline_seconds": qualificationSnapshotTimeout.Seconds(), "error": cause.Error()}
	if contextErr != nil {
		proof["context_error"] = contextErr.Error()
	}
	if phase == "inbox_counts" {
		proof["query"] = qualificationInboxSnapshotSQL
	}
	name := filepath.Join(output, "snapshot-failure.json")
	if _, err := os.Stat(name); err == nil {
		return fmt.Errorf("snapshot failure evidence already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(name+".partial", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(proof)
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name+".partial", name)
	}
	return err
}

func failQualificationSnapshot(t *testing.T, ctx context.Context, started time.Time, phase string, cause error) {
	t.Helper()
	if err := recordQualificationSnapshotFailure(os.Getenv("EMITLANE_QUALIFICATION_OUTPUT"), phase, time.Since(started), ctx.Err(), cause); err != nil {
		t.Errorf("record snapshot failure: %v", err)
	}
	t.Fatalf("qualification snapshot %s: %v", phase, cause)
}

func TestQualificationInboxSnapshotCounts(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	consumer := "snapshot-counts"
	defer func() {
		if _, err := e.pool.Exec(context.Background(), `DELETE FROM emitlane.inbox_events WHERE consumer IN ($1,$2)`, consumer, consumer+"-other"); err != nil {
			t.Error(err)
		}
	}()
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events
 (consumer,event_id,status,processed_at,lease_owner,lease_token,lease_until,source_topic,source_partition,source_offset)
 SELECT $1,md5('snapshot-'||g)::uuid,s,CASE WHEN s='processed' THEN NOW() END,
 CASE WHEN s='inflight' THEN 'snapshot-owner' END,CASE WHEN s='inflight' THEN md5('lease-'||g)::uuid END,
 CASE WHEN s='inflight' THEN NOW()+INTERVAL '1 hour' END,'snapshot',0,g
 FROM (VALUES (1,'pending'),(2,'pending'),(3,'inflight'),(4,'retry_wait'),(5,'dead'),(6,'processed'),(7,'processed')) AS v(g,s);
 `, consumer); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id) VALUES ($1,md5('other')::uuid)`, consumer+"-other"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{consumer, consumer + "-other", consumer + "-missing"} {
		got, err := readQualificationInboxSnapshot(ctx, e.pool, name)
		if err != nil {
			t.Fatal(err)
		}
		var expected [5]int64
		if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FILTER(WHERE status='pending'),COUNT(*) FILTER(WHERE status='inflight'),COUNT(*) FILTER(WHERE status='retry_wait'),COUNT(*) FILTER(WHERE status='dead'),COUNT(*) FILTER(WHERE status='processed') FROM emitlane.inbox_events WHERE consumer=$1`, name).Scan(&expected[0], &expected[1], &expected[2], &expected[3], &expected[4]); err != nil {
			t.Fatal(err)
		}
		if got != expected {
			t.Fatalf("counts for %s: got=%v expected=%v", name, got, expected)
		}
		if name == consumer && got != [5]int64{2, 1, 1, 1, 2} {
			t.Fatalf("unexpected fixture counts: %v", got)
		}
	}
	// A caller deadline must still cancel a blocked count; no internal retry or
	// replacement context is allowed to turn diagnostic failure into success.
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `LOCK TABLE emitlane.inbox_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	blocked, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = readQualificationInboxSnapshot(blocked, e.pool, consumer)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked snapshot did not retain caller deadline: %v", err)
	}
}

func TestQualificationSnapshotFailureEvidence(t *testing.T) {
	output := t.TempDir()
	if err := recordQualificationSnapshotFailure(output, "inbox_counts", 10*time.Second, context.DeadlineExceeded, fmt.Errorf("query: %w", context.DeadlineExceeded)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(output, "snapshot-failure.json"))
	if err != nil {
		t.Fatal(err)
	}
	var proof struct {
		Phase        string  `json:"phase"`
		Query        string  `json:"query"`
		Deadline     float64 `json:"shared_deadline_seconds"`
		ContextError string  `json:"context_error"`
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Phase != "inbox_counts" || proof.Query != qualificationInboxSnapshotSQL || proof.Deadline != 10 || proof.ContextError != context.DeadlineExceeded.Error() {
		t.Fatalf("incomplete snapshot failure evidence: %+v", proof)
	}
	if err := recordQualificationSnapshotFailure(output, "later", time.Second, nil, errors.New("later failure")); err == nil {
		t.Fatal("first snapshot failure was overwritten")
	}
}

// A small fresh fixture cannot detect accidental reintroduction of the
// FILTER-only heap scan. This bounded opt-in checks the real query's access
// path with retained history, without changing any released index or planner.
func TestQualificationInboxSnapshotRetainedHistory(t *testing.T) {
	if os.Getenv("EMITLANE_QUALIFICATION_SNAPSHOT_CHECK") != "1" {
		t.Skip("opt-in retained-history snapshot regression")
	}
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id)
 SELECT c,md5(c||g)::uuid FROM generate_series(1,200000)g CROSS JOIN (VALUES ('snapshot-history'),('snapshot-other'))v(c)`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `VACUUM (ANALYZE) emitlane.inbox_events`); err != nil {
		t.Fatal(err)
	}
	queryCtx, stop := context.WithTimeout(ctx, qualificationSnapshotTimeout)
	got, err := readQualificationInboxSnapshot(queryCtx, e.pool, "snapshot-history")
	stop()
	if err != nil || got != [5]int64{0, 0, 0, 0, 200000} {
		t.Fatalf("retained history count: %v %v", got, err)
	}
	plan, err := qualificationInboxSnapshotPlan(ctx, e.pool, "snapshot-history")
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := json.Unmarshal(plan, &document); err != nil {
		t.Fatal(err)
	}
	indexes := make(map[string]bool)
	var walk func(any)
	walk = func(value any) {
		switch node := value.(type) {
		case []any:
			for _, child := range node {
				walk(child)
			}
		case map[string]any:
			if index, ok := node["Index Name"].(string); ok {
				indexes[index] = true
			}
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(document)
	for _, index := range []string{"inbox_due_idx", "inbox_expired_lease_idx", "inbox_dead_idx"} {
		if !indexes[index] {
			t.Fatalf("inactive-state partial index %s not used: %s", index, plan)
		}
	}
	// PostgreSQL may prefer a sequential scan for a dominant consumer's
	// processed history, even after VACUUM. Verify processed-index eligibility
	// with a selective consumer instead of forcing a planner choice globally.
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id)
 SELECT 'snapshot-selective',md5('selective-'||g)::uuid FROM generate_series(1,1000)g`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `VACUUM (ANALYZE) emitlane.inbox_events`); err != nil {
		t.Fatal(err)
	}
	selectiveCtx, stopSelective := context.WithTimeout(ctx, qualificationSnapshotTimeout)
	selective, err := readQualificationInboxSnapshot(selectiveCtx, e.pool, "snapshot-selective")
	stopSelective()
	if err != nil || selective != [5]int64{0, 0, 0, 0, 1000} {
		t.Fatalf("selective history count: %v %v", selective, err)
	}
	selectivePlan, err := qualificationInboxSnapshotPlan(ctx, e.pool, "snapshot-selective")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(selectivePlan, &document); err != nil {
		t.Fatal(err)
	}
	indexes = make(map[string]bool)
	walk(document)
	if !indexes["inbox_processed_retention_idx"] {
		t.Fatalf("selective processed partial index not used: %s", selectivePlan)
	}
	t.Logf("SNAPSHOT_SELECTIVE_PROOF retained=1000 exact_counts=%v plan=%s", selective, selectivePlan)
	t.Logf("SNAPSHOT_HISTORY_PROOF retained_per_consumer=200000 consumers=2 exact_counts=%v plan=%s", got, plan)
}
