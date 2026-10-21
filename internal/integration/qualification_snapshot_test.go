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

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const qualificationSnapshotTimeout = 10 * time.Second

// All five exact counts use one MVCC statement. Range boundaries only split
// work; they never filter history out. The first/last ranges are unbounded, so
// inserts between the boundary lookup and COUNT remain visible to the COUNT's
// snapshot. Both queries use the caller's original deadline, without retries.
// External bounds permit Parallel Append of ordered retention-index scans;
// CTE/InitPlan bounds inhibited this plan in the measured PostgreSQL 16 fixture.
// This remains O(n), with no guarantee of parallel workers or a universal 10s
// capacity bound. Equal timestamps and extreme history can leave skewed ranges.
const qualificationInboxBoundsSQL = `SELECT min(processed_at),max(processed_at)
 FROM emitlane.inbox_events WHERE consumer=$1 AND status='processed'`

const qualificationInboxSnapshotSQL = `SELECT
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='pending'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='inflight'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='retry_wait'),
 (SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='dead'),
 (SELECT SUM(n)::bigint FROM (
   SELECT COUNT(*) AS n FROM (SELECT event_id FROM emitlane.inbox_events
     WHERE consumer=$1 AND status='processed' AND processed_at < $2::timestamptz
     ORDER BY processed_at,event_id) h0
   UNION ALL
   SELECT COUNT(*) AS n FROM (SELECT event_id FROM emitlane.inbox_events
     WHERE consumer=$1 AND status='processed' AND processed_at >= $2::timestamptz AND processed_at < $3::timestamptz
     ORDER BY processed_at,event_id) h1
   UNION ALL
   SELECT COUNT(*) AS n FROM (SELECT event_id FROM emitlane.inbox_events
     WHERE consumer=$1 AND status='processed' AND processed_at >= $3::timestamptz AND processed_at < $4::timestamptz
     ORDER BY processed_at,event_id) h2
   UNION ALL
   SELECT COUNT(*) AS n FROM (SELECT event_id FROM emitlane.inbox_events
     WHERE consumer=$1 AND status='processed' AND processed_at >= $4::timestamptz
     ORDER BY processed_at,event_id) h3
 ) processed_history)`

func qualificationInboxBounds(ctx context.Context, conn *pgxpool.Conn, name string) ([3]time.Time, error) {
	var lo, hi pgtype.Timestamptz
	bounds := [3]time.Time{time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC()}
	if err := conn.QueryRow(ctx, qualificationInboxBoundsSQL, name).Scan(&lo, &hi); err != nil {
		return bounds, fmt.Errorf("processed history boundaries: %w", err)
	}
	// NULL (empty history) and infinities use equal finite split points. All
	// values still belong to exactly one range, including +/-infinity. time.Sub
	// saturates for very wide finite histories; the unbounded tail still covers
	// every row, although this can reduce parallel speedup.
	if lo.Valid && hi.Valid && lo.InfinityModifier == pgtype.Finite && hi.InfinityModifier == pgtype.Finite {
		step := hi.Time.Sub(lo.Time) / 4
		for i := range bounds {
			bounds[i] = lo.Time.Add(time.Duration(i+1) * step)
		}
	}
	return bounds, nil
}

func readQualificationInboxSnapshot(ctx context.Context, pool *pgxpool.Pool, name string) ([5]int64, error) {
	var counts [5]int64
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return counts, err
	}
	defer conn.Release()
	bounds, err := qualificationInboxBounds(ctx, conn, name)
	if err != nil {
		return counts, err
	}
	err = conn.QueryRow(ctx, qualificationInboxSnapshotSQL, name, bounds[0], bounds[1], bounds[2]).Scan(&counts[0], &counts[1], &counts[2], &counts[3], &counts[4])
	if err != nil {
		return counts, fmt.Errorf("processed history range count bounds=%v: %w", bounds, err)
	}
	return counts, nil
}

func qualificationInboxSnapshotPlan(ctx context.Context, pool *pgxpool.Pool, name string) (json.RawMessage, error) {
	var plan json.RawMessage
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	bounds, err := qualificationInboxBounds(ctx, conn, name)
	if err != nil {
		return nil, err
	}
	err = conn.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+qualificationInboxSnapshotSQL, name, bounds[0], bounds[1], bounds[2]).Scan(&plan)
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
		proof["bounds_query"] = qualificationInboxBoundsSQL
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
	// Boundary equality, absent consumers, infinities and values outside an older
	// boundary lookup must remain exact. Boundaries are optimization inputs, not
	// constraints on which history is eligible for counting.
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id,processed_at)
 SELECT $1,md5('edge-'||g)::uuid,stamp FROM (VALUES
 (1,'-infinity'::timestamptz),(2,'infinity'::timestamptz),
 (3,'1970-01-01'::timestamptz),(4,'2000-01-01'::timestamptz),
 (5,'2000-01-02'::timestamptz),(6,'2000-01-03'::timestamptz),
 (7,'1900-01-01'::timestamptz),(8,'2200-01-01'::timestamptz))v(g,stamp)`, consumer); err != nil {
		t.Fatal(err)
	}
	expectedEdges := [5]int64{2, 1, 1, 1, 10}
	gotEdges, err := readQualificationInboxSnapshot(ctx, e.pool, consumer)
	if err != nil || gotEdges != expectedEdges {
		t.Fatalf("infinite history counts: %v %v", gotEdges, err)
	}
	for _, bounds := range [][3]time.Time{
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC)},
		{time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)},
	} {
		var got [5]int64
		err := e.pool.QueryRow(ctx, qualificationInboxSnapshotSQL, consumer, bounds[0], bounds[1], bounds[2]).Scan(&got[0], &got[1], &got[2], &got[3], &got[4])
		if err != nil || got != expectedEdges {
			t.Fatalf("range boundary counts: %v %v", got, err)
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
		BoundsQuery  string  `json:"bounds_query"`
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Phase != "inbox_counts" || proof.Query != qualificationInboxSnapshotSQL || proof.BoundsQuery != qualificationInboxBoundsSQL || proof.Deadline != 10 || proof.ContextError != context.DeadlineExceeded.Error() {
		t.Fatalf("incomplete snapshot failure evidence: %+v", proof)
	}
	if err := recordQualificationSnapshotFailure(output, "later", time.Second, nil, errors.New("later failure")); err == nil {
		t.Fatal("first snapshot failure was overwritten")
	}
}

// A small fresh fixture cannot detect accidental reintroduction of the
// heap scan of processed history. This bounded opt-in checks the real query's access
// path with retained history, without changing any released index or planner.
func TestQualificationInboxSnapshotRetainedHistory(t *testing.T) {
	if os.Getenv("EMITLANE_QUALIFICATION_SNAPSHOT_CHECK") != "1" {
		t.Skip("opt-in retained-history snapshot regression")
	}
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id,processed_at)
 SELECT c,md5(c||g)::uuid,NOW()-g*INTERVAL '1 second' FROM generate_series(1,200000)g CROSS JOIN (VALUES ('snapshot-history'),('snapshot-other'))v(c)`); err != nil {
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
	for _, index := range []string{"inbox_due_idx", "inbox_expired_lease_idx", "inbox_dead_idx", "inbox_processed_retention_idx"} {
		if !indexes[index] {
			t.Fatalf("snapshot partial index %s not used: %s", index, plan)
		}
	}
	// Recheck after this consumer becomes the entire retained history. The
	// previous scalar COUNT could choose a parallel heap scan for this case.
	if _, err := e.pool.Exec(ctx, `DELETE FROM emitlane.inbox_events WHERE consumer='snapshot-other'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `VACUUM (ANALYZE) emitlane.inbox_events`); err != nil {
		t.Fatal(err)
	}
	dominantCtx, stopDominant := context.WithTimeout(ctx, qualificationSnapshotTimeout)
	dominant, err := readQualificationInboxSnapshot(dominantCtx, e.pool, "snapshot-history")
	stopDominant()
	if err != nil || dominant != got {
		t.Fatalf("dominant history count: %v %v", dominant, err)
	}
	dominantPlan, err := qualificationInboxSnapshotPlan(ctx, e.pool, "snapshot-history")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(dominantPlan, &document); err != nil {
		t.Fatal(err)
	}
	indexes = make(map[string]bool)
	walk(document)
	if !indexes["inbox_processed_retention_idx"] {
		t.Fatalf("dominant processed partial index not used: %s", dominantPlan)
	}
	t.Logf("SNAPSHOT_DOMINANT_PROOF retained=200000 exact_counts=%v plan=%s", dominant, dominantPlan)
	// Also verify the exact count and access path for a selective consumer.
	// No session planner settings or new indexes are used for either fixture.
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
