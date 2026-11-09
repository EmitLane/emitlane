package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/emitlane/emitlane/outbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestQueueWorkProbePostgreSQL(t *testing.T) {
	runID := os.Getenv("EMITLANE_SOAK_QUEUE_PROBE_RUN_ID")
	if runID == "" {
		t.Skip("set EMITLANE_SOAK_QUEUE_PROBE_RUN_ID for a private PostgreSQL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pg, err := startSoakPostgres(ctx, map[string]string{"com.emitlane.soak.run_id": runID, "com.emitlane.soak.owned": "true"})
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
	for _, tc := range []struct {
		name       string
		status     string
		scheduled  bool
		ordered    bool
		removeHead bool
		want       bool
	}{
		{name: "empty"},
		{name: "pending", status: "pending", want: true},
		{name: "scheduled", status: "pending", scheduled: true, want: true},
		{name: "inflight", status: "inflight", want: true},
		{name: "dead", status: "dead", want: true},
		{name: "delivered only", status: "delivered"},
		{name: "empty stream", ordered: true, removeHead: true},
		// No active queue rows remain in this case. A durable gap must still
		// keep the probe true, rather than start a full count on every tick.
		{name: "delivered future gap", status: "delivered", ordered: true, removeHead: true, want: true},
		{name: "delivered head", status: "delivered", ordered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `TRUNCATE emitlane.outbox_events,emitlane.ordering_streams`); err != nil {
				t.Fatal(err)
			}
			if tc.ordered {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				sequence := int64(1)
				if tc.removeHead && tc.status != "" {
					sequence = 2
				}
				id, err := outbox.NewWriter().Enqueue(ctx, tx, outbox.Event{Destination: "probe", Type: "probe", OrderingKey: "stream", Sequence: sequence, Payload: []byte("{}")})
				if err != nil {
					t.Fatal(err)
				}
				eventID, err := uuid.Parse(id)
				if err != nil {
					t.Fatal(err)
				}
				if tc.status == "delivered" {
					if _, err := tx.Exec(ctx, `UPDATE emitlane.outbox_events SET status='delivered',delivered_at=NOW() WHERE id=$1`, eventID); err != nil {
						t.Fatal(err)
					}
				} else if tc.removeHead {
					if _, err := tx.Exec(ctx, `DELETE FROM emitlane.outbox_events WHERE id=$1`, eventID); err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			} else if tc.status != "" {
				_, err := pool.Exec(ctx, `INSERT INTO emitlane.outbox_events
(id,destination,event_type,payload,status,available_at,lease_owner,lease_until,delivered_at)
VALUES ($1,'probe','probe',$2,$3,NOW()+($4*INTERVAL '1 hour'),
 CASE WHEN $3='inflight' THEN 'old' END,
 CASE WHEN $3='inflight' THEN NOW()-INTERVAL '1 second' END,
 CASE WHEN $3='delivered' THEN NOW() END)`, uuid.New(), []byte("{}"), tc.status, boolInt(tc.scheduled))
				if err != nil {
					t.Fatal(err)
				}
			}
			unfinished, err := queueHasUnfinishedWork(ctx, pool)
			if err != nil || unfinished != tc.want {
				t.Fatalf("probe=%t want=%t err=%v", unfinished, tc.want, err)
			}
			pending, inflight, dead, blocked, gaps, err := queueState(ctx, pool)
			if err != nil || unfinished != (pending != 0 || inflight != 0 || dead != 0 || blocked != 0 || gaps != 0) {
				t.Fatalf("probe disagrees with exact state %d/%d/%d/%d/%d: %v", pending, inflight, dead, blocked, gaps, err)
			}
		})
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := queueHasUnfinishedWork(canceled, pool); !errors.Is(err, context.Canceled) {
		t.Fatalf("query error became an empty queue: %v", err)
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
