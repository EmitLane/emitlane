//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/inbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

// A completion can wait behind a renewal or lease takeover. It must inspect
// the row version that wins that lock, rather than using a stale pre-lock token.
func TestInboxTransitionsRecheckFenceAfterConcurrentUpdate(t *testing.T) {
	e := startEnv(t)
	for _, operation := range []string{"processed", "retry", "dead", "renew"} {
		for _, takeover := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/takeover=%t", operation, takeover), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				consumer, id, token := "fence-race-"+uuid.NewString(), uuid.New(), uuid.New()
				_, err := e.pool.Exec(ctx, `INSERT INTO emitlane.inbox_events
(consumer,event_id,status,processed_at,lease_owner,lease_token,lease_until,source_topic,source_partition,source_offset)
VALUES($1,$2,'inflight',NULL,'old',$3,NOW()+INTERVAL '1 minute','race',0,0)`, consumer, id, token)
				if err != nil {
					t.Fatal(err)
				}

				config, err := pgxpool.ParseConfig(e.databaseURL)
				if err != nil {
					t.Fatal(err)
				}
				config.MaxConns = 1
				pool, err := pgxpool.NewWithConfig(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				defer pool.Close()
				store, err := pgstore.NewInboxStore(pool)
				if err != nil {
					t.Fatal(err)
				}
				var pid int32
				if err := pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
					t.Fatal(err)
				}
				var businessTx pgx.Tx
				if operation == "processed" {
					businessTx, err = pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer businessTx.Rollback(context.Background())
					if _, err := businessTx.Exec(ctx, `INSERT INTO public.business_payments(order_id,amount) VALUES($1,1)`, id.String()); err != nil {
						t.Fatal(err)
					}
				}
				locker, err := e.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer locker.Rollback(context.Background())
				var locked uuid.UUID
				if err := locker.QueryRow(ctx, `SELECT event_id FROM emitlane.inbox_events WHERE consumer=$1 AND event_id=$2 FOR UPDATE`, consumer, id).Scan(&locked); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					switch operation {
					case "processed":
						done <- store.MarkProcessed(ctx, businessTx, consumer, id, token)
					case "retry":
						done <- store.MarkRetry(ctx, consumer, id, token, time.Second, "retry")
					case "dead":
						done <- store.MarkDead(ctx, consumer, id, token, "dead")
					case "renew":
						done <- store.Renew(ctx, consumer, id, token, time.Minute)
					}
				}()
				completed := false
				defer func() {
					cancel()
					_ = locker.Rollback(context.Background())
					if !completed {
						<-done
					}
				}()
				// Observe a real database lock wait; elapsed sleeps do not establish
				// that the transition was already in flight before the token changed.
				for {
					var waiting bool
					if err := e.pool.QueryRow(ctx, `SELECT COALESCE(wait_event_type='Lock',FALSE) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case err := <-done:
						completed = true
						t.Fatalf("transition returned before the locked row was released: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(5 * time.Millisecond):
					}
				}
				winner := token
				if takeover {
					winner = uuid.New()
				}
				if _, err := locker.Exec(ctx, `UPDATE emitlane.inbox_events SET lease_token=$3,lease_until=NOW()+INTERVAL '2 minutes' WHERE consumer=$1 AND event_id=$2`, consumer, id, winner); err != nil {
					t.Fatal(err)
				}
				if err := locker.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				err = <-done
				completed = true
				if takeover {
					if !errors.Is(err, inbox.ErrLeaseLost) {
						t.Fatalf("stale transition error=%v, want lease lost", err)
					}
				} else if err != nil {
					t.Fatalf("same-token renewal made a valid transition fail: %v", err)
				}
				if businessTx != nil {
					if takeover {
						err = businessTx.Rollback(ctx)
					} else {
						err = businessTx.Commit(ctx)
					}
					if err != nil {
						t.Fatal(err)
					}
					var effects int
					if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_payments WHERE order_id=$1`, id.String()).Scan(&effects); err != nil {
						t.Fatal(err)
					}
					if effects != boolCount(!takeover) {
						t.Fatalf("protected effects=%d after takeover=%t", effects, takeover)
					}
				}
				event, err := store.Get(ctx, consumer, id)
				if err != nil {
					t.Fatal(err)
				}
				if takeover || operation == "renew" {
					if event.Status != inbox.StatusInflight || event.LeaseToken != winner {
						t.Fatalf("winning lease changed: %+v", event)
					}
				} else {
					want := map[string]inbox.Status{"processed": inbox.StatusProcessed, "retry": inbox.StatusRetryWait, "dead": inbox.StatusDead}[operation]
					if event.Status != want || event.LeaseToken != uuid.Nil {
						t.Fatalf("transition state=%+v, want %s without lease", event, want)
					}
				}
			})
		}
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
