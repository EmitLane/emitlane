//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	containernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

// This explicit opt-in keeps millions of retained rows and stale partial-index
// entries. Small fresh fixtures cannot qualify identity-transition access paths.
func TestInboxTransitionsOnAgedTable(t *testing.T) {
	raw := os.Getenv("EMITLANE_INBOX_AGING_ROWS")
	if raw == "" {
		t.Skip("opt-in retained-row and index-churn regression")
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows < 100000 || rows > 4000000 {
		t.Fatal("EMITLANE_INBOX_AGING_ROWS must be between100000 and4000000")
	}
	output := os.Getenv("EMITLANE_INBOX_AGING_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("EMITLANE_INBOX_AGING_OUTPUT must be an absolute fresh file path")
	}
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	write := func(v any) {
		t.Helper()
		if err := json.NewEncoder(file).Encode(v); err != nil {
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	commit, _, dirty, _ := soakGitProvenance(t)
	if dirty {
		t.Fatal("aged-table evidence requires clean committed source")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	port, err := reserveLocalPort()
	if err != nil {
		t.Fatal(err)
	}
	pg, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("emitlane"),
		postgres.WithUsername("emitlane"), postgres.WithPassword("emitlane"), postgres.BasicWaitStrategies(),
		testcontainers.WithCmd("postgres", "-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "full_page_writes=on"),
		testcontainers.WithHostConfigModifier(func(c *container.HostConfig) {
			c.ShmSize = qualificationPostgresShmBytes
			c.Memory = 3 * 1024 * 1024 * 1024
			if c.PortBindings == nil {
				c.PortBindings = containernetwork.PortMap{}
			}
			c.PortBindings[containernetwork.MustParsePort("5432/tcp")] = []containernetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer testcontainers.TerminateContainer(pg)
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
	var durability [3]string
	if err := control.QueryRow(ctx, `SELECT current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')`).Scan(&durability[0], &durability[1], &durability[2]); err != nil {
		t.Fatal(err)
	}
	if durability != [3]string{"on", "on", "on"} {
		t.Fatalf("durability=%v", durability)
	}
	write(map[string]any{"kind": "start", "time": time.Now().UTC(), "commit": commit, "retained_rows": rows, "durability": durability,
		"scope": "Synthetic retained rows plus controlled stale-index entries; not replayed24h history or sustained-throughput proof."})
	if _, err := control.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id,status,processed_at,source_topic,source_partition,source_offset)
SELECT 'aging',md5(g::text)::uuid,'pending',NULL,'aging',0,g FROM generate_series(1,$1::bigint)g`, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, `UPDATE emitlane.inbox_events SET status='processed',processed_at=NOW() WHERE consumer='aging'`); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, `VACUUM (ANALYZE) emitlane.inbox_events`); err != nil {
		t.Fatal(err)
	}
	// Keep stale entries as a controlled fixture variable. No production tuning
	// or released migration changes are part of this regression.
	if _, err := control.Exec(ctx, `ALTER TABLE emitlane.inbox_events SET (autovacuum_enabled=false)`); err != nil {
		t.Fatal(err)
	}
	churn := rows / 15
	if _, err := control.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id,status,processed_at,lease_owner,lease_token,lease_until,source_topic,source_partition,source_offset)
SELECT 'aging',md5(g::text)::uuid,'inflight',NULL,'fixture','00000000-0000-4000-8000-000000000001',NOW()+INTERVAL '1 hour','aging',0,g
FROM generate_series($1::bigint+1,$1::bigint+$2::bigint)g`, rows, churn); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, `UPDATE emitlane.inbox_events SET status='processed',processed_at=NOW(),lease_owner=NULL,lease_token=NULL,lease_until=NULL WHERE consumer='aging' AND source_offset>$1`, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, `ANALYZE emitlane.inbox_events`); err != nil {
		t.Fatal(err)
	}
	capture := &inboxPlanCapture{}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = capture
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.NewInboxStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"stale-index", "clean-index"} {
		if phase == "clean-index" {
			if _, err := control.Exec(ctx, `VACUUM (ANALYZE,INDEX_CLEANUP ON) emitlane.inbox_events`); err != nil {
				t.Fatal(err)
			}
		}
		for offset, operation := range []string{"processed", "retry", "dead", "renew"} {
			consumer, id, token := "probe-"+uuid.NewString(), uuid.New(), uuid.New()
			rearm := func() {
				t.Helper()
				_, err := control.Exec(ctx, `INSERT INTO emitlane.inbox_events(consumer,event_id,status,processed_at,lease_owner,lease_token,lease_until,source_topic,source_partition,source_offset)
VALUES($1,$2,'inflight',NULL,'probe',$3,NOW()+INTERVAL '1 hour','probe',0,$4)
ON CONFLICT(consumer,event_id) DO UPDATE SET status='inflight',processed_at=NULL,lease_owner='probe',lease_token=$3,lease_until=NOW()+INTERVAL '1 hour'`, consumer, id, token, offset)
				if err != nil {
					t.Fatal(err)
				}
			}
			rearm()
			started := time.Now()
			if operation == "processed" {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				err = store.MarkProcessed(ctx, tx, consumer, id, token)
				_ = tx.Rollback(ctx)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				switch operation {
				case "retry":
					err = store.MarkRetry(ctx, consumer, id, token, time.Second, "probe")
				case "dead":
					err = store.MarkDead(ctx, consumer, id, token, "probe")
				case "renew":
					err = store.Renew(ctx, consumer, id, token, time.Minute)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			elapsed := time.Since(started)
			query, args := capture.last()
			if query == "" {
				t.Fatal("did not capture production transition query")
			}
			rearm()
			tx, err := control.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var plan json.RawMessage
			err = tx.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) "+query, args...).Scan(&plan)
			_ = tx.Rollback(ctx)
			if err != nil {
				t.Fatal(err)
			}
			write(map[string]any{"kind": "transition", "phase": phase, "operation": operation, "client_seconds": elapsed.Seconds(), "query": query, "plan": plan})
			var nodes []struct{ Plan map[string]any }
			if err := json.Unmarshal(plan, &nodes); err != nil || len(nodes) != 1 {
				t.Fatalf("decode plan: %v", err)
			}
			if err := checkInboxIdentityPlan(nodes[0].Plan); err != nil {
				t.Fatalf("%s/%s: %v", phase, operation, err)
			}
		}
	}
	write(map[string]any{"kind": "PASS", "commit": commit, "retained_rows": rows, "controlled_churn_rows": churn, "time": time.Now().UTC()})
}

// Capture the real store statement, not a separately maintained SQL copy.
type inboxPlanCapture struct {
	mu    sync.Mutex
	query string
	args  []any
}

func (c *inboxPlanCapture) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "UPDATE emitlane.inbox_events") {
		c.mu.Lock()
		c.query, c.args = data.SQL, append([]any(nil), data.Args...)
		c.mu.Unlock()
	}
	return ctx
}
func (*inboxPlanCapture) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (c *inboxPlanCapture) last() (string, []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.query, append([]any(nil), c.args...)
}

func checkInboxIdentityPlan(plan map[string]any) error {
	primary := false
	var visit func(map[string]any) error
	visit = func(node map[string]any) error {
		if name, _ := node["Index Name"].(string); name != "" {
			if name == "inbox_events_pkey" {
				primary = true
			}
			if name == "inbox_expired_lease_idx" {
				return fmt.Errorf("identity transition scanned the expiry index")
			}
		}
		if node["Node Type"] == "Seq Scan" && node["Relation Name"] == "inbox_events" {
			return fmt.Errorf("identity transition scanned the retained Inbox table")
		}
		children, _ := node["Plans"].([]any)
		for _, raw := range children {
			if err := visit(raw.(map[string]any)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(plan); err != nil {
		return err
	}
	if !primary {
		return fmt.Errorf("identity transition did not use the primary key")
	}
	return nil
}
