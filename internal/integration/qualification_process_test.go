//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkaadapter "github.com/emitlane/emitlane/broker/kafka"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type qualificationCrashWindow struct {
	Mode    string    `json:"mode"`
	EventID uuid.UUID `json:"event_id"`
}

func killQualificationChild(t *testing.T, e *env, c qualificationConfig, topic, group, name, mode string, cycle int) uuid.UUID {
	t.Helper()
	var probe uuid.UUID
	if mode != "consumer" {
		// The parent Relay is stopped. A quiet or stopped producer must not
		// leave the helper waiting for an event that will never be committed.
		probe = uuid.MustParse(qualificationPauseProbe(t, e, topic, c.seed))
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	signalPath := filepath.Join(t.TempDir(), "ready")
	logPath := filepath.Join(c.output, mode+"-"+time.Now().UTC().Format("20060102T150405.000000000")+".log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(executable, "-test.run=^TestQualificationProcessHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "EMITLANE_QUALIFICATION_HELPER="+mode, "EMITLANE_QUALIFICATION_DATABASE="+e.databaseURL,
		"EMITLANE_QUALIFICATION_BROKERS="+strings.Join(e.brokers, ","), "EMITLANE_QUALIFICATION_TOPIC="+topic,
		"EMITLANE_QUALIFICATION_GROUP="+group, "EMITLANE_QUALIFICATION_CONSUMER="+name, "EMITLANE_QUALIFICATION_SIGNAL="+signalPath,
		"EMITLANE_QUALIFICATION_PROBE="+probe.String())
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	deadline := time.Now().Add(25 * time.Second)
	for {
		if _, err := os.Stat(signalPath); err == nil {
			break
		}
		select {
		case err := <-done:
			waited = true
			t.Fatalf("%s helper exited before crash window: %v (see %s)", mode, err, logPath)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s helper missed crash window in cycle %d (see %s)", mode, cycle+1, logPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = <-done
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != -1 {
		t.Fatalf("%s helper was not killed: %v", mode, err)
	}
	data, err := os.ReadFile(signalPath)
	if err != nil {
		t.Fatal(err)
	}
	var window qualificationCrashWindow
	if err := json.Unmarshal(data, &window); err != nil || window.Mode != mode || window.EventID == uuid.Nil || (probe != uuid.Nil && window.EventID != probe) {
		t.Fatalf("invalid crash-window proof: %+v error=%v", window, err)
	}
	return window.EventID
}

// Spawned using this test binary: the parent kills us only after the ready
// marker proves we are inside the requested transaction/ACK/claim window.
func TestQualificationProcessHelper(t *testing.T) {
	mode := os.Getenv("EMITLANE_QUALIFICATION_HELPER")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(os.Getenv("EMITLANE_QUALIFICATION_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logger.Info("crash helper started", "mode", mode)
	// Capture the child itself before the parent's 25s deadline. Parent profiles
	// cannot explain a child blocked on a pool, partition fence or broker ACK.
	go func() {
		time.Sleep(20 * time.Second)
		logger.Warn("crash helper still waiting", "mode", mode,
			"pool_acquired", pool.Stat().AcquiredConns(), "pool_total", pool.Stat().TotalConns())
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
	}()
	probe, err := uuid.Parse(os.Getenv("EMITLANE_QUALIFICATION_PROBE"))
	if err != nil {
		t.Fatal(err)
	}
	ready := func(eventID uuid.UUID) {
		logger.Info("crash helper reached window", "mode", mode, "event_id", eventID)
		data, err := json.Marshal(qualificationCrashWindow{Mode: mode, EventID: eventID})
		if err != nil {
			t.Fatal(err)
		}
		path := os.Getenv("EMITLANE_QUALIFICATION_SIGNAL")
		if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	brokers := strings.Split(os.Getenv("EMITLANE_QUALIFICATION_BROKERS"), ",")
	if mode == "consumer" {
		store, err := pgstore.NewInboxStore(pool)
		if err != nil {
			t.Fatal(err)
		}
		factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{Brokers: brokers,
			Group: os.Getenv("EMITLANE_QUALIFICATION_GROUP"), Topics: []string{os.Getenv("EMITLANE_QUALIFICATION_TOPIC")},
			SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		c := qualificationConsumerConfig(os.Getenv("EMITLANE_QUALIFICATION_CONSUMER"), "qualification-crash-child")
		c.Concurrency = 1
		r, err := managed.New(c, pool, store, factory, func(ctx context.Context, tx pgx.Tx, m managed.Message) error {
			var p qualificationPayload
			if err := json.Unmarshal(m.Payload, &p); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments(order_id,amount) VALUES($1,$2)`, m.EventID.String(), p.Sequence); err != nil {
				return err
			}
			ready(m.EventID)
			return nil
		}, managed.WithLogger(logger))
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Run(ctx); err != nil {
			t.Fatal(err)
		}
		return
	}
	store, err := pgstore.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	// franz-go requires RecordDeliveryTimeout >= 1s. Relay still supplies its
	// tighter 500ms publish context and matching ordering fence.
	pub, err := kafkaadapter.NewPublisher(kafkaadapter.Config{Brokers: brokers, ClientID: "qualification-crash-child", PublishTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	hooks := relay.FailureHooks{}
	hooks.AfterClaimCommit = func(_ context.Context, event relay.Event) error {
		logger.Info("crash helper claim committed", "mode", mode, "event_id", event.ID,
			"ordered", event.OrderingKey != "", "attempts", event.Attempts)
		if mode == "relay_claim_crash" && event.ID == probe {
			ready(event.ID)
		}
		return nil
	}
	switch mode {
	case "relay_claim_crash":
	case "relay_ack_crash":
		hooks.AfterPublishAck = func(_ context.Context, event relay.Event) error {
			if event.ID == probe {
				ready(event.ID)
			}
			return nil
		}
	default:
		t.Fatalf("unknown qualification helper mode %q", mode)
	}
	c := qualificationRelayConfig("qualification-crash-child")
	c.Concurrency = 1
	r, err := relay.New(c, store, pub, relay.WithLogger(logger), relay.WithFailureHooks(hooks))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

// Restore only into the disposable fixture database, never the original run.
// This opt-in reproducer keeps crash-window failures inspectable after teardown.
func TestQualificationCrashHelperFromFailureSnapshot(t *testing.T) {
	dumpPath := os.Getenv("EMITLANE_QUALIFICATION_REPLAY_DUMP")
	if dumpPath == "" {
		t.Skip("opt-in replay of a preserved qualification database")
	}
	output := os.Getenv("EMITLANE_QUALIFICATION_OUTPUT")
	if output == "" {
		t.Fatal("EMITLANE_QUALIFICATION_OUTPUT is required")
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dump, err := os.Open(dumpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer dump.Close()
	restore := exec.CommandContext(ctx, "docker", "exec", "-i", e.postgres.GetContainerID(),
		"pg_restore", "--clean", "--if-exists", "--exit-on-error", "-U", "emitlane", "-d", "emitlane")
	restore.Stdin = dump
	if out, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("restore disposable database: %v %s", err, out)
	}
	e.pool.Reset()
	var topic, consumer string
	if err := e.pool.QueryRow(ctx, `SELECT destination FROM emitlane.outbox_events LIMIT 1`).Scan(&topic); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT consumer FROM emitlane.inbox_events LIMIT 1`).Scan(&consumer); err != nil {
		t.Fatal(err)
	}
	e.ensureTopicPartitions(t, topic, 8)
	var expected, processed, effects int64
	if err := e.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.soak_expected),
 (SELECT count(*) FROM emitlane.inbox_events WHERE status='processed'),
 (SELECT count(*) FROM public.business_payments)`).Scan(&expected, &processed, &effects); err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved ledger: expected=%d processed=%d protected_effects=%d", expected, processed, effects)
	if value := os.Getenv("EMITLANE_QUALIFICATION_REPLAY_RETRIES"); value != "" {
		retries, err := strconv.ParseInt(value, 10, 64)
		if err != nil || retries < 1 {
			t.Fatalf("invalid preserved retry count %q", value)
		}
		total := auditQualificationDatabase(t, ctx, e, topic, consumer, retries)
		t.Logf("preserved completed database audit: committed=%d retry_operations=%d", total, retries)
	}
	rows, err := e.pool.Query(ctx, `SELECT status, ordering_key IS NOT NULL, count(*), min(available_at)
 FROM emitlane.outbox_events GROUP BY status, ordering_key IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var status string
		var ordered bool
		var count int64
		var available time.Time
		if err := rows.Scan(&status, &ordered, &count, &available); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		t.Logf("preserved outbox: status=%s ordered=%t count=%d earliest=%s", status, ordered, count, available)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	probe := killQualificationChild(t, e, qualificationConfig{output: output}, topic, "qualification-replay", consumer, "relay_ack_crash", 0)
	var pendingAck bool
	if err := e.pool.QueryRow(ctx, `SELECT status='inflight' AND attempts=1 AND delivered_at IS NULL FROM emitlane.outbox_events WHERE id=$1`, probe).Scan(&pendingAck); err != nil || !pendingAck {
		t.Fatalf("probe not left in ACK-before-DB-ack window: %t %v", pendingAck, err)
	}
}
