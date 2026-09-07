//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/integrity"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
)

func TestManagedConsumerReliabilitySoak(t *testing.T) {
	rawDuration := os.Getenv("EMITLANE_CONSUMER_SOAK_DURATION")
	if rawDuration == "" {
		t.Skip("set EMITLANE_CONSUMER_SOAK_DURATION=60s to run the consumer soak")
	}
	duration, err := time.ParseDuration(rawDuration)
	if err != nil || duration < 20*time.Second {
		t.Fatalf("EMITLANE_CONSUMER_SOAK_DURATION must be at least 20s: %q", rawDuration)
	}
	e := startEnv(t)
	t.Cleanup(func() {
		e.restoreKafka(t)
		e.restorePostgres(t)
	})
	seed := os.Getenv("EMITLANE_CONSUMER_SOAK_SEED")
	if seed == "" {
		seed = "20260907"
	}
	topic := "managed-soak-" + uuid.NewString()
	group := "managed-soak-group-" + uuid.NewString()
	consumerName := "managed-soak-v1"
	e.ensureTopicPartitions(t, topic, 8)

	producerPublisher := e.publisher(t)
	relayConfig := relay.Config{
		InstanceID: "managed-soak-relay-a", Concurrency: 4, BatchSize: 50,
		PollInterval: 20 * time.Millisecond, LeaseDuration: 5 * time.Second,
		MaxAttempts: 100, BaseDelay: 20 * time.Millisecond, MaxDelay: 500 * time.Millisecond,
		PublishTimeout: time.Second,
	}
	runRelay(t, e.newRelay(t, relayConfig, producerPublisher, relay.FailureHooks{}))

	store := newManagedInboxStore(t, e)
	configA := managedTestConfig(consumerName, "managed-soak-a")
	configA.Concurrency = 2
	configA.MaxAttempts = 5
	configA.BaseDelay, configA.MaxDelay, configA.Jitter = 20*time.Millisecond, 100*time.Millisecond, 0
	commitFailure := make(chan struct{}, 1)
	failFactory := &failFirstCommitFactory{inner: newManagedFactory(t, e, topic, group), failed: commitFailure}
	handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		if int(message.EventID[0])%19 == 0 && message.Attempt == 1 {
			return errors.New("injected retryable handler failure")
		}
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`,
			message.EventID.String(), message.Attempt)
		return err
	}
	consumerA := launchManaged(t, e, configA, store, failFactory, handler)

	soakCtx, stopSoak := context.WithTimeout(context.Background(), duration)
	defer stopSoak()
	var producerFailures atomic.Int64
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		writer := outbox.NewWriter()
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		for sequence := int64(1); ; sequence++ {
			select {
			case <-soakCtx.Done():
				return
			case <-ticker.C:
			}
			eventID := uuid.New()
			tx, err := e.pool.Begin(soakCtx)
			if err != nil {
				producerFailures.Add(1)
				continue
			}
			if _, err = tx.Exec(soakCtx, `INSERT INTO public.business_orders (id, amount) VALUES ($1, $2)`,
				"soak-"+eventID.String(), sequence); err == nil {
				_, err = writer.Enqueue(soakCtx, tx, outbox.Event{
					ID: eventID.String(), Destination: topic, Type: "soak.input",
					Key: []byte(eventID.String()), Payload: []byte(seed),
				})
			}
			if err == nil {
				err = tx.Commit(soakCtx)
			}
			if err != nil {
				_ = tx.Rollback(context.Background())
			}
			if err != nil && soakCtx.Err() == nil {
				producerFailures.Add(1)
			}
		}
	}()

	phase := duration / 5
	waitSoakPhase(t, soakCtx, phase)
	configB := configA
	configB.InstanceID = "managed-soak-b"
	consumerB := launchManaged(t, e, configB, store, newManagedFactory(t, e, topic, group), handler)
	defer consumerB.stop(t)

	waitSoakPhase(t, soakCtx, phase)
	consumerA.stop(t)

	waitSoakPhase(t, soakCtx, phase)
	e.stopKafka(t)
	waitSoakPhase(t, soakCtx, min(2*time.Second, phase/2))
	e.startKafka(t)

	waitSoakPhase(t, soakCtx, phase)
	e.stopPostgres(t)
	waitSoakPhase(t, soakCtx, min(2*time.Second, phase/2))
	e.startPostgres(t)
	relayConfig.InstanceID = "managed-soak-relay-b"
	runRelay(t, e.newRelay(t, relayConfig, e.publisher(t), relay.FailureHooks{}))

	<-soakCtx.Done()
	<-producerDone
	recoveryDeadline := time.Now().Add(90 * time.Second)
	var total, delivered, outboxDead, processed, inboxPending, inboxInflight, retryWait, inboxDead, business, retried int64
	for time.Now().Before(recoveryDeadline) {
		queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := e.pool.QueryRow(queryCtx, `
SELECT COUNT(*), COUNT(*) FILTER (WHERE status='delivered'), COUNT(*) FILTER (WHERE status='dead')
FROM emitlane.outbox_events WHERE destination=$1`, topic).Scan(&total, &delivered, &outboxDead)
		if err == nil {
			err = e.pool.QueryRow(queryCtx, `
SELECT COUNT(*) FILTER (WHERE status='processed'),
       COUNT(*) FILTER (WHERE status='pending'),
       COUNT(*) FILTER (WHERE status='inflight'),
       COUNT(*) FILTER (WHERE status='retry_wait'),
       COUNT(*) FILTER (WHERE status='dead'),
       COUNT(*) FILTER (WHERE attempts > 1)
FROM emitlane.inbox_events WHERE consumer=$1`, consumerName).Scan(
				&processed, &inboxPending, &inboxInflight, &retryWait, &inboxDead, &retried)
		}
		if err == nil {
			err = e.pool.QueryRow(queryCtx, `SELECT COUNT(*) FROM public.business_payments`).Scan(&business)
		}
		cancel()
		if err == nil && total > 0 && delivered == total && processed == total && business == total &&
			outboxDead == 0 && inboxPending == 0 && inboxInflight == 0 && retryWait == 0 && inboxDead == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if total == 0 || delivered != total || processed != total || business != total ||
		outboxDead != 0 || inboxPending != 0 || inboxInflight != 0 || retryWait != 0 || inboxDead != 0 {
		t.Fatalf("soak did not quiesce: outbox=%d/%d dead=%d inbox processed=%d pending=%d inflight=%d retry=%d dead=%d business=%d",
			delivered, total, outboxDead, processed, inboxPending, inboxInflight, retryWait, inboxDead, business)
	}
	if !failFactory.done.Load() {
		t.Fatal("soak did not exercise offset commit recovery")
	}
	if retried == 0 {
		t.Fatal("soak did not exercise durable handler retry")
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(context.Background(), integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Violations != 0 || report.Summary.InboxStaleLeases != 0 {
		t.Fatalf("soak integrity result: %+v", report)
	}
	commit, branch, dirty, diffHash := soakGitProvenance(t)
	t.Logf("consumer_soak result=PASS profile=short duration=%s seed=%s platform=%s/%s git_commit=%s git_branch=%s git_dirty=%t git_diff_sha256=%s committed=%d processed=%d protected_effects=%d retries=%d producer_transient_failures=%d",
		duration, seed, runtime.GOOS, runtime.GOARCH, commit, branch, dirty, diffHash,
		total, processed, business, retried, producerFailures.Load())
}

func waitSoakPhase(t *testing.T, ctx context.Context, duration time.Duration) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatal("consumer soak ended before all fault phases ran")
	case <-time.After(duration):
	}
}

func soakGitProvenance(t *testing.T) (commit, branch string, dirty bool, diffHash string) {
	t.Helper()
	run := func(args ...string) string {
		output, err := exec.Command("git", args...).Output()
		if err != nil {
			t.Fatalf("read soak Git provenance: %v", err)
		}
		return strings.TrimSpace(string(output))
	}
	commit = run("rev-parse", "HEAD")
	branch = run("branch", "--show-current")
	status := run("status", "--porcelain")
	dirty = status != ""
	if dirty {
		diff, err := exec.Command("git", "diff", "--binary", "HEAD").Output()
		if err != nil {
			t.Fatalf("hash soak Git diff: %v", err)
		}
		untracked := run("ls-files", "--others", "--exclude-standard")
		for _, path := range strings.Fields(untracked) {
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("hash untracked soak input %s: %v", path, err)
			}
			diff = append(diff, []byte("\nuntracked:"+path+"\n")...)
			diff = append(diff, contents...)
		}
		diffHash = fmt.Sprintf("%x", sha256.Sum256(diff))
	}
	return commit, branch, dirty, diffHash
}
