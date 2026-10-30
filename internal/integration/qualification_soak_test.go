//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/inbox"
	"github.com/emitlane/emitlane/integrity"
	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
)

type qualificationPayload struct {
	Seed           int64  `json:"seed"`
	Sequence       int64  `json:"sequence"`
	Stream         string `json:"stream"`
	StreamSequence int64  `json:"stream_sequence"`
	Poison         bool   `json:"poison"`
}

// This is deliberately separate from the fast, one-pass managed regression.
// Every cycle exercises every fault; increasing duration increases coverage.
func TestManagedConsumerQualificationSoak(t *testing.T) {
	if os.Getenv("EMITLANE_QUALIFICATION_DURATION") == "" {
		t.Skip("opt-in repeated qualification soak")
	}
	c, err := readQualificationConfig()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(c.output, 0700); err != nil {
		t.Fatal(err)
	}
	// Refuse to append to evidence from an earlier candidate/run.
	evidence, err := os.OpenFile(filepath.Join(c.output, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer evidence.Close()
	commit, branch, dirty, _ := soakGitProvenance(t)
	if dirty {
		t.Fatal("qualification requires a clean committed source")
	}
	capacity, err := readQualificationCapacity()
	if err != nil {
		t.Fatal(err)
	}
	encode := json.NewEncoder(evidence)
	write := func(value any) {
		if err := encode.Encode(value); err != nil {
			t.Fatal(err)
		}
		if err := evidence.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	write(map[string]any{"kind": "start", "time": time.Now().UTC(), "commit": commit, "branch": branch,
		"duration": c.duration.String(), "interval": c.interval.String(), "rate": c.rate, "seed": c.seed,
		"go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH, "recovery": c.recovery.String(), "audit": c.audit.String(),
		"capacity": capacity, "topic_partitions": c.partitions, "diagnostic_interval": "30s", "operator_repair_interval": qualificationOperatorRepairInterval.String(),
		"rebalance_assignment_timeout": qualificationRebalanceAssignmentTimeout.String(), "durability": "fsync=on synchronous_commit=on full_page_writes=on"})
	if err := validateQualificationRebalanceCapacity(c.partitions, capacity.Consumers); err != nil {
		t.Fatal(err)
	}
	// Absence of PASS (including interruption) is never passing evidence.
	passed := false
	defer func() {
		if !passed {
			_ = json.NewEncoder(evidence).Encode(map[string]any{"kind": "incomplete", "time": time.Now().UTC()})
		}
	}()
	original := startEnv(t)
	e, diagnostics := startQualificationDiagnostics(t, original, c.output)
	defer func() { diagnostics.finish(t, passed) }()
	t.Cleanup(func() { e.restoreKafka(t); e.restorePostgres(t) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := e.pool.Exec(ctx, `
DROP TABLE IF EXISTS public.soak_expected, public.soak_stream_progress, public.soak_effect_progress, public.soak_pruned, public.soak_observed;
CREATE TABLE public.soak_expected (
 event_id UUID PRIMARY KEY, sequence BIGINT UNIQUE NOT NULL,
 stream TEXT NOT NULL, stream_sequence BIGINT NOT NULL, poison BOOLEAN NOT NULL, repaired BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE public.soak_stream_progress (stream TEXT PRIMARY KEY, sequence BIGINT NOT NULL);
INSERT INTO public.soak_stream_progress SELECT 'stream-' || n, 0 FROM generate_series(0,7) n;
CREATE TABLE public.soak_effect_progress (stream TEXT PRIMARY KEY, sequence BIGINT NOT NULL);
INSERT INTO public.soak_effect_progress SELECT stream,0 FROM public.soak_stream_progress;
CREATE TABLE public.soak_pruned (event_id UUID PRIMARY KEY);
CREATE TABLE public.soak_observed (event_id UUID PRIMARY KEY, records BIGINT NOT NULL);
`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			return // Preserve the ledger until the failure dump is complete.
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := e.pool.Exec(cleanupCtx, `DROP TABLE IF EXISTS public.soak_expected, public.soak_stream_progress, public.soak_effect_progress, public.soak_pruned, public.soak_observed`); err != nil {
			t.Error(err)
		}
	})
	setupQualificationInboxAccounting(t, e.pool)
	topic, group, name := "qualification-"+uuid.NewString(), "qualification-group-"+uuid.NewString(), "qualification-v1"
	e.ensureTopicPartitions(t, topic, int32(c.partitions))
	diagnostics.start(topic, group)
	consumerConfig := qualificationConsumerConfig(name, "qualification-main")
	consumerConfig.Concurrency = capacity.Consumers
	store := newManagedInboxStore(t, e)
	factory := &qualificationCommitFactory{inner: &qualificationDiagnosticFactory{inner: newManagedFactory(t, e, topic, group), tracer: diagnostics.tracer}}
	factory.armed.Store(true)
	handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		start := time.Now()
		err := qualificationHandler(ctx, tx, message)
		diagnostics.tracer.observe("business.handler", time.Since(start), err)
		diagnostics.tracer.observe(qualificationPartitionKey(message.Partition, "handler"), time.Since(start), err)
		return err
	}
	consumer := launchManaged(t, e, consumerConfig, store, factory, handler, managed.WithMetrics(diagnostics.metrics))
	defer func() { consumer.stop(t) }()
	pub := e.publisher(t)
	relayCfg := qualificationRelayConfig("qualification-main")
	startRelay := func() *runningQualificationRelay {
		return launchQualificationRelay(e.newRelay(t, relayCfg, pub, relay.FailureHooks{}, relay.WithMetrics(diagnostics.metrics)))
	}
	rly := startRelay()
	defer func() { rly.stop(t) }()
	loadCtx, stopLoad := context.WithTimeout(ctx, c.duration)
	producerDone := make(chan struct{})
	var producerFailures atomic.Int64
	var producerResult error // Read only after producerDone closes.
	go func() {
		defer close(producerDone)
		producerResult = produceQualification(loadCtx, e, topic, c, &producerFailures)
		e.log.Info("qualification producer stopped", "error", producerResult, "context_error", loadCtx.Err())
	}()
	defer func() { stopLoad(); <-producerDone }()
	checkProducer := func() {
		select {
		case <-producerDone:
			if producerResult != nil {
				t.Fatal(producerResult)
			}
			if loadCtx.Err() == nil {
				t.Fatal("qualification producer stopped before the load deadline")
			}
		default:
		}
	}
	rng := rand.New(rand.NewSource(c.seed))
	cycles, retriedDead, pruned := 0, int64(0), int64(0)
	deadline := time.Now().Add(c.duration)
	// Keep operator repairs active between fault cycles. Otherwise a five-minute
	// idle interval accumulates poison-blocked partitions faster than a short
	// fault cycle can drain them, making a long run a growing-backlog test.
	waitHealthy := func(until time.Time) {
		for loadCtx.Err() == nil && time.Now().Before(until) {
			checkProducer()
			if n := retryQualificationDead(t, e, name); n > 0 {
				retriedDead += n
				write(map[string]any{"kind": "operator_retry", "time": time.Now().UTC(), "cycle": cycles, "events": n})
			}
			select {
			case <-loadCtx.Done():
			case <-time.After(min(qualificationOperatorRepairInterval, time.Until(until))):
			}
		}
	}
	faults := []string{"consumer_crash", "relay_claim_crash", "relay_ack_crash", "rebalance", "kafka_restart", "postgres_restart", "pause_resume", "dead_retry_retention"}
	coverage := make(map[string]int)
	for time.Until(deadline) >= 30*time.Second {
		cycleStart := time.Now()
		// Shuffle reproducibly, while preserving every fault in every full cycle.
		rng.Shuffle(len(faults), func(i, j int) { faults[i], faults[j] = faults[j], faults[i] })
		for _, fault := range faults {
			checkProducer()
			write(map[string]any{"kind": "fault_begin", "time": time.Now().UTC(), "cycle": cycles + 1, "fault": fault})
			switch fault {
			case "consumer_crash":
				consumer.stop(t)
				killQualificationChild(t, e, c, topic, group, name, "consumer", cycles)
				consumer = launchManaged(t, e, consumerConfig, store, factory, handler, managed.WithMetrics(diagnostics.metrics))
			case "relay_claim_crash", "relay_ack_crash":
				rly.stop(t)
				killQualificationChild(t, e, c, topic, group, name, fault, cycles)
				rly = startRelay()
			case "rebalance":
				func() {
					other := consumerConfig
					other.InstanceID = "qualification-peer"
					peerFactory := &qualificationCommitFactory{inner: newManagedFactory(t, e, topic, group)}
					peer := launchManaged(t, e, other, store, peerFactory, handler, managed.WithMetrics(diagnostics.metrics))
					defer peer.stop(t)
					// Observe actual assignment, rather than equating elapsed time with
					// a successful group join. Cleanup also covers a failed assertion.
					assignmentStart := time.Now()
					assignmentCtx, stopAssignment := context.WithTimeout(ctx, qualificationRebalanceAssignmentTimeout)
					err := waitQualificationAssignment(assignmentCtx, peerFactory)
					stopAssignment()
					proof := map[string]any{"kind": "rebalance_assignment", "time": time.Now().UTC(), "cycle": cycles + 1,
						"assignments": peerFactory.assignments.Load(), "wait_seconds": time.Since(assignmentStart).Seconds(),
						"timeout": qualificationRebalanceAssignmentTimeout.String()}
					if err != nil {
						proof["kind"], proof["error"] = "rebalance_timeout", err.Error()
						write(proof)
						t.Fatalf("peer consumer received no partition assignment within %s: %v", qualificationRebalanceAssignmentTimeout, err)
					}
					write(proof)
					peer.stop(t)
				}()
			case "kafka_restart":
				e.stopKafka(t)
				time.Sleep(2 * time.Second)
				e.startKafka(t)
			case "postgres_restart":
				e.stopPostgres(t)
				time.Sleep(2 * time.Second)
				e.startPostgres(t)
			case "pause_resume":
				mutation := adminapi.Mutation{Actor: "qualification", Reason: "repeated maintenance drill"}
				if _, err := e.store.SetPaused(ctx, true, mutation); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				// A committed new row while paused must remain recoverable/pending.
				probe := qualificationPauseProbe(t, e, topic, c.seed)
				time.Sleep(200 * time.Millisecond)
				if got := e.eventStatus(t, probe); got != "pending" {
					t.Fatalf("pause probe state=%s", got)
				}
				if _, err := e.store.SetPaused(ctx, false, mutation); err != nil {
					t.Fatal(err)
				}
			case "dead_retry_retention":
				retriedDead += retryQualificationDead(t, e, name)
				// Record proof of delivered state before invoking the public cleanup.
				if _, err := e.pool.Exec(ctx, `INSERT INTO public.soak_pruned SELECT id FROM emitlane.outbox_events WHERE destination=$1 AND status='delivered' ON CONFLICT DO NOTHING`, topic); err != nil {
					t.Fatal(err)
				}
				for {
					n, err := e.store.CleanupDelivered(ctx, 5*time.Second, 1000)
					if err != nil {
						t.Fatal(err)
					}
					pruned += n
					if n < 1000 {
						break
					}
				}
			}
			checkProducer()
			coverage[fault]++
			write(map[string]any{"kind": "fault_end", "time": time.Now().UTC(), "cycle": cycles + 1, "fault": fault})
			if n := retryQualificationDead(t, e, name); n > 0 {
				retriedDead += n
				write(map[string]any{"kind": "operator_retry", "time": time.Now().UTC(), "cycle": cycles + 1, "events": n})
			}
			// Re-arm an offset failure each cycle; require observed injection below.
		}
		factory.armed.Store(true)
		cycles++
		write(qualificationSnapshot(t, e, topic, group, name, cycles, producerFailures.Load()))
		if wait := c.interval - time.Since(cycleStart); wait > 0 {
			waitHealthy(cycleStart.Add(c.interval))
		}
	}
	waitHealthy(deadline)
	<-loadCtx.Done()
	<-producerDone
	checkProducer()
	stopLoad()
	if cycles < 2 {
		t.Fatalf("only %d full fault cycles completed; increase duration", cycles)
	}
	// Fast offset progress drives recovery. Do not repeatedly join millions of
	// ledger rows while the consumer is trying to catch up. The independent full
	// identity/effect audit still runs after recovery and remains mandatory.
	recoveryStart := time.Now()
	recoveryEnd := recoveryStart.Add(c.recovery)
	write(qualificationSnapshot(t, e, topic, group, name, cycles, producerFailures.Load()))
	loadEndCtx, stopLoadEnd := context.WithTimeout(ctx, 10*time.Second)
	loadEndKafka, err := qualificationKafkaSnapshot(loadEndCtx, e.brokers, topic, group)
	stopLoadEnd()
	if err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"kind": "recovery_begin", "time": time.Now().UTC(), "timeout": c.recovery.String(), "kafka": loadEndKafka})
	var remaining int64
	lastProgress := time.Time{}
	for {
		retriedDead += retryQualificationDead(t, e, name)
		if time.Since(lastProgress) < 5*time.Second && time.Now().Before(recoveryEnd) {
			time.Sleep(qualificationOperatorRepairInterval)
			continue
		}
		lastProgress = time.Now()
		progressCtx, stopProgress := context.WithTimeout(ctx, 5*time.Second)
		kafka, err := qualificationKafkaSnapshot(progressCtx, e.brokers, topic, group)
		stopProgress()
		if err != nil {
			t.Fatal(err)
		}
		write(map[string]any{"kind": "recovery_progress", "time": time.Now().UTC(), "elapsed_seconds": time.Since(recoveryStart).Seconds(), "kafka": kafka})
		if kafka["lag"].(int64) == 0 || time.Now().After(recoveryEnd) {
			checkCtx, stopCheck := context.WithTimeout(ctx, 30*time.Second)
			err := e.pool.QueryRow(checkCtx, `SELECT COUNT(*) FROM public.soak_expected x LEFT JOIN emitlane.inbox_events i ON i.consumer=$1 AND i.event_id=x.event_id LEFT JOIN emitlane.outbox_events o ON o.id=x.event_id WHERE i.status IS DISTINCT FROM 'processed' OR (o.id IS NOT NULL AND o.status<>'delivered')`, name).Scan(&remaining)
			stopCheck()
			if err != nil {
				t.Fatal(err)
			}
			if remaining == 0 && kafka["lag"].(int64) == 0 {
				break
			}
		}
		if time.Now().After(recoveryEnd) {
			t.Fatalf("recovery exceeded %s; %d unfinished events, Kafka lag=%v", c.recovery, remaining, kafka["lag"])
		}
		time.Sleep(qualificationOperatorRepairInterval)
	}
	recoverySeconds := time.Since(recoveryStart).Seconds()
	write(map[string]any{"kind": "recovery_end", "time": time.Now().UTC(), "elapsed_seconds": recoverySeconds, "remaining": remaining})

	consumer.stop(t)
	rly.stop(t)
	if retriedDead == 0 || pruned == 0 || factory.injected.Load() < 2 {
		t.Fatalf("missing coverage: dead retries=%d pruned=%d offset failures=%d", retriedDead, pruned, factory.injected.Load())
	}
	auditCtx, stopAudit := context.WithTimeout(ctx, c.audit)
	defer stopAudit()
	// Persist the independent operation counter before an audit can fail. The
	// per-action records omit retries made inside the retention fault itself.
	write(map[string]any{"kind": "audit_begin", "time": time.Now().UTC(), "retried_dead": retriedDead,
		"pruned": pruned, "offset_failures": factory.injected.Load()})
	kafkaRecords, duplicates := auditQualificationKafka(t, auditCtx, e, topic, c.seed, c.partitions)
	total := auditQualificationDatabase(t, auditCtx, e, topic, name, retriedDead)
	if err := reconcileQualificationInboxAccounting(auditCtx, e.pool); err != nil {
		t.Fatal(err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(auditCtx, integrity.ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Violations != 0 || report.Summary.InboxStaleLeases != 0 {
		t.Fatalf("qualification integrity: %+v", report)
	}
	endCommit, _, endDirty, _ := soakGitProvenance(t)
	if endCommit != commit || endDirty {
		t.Fatal("source changed during qualification")
	}
	write(qualificationSnapshot(t, e, topic, group, name, cycles, producerFailures.Load()))
	write(map[string]any{"kind": "PASS", "time": time.Now().UTC(), "commit": commit,
		"cycles": cycles, "coverage": coverage, "committed": total, "kafka_records": kafkaRecords, "kafka_duplicates": duplicates,
		"load_end_kafka": loadEndKafka, "recovery_seconds": recoverySeconds, "recovery_budget": c.recovery.String(),
		"retried_dead": retriedDead, "pruned": pruned, "offset_failures": factory.injected.Load(), "integrity": report})
	passed = true
	t.Logf("qualification PASS commit=%s cycles=%d committed=%d kafka_records=%d duplicates=%d dead_retries=%d pruned=%d offset_failures=%d", commit, cycles, total, kafkaRecords, duplicates, retriedDead, pruned, factory.injected.Load())
}

// Repair polling is part of the simulated operator, not consumer retry policy.
// A one-second check imposed extra head-of-line stalls after retry exhaustion.
const qualificationOperatorRepairInterval = 100 * time.Millisecond

func qualificationConsumerConfig(name, instance string) managed.Config {
	c := managedTestConfig(name, instance)
	c.Concurrency = 2
	c.MaxAttempts = 8
	c.BaseDelay = 50 * time.Millisecond
	c.MaxDelay = 500 * time.Millisecond
	c.Jitter = 0
	return c
}

func qualificationRelayConfig(instance string) relay.Config {
	c := relay.DefaultConfig()
	c.InstanceID = instance
	c.Concurrency = 2
	c.BatchSize = 20
	c.PollInterval = 20 * time.Millisecond
	c.IdleBackoffMax = 100 * time.Millisecond
	c.LeaseDuration = 2 * time.Second
	c.PublishTimeout = 500 * time.Millisecond
	c.MaxAttempts = 100
	c.BaseDelay = 50 * time.Millisecond
	c.MaxDelay = 500 * time.Millisecond
	c.ShutdownTimeout = 5 * time.Second
	c.OrderingLeaseDuration = 3 * time.Second
	c.OrderingSafetyMargin = 100 * time.Millisecond
	c.OrderingRebalanceInterval = 100 * time.Millisecond
	c.HeartbeatInterval = 200 * time.Millisecond
	c.PresenceStaleAfter = time.Second
	c.ControlInterval = 50 * time.Millisecond
	c.StatsInterval = 0
	c.Retention = 0
	c.CleanupInterval = 0
	return c
}

func qualificationHandler(ctx context.Context, tx pgx.Tx, message managed.Message) error {
	var p qualificationPayload
	if err := json.Unmarshal(message.Payload, &p); err != nil {
		return inbox.Permanent(err)
	}
	if p.Poison {
		var repaired bool
		if err := tx.QueryRow(ctx, `SELECT repaired FROM public.soak_expected WHERE event_id=$1`, message.EventID).Scan(&repaired); err != nil {
			return err
		}
		if !repaired {
			return errors.New("injected poison; requires operator repair")
		}
	}
	if p.Sequence%19 == 0 && message.Attempt == 1 {
		return errors.New("injected transient handler failure")
	}
	if p.Stream != "" {
		tag, err := tx.Exec(ctx, `UPDATE public.soak_effect_progress SET sequence=$2 WHERE stream=$1 AND sequence=$2-1`, p.Stream, p.StreamSequence)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return inbox.Permanent(fmt.Errorf("business stream %s expected predecessor of %d", p.Stream, p.StreamSequence))
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id,amount) VALUES ($1,$2)`, message.EventID.String(), p.Sequence)
	return err
}

func produceQualification(ctx context.Context, e *env, topic string, c qualificationConfig, failures *atomic.Int64) error {
	ticker := time.NewTicker(time.Second / time.Duration(c.rate))
	defer ticker.Stop()
	// Count the real producer transaction outcomes in bounded time windows.
	// The ticker can drop ticks while a synchronous enqueue blocks; configured
	// rate alone must never be reported as admitted throughput. A successful
	// return is distinct from a final identity audit after an ambiguous commit.
	windowStart := time.Now()
	var attempted, successful, failed int64
	var enqueueTime, maxEnqueue time.Duration
	report := func(force bool) {
		now := time.Now()
		if !force && now.Sub(windowStart) < 30*time.Second {
			return
		}
		e.log.Info("qualification producer interval", "started_at", windowStart.UTC(), "ended_at", now.UTC(),
			"seconds", now.Sub(windowStart).Seconds(), "target_rate", c.rate,
			"scheduled_slots_estimate", now.Sub(windowStart).Seconds()*float64(c.rate),
			"attempted", attempted, "commit_returns_success", successful, "errors", failed,
			"enqueue_seconds", enqueueTime.Seconds(), "enqueue_max_seconds", maxEnqueue.Seconds())
		windowStart, attempted, successful, failed, enqueueTime, maxEnqueue = now, 0, 0, 0, 0, 0
	}
	defer report(true)
	for seq := int64(1); ; seq++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		// Sequence allocation is transactional, so an ambiguous DB commit does
		// not create an ordered gap in a Go-side counter.
		p := qualificationPayload{Seed: c.seed, Sequence: seq, Poison: seq%37 == 0}
		if seq%2 == 0 {
			p.Stream = fmt.Sprintf("stream-%d", (seq/2)%8)
		}
		started := time.Now()
		err := enqueueQualification(ctx, e, topic, uuid.NewString(), p)
		elapsed := time.Since(started)
		attempted++
		enqueueTime += elapsed
		maxEnqueue = max(maxEnqueue, elapsed)
		if err == nil {
			successful++
		} else {
			failed++
		}
		report(false)
		if errors.Is(err, outbox.ErrInvalidEvent) || errors.Is(err, outbox.ErrDuplicateSequence) || errors.Is(err, outbox.ErrOrderingConflict) || errors.Is(err, outbox.ErrSequenceAlreadyPassed) {
			return fmt.Errorf("qualification producer: %w", err)
		}
		if err != nil && ctx.Err() == nil {
			failures.Add(1)
		}
	}
}

func enqueueQualification(ctx context.Context, e *env, topic, id string, p qualificationPayload) error {
	return enqueueQualificationAt(ctx, e, topic, id, p, time.Time{})
}

func enqueueQualificationAt(ctx context.Context, e *env, topic, id string, p qualificationPayload, availableAt time.Time) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if p.Stream != "" {
		if err := tx.QueryRow(ctx, `UPDATE public.soak_stream_progress SET sequence=sequence+1 WHERE stream=$1 RETURNING sequence`, p.Stream).Scan(&p.StreamSequence); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.business_orders(id,amount) VALUES ($1,$2)`, "soak-"+id, p.Sequence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.soak_expected(event_id,sequence,stream,stream_sequence,poison) VALUES($1,$2,$3,$4,$5)`, id, p.Sequence, p.Stream, p.StreamSequence, p.Poison); err != nil {
		return err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	key := id
	if p.Stream != "" {
		key = p.Stream
	}
	_, err = e.writer.Enqueue(ctx, tx, outbox.Event{ID: id, Destination: topic, Type: "qualification.input", Key: []byte(key), Payload: payload, OrderingKey: p.Stream, Sequence: p.StreamSequence, AvailableAt: availableAt})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func qualificationPauseProbe(t *testing.T, e *env, topic string, seed int64) string {
	t.Helper()
	return qualificationProbe(t, e, topic, seed, time.Time{})
}

func qualificationProbe(t *testing.T, e *env, topic string, seed int64, availableAt time.Time) string {
	t.Helper()
	id := uuid.NewString()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	// Negative unique sequence distinguishes operator probes from normal load.
	var seq int64
	if err := e.pool.QueryRow(ctx, `SELECT COALESCE(MIN(sequence),0)-1 FROM public.soak_expected WHERE sequence<0`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if err := enqueueQualificationAt(ctx, e, topic, id, qualificationPayload{Seed: seed, Sequence: seq}, availableAt); err != nil {
		t.Fatal(err)
	}
	return id
}

func retryQualificationDead(t *testing.T, e *env, name string) int64 {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	rows, err := e.pool.Query(ctx, `SELECT i.event_id,x.poison,i.attempts FROM emitlane.inbox_events i LEFT JOIN public.soak_expected x ON x.event_id=i.event_id WHERE i.consumer=$1 AND i.status='dead' LIMIT 100`, name)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		var poison *bool
		var attempts int
		if err := rows.Scan(&id, &poison, &attempts); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if poison == nil || !*poison {
			rows.Close()
			t.Fatalf("unexpected dead event %s", id)
		}
		ids = append(ids, id)
		if attempts < qualificationConsumerConfig(name, "").MaxAttempts {
			rows.Close()
			t.Fatalf("poison %s became dead before retry exhaustion: %d attempts", id, attempts)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := newManagedInboxStore(t, e)
	for _, id := range ids {
		if _, err := e.pool.Exec(ctx, `UPDATE public.soak_expected SET repaired=TRUE WHERE event_id=$1`, id); err != nil {
			t.Fatal(err)
		}
		if err := store.RetryDead(ctx, inbox.RetryRequest{Consumer: name, EventID: id, Actor: "qualification", Reason: "poison input repaired"}); err != nil {
			t.Fatal(err)
		}
	}
	return int64(len(ids))
}

type runningQualificationRelay struct {
	cancel  context.CancelFunc
	done    chan error
	stopped bool
}

func launchQualificationRelay(r *relay.Relay) *runningQualificationRelay {
	ctx, cancel := context.WithCancel(context.Background())
	run := &runningQualificationRelay{cancel: cancel, done: make(chan error, 1)}
	go func() { run.done <- r.Run(ctx) }()
	return run
}
func (r *runningQualificationRelay) stop(t *testing.T) {
	t.Helper()
	if r.stopped {
		return
	}
	r.stopped = true
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("qualification relay: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("qualification relay did not stop")
	}
}

type qualificationCommitFactory struct {
	inner       managed.SourceFactory
	armed       atomic.Bool
	injected    atomic.Int64
	assignments atomic.Int64
	joins       atomic.Int64
}

func (f *qualificationCommitFactory) NewSource(id string, listener managed.RebalanceListener) (managed.Source, error) {
	s, err := f.inner.NewSource(id, &qualificationAssignmentListener{RebalanceListener: listener, factory: f})
	if err != nil {
		return nil, err
	}
	return &qualificationCommitSource{Source: s, factory: f}, nil
}

type qualificationAssignmentListener struct {
	managed.RebalanceListener
	factory *qualificationCommitFactory
}

func (l *qualificationAssignmentListener) Assigned(partitions map[string][]int32) {
	l.factory.joins.Add(1)
	for _, p := range partitions {
		l.factory.assignments.Add(int64(len(p)))
	}
	l.RebalanceListener.Assigned(partitions)
}

type qualificationCommitSource struct {
	managed.Source
	factory *qualificationCommitFactory
}

func (s *qualificationCommitSource) Commit(ctx context.Context, record managed.SourceRecord) error {
	if s.factory.armed.CompareAndSwap(true, false) {
		s.factory.injected.Add(1)
		return errors.New("repeated offset commit failure")
	}
	return s.Source.Commit(ctx, record)
}

func qualificationSnapshot(t *testing.T, e *env, topic, group, name string, cycles int, failures int64) map[string]any {
	t.Helper()
	started := time.Now()
	ctx, stop := context.WithTimeout(context.Background(), qualificationSnapshotTimeout)
	defer stop()
	counts, err := readQualificationInboxSnapshot(ctx, e.pool, name)
	if err != nil {
		failQualificationSnapshot(t, ctx, started, "inbox_counts", err)
	}
	var dbBytes, connections, deadTuples, autovacuums int64
	if err := e.pool.QueryRow(ctx, `SELECT pg_database_size(current_database()),(SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database()),COALESCE((SELECT SUM(n_dead_tup) FROM pg_stat_user_tables),0),COALESCE((SELECT SUM(autovacuum_count) FROM pg_stat_user_tables),0)`).Scan(&dbBytes, &connections, &deadTuples, &autovacuums); err != nil {
		failQualificationSnapshot(t, ctx, started, "database_stats", err)
	}
	var outboxPending, outboxInflight, outboxDead int64
	var oldest float64
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FILTER(WHERE status='pending'),COUNT(*) FILTER(WHERE status='inflight'),COUNT(*) FILTER(WHERE status='dead'),COALESCE(MAX(EXTRACT(EPOCH FROM NOW()-created_at)) FILTER(WHERE status='pending'),0) FROM emitlane.outbox_events`).Scan(&outboxPending, &outboxInflight, &outboxDead, &oldest); err != nil {
		failQualificationSnapshot(t, ctx, started, "outbox_counts", err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	kafka, err := qualificationKafkaSnapshot(ctx, e.brokers, topic, group)
	if err != nil {
		failQualificationSnapshot(t, ctx, started, "kafka_offsets", err)
	}
	return map[string]any{"kind": "snapshot", "time": time.Now().UTC(), "cycles": cycles, "producer_failures": failures,
		"inbox_pending": counts[0], "inbox_inflight": counts[1], "inbox_retry": counts[2], "inbox_dead": counts[3], "processed": counts[4],
		"go_heap_bytes": memory.HeapAlloc, "goroutines": runtime.NumGoroutine(), "pool_acquired": e.pool.Stat().AcquiredConns(), "pool_total": e.pool.Stat().TotalConns(),
		"database_bytes": dbBytes, "database_connections": connections, "dead_tuples_estimate": deadTuples, "autovacuum_count": autovacuums,
		"outbox_pending": outboxPending, "outbox_inflight": outboxInflight, "outbox_dead": outboxDead, "oldest_pending_seconds": oldest, "kafka": kafka}
}
