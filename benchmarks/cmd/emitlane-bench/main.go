package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
	"github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
	"github.com/emitlane/emitlane/storage/postgres"
)

type options struct {
	scenario    string
	events      int
	streams     int
	relays      int
	duration    time.Duration
	output      string
	payloadSize int
}

type result struct {
	Scenario          string         `json:"scenario"`
	StartedAt         time.Time      `json:"started_at"`
	GoVersion         string         `json:"go_version"`
	OS                string         `json:"os"`
	Arch              string         `json:"arch"`
	Events            int            `json:"events"`
	RelayInstances    int            `json:"relay_instances"`
	DurationSeconds   float64        `json:"duration_seconds"`
	EventsPerSecond   float64        `json:"events_per_second,omitempty"`
	LatencyP50Millis  float64        `json:"latency_p50_ms,omitempty"`
	LatencyP95Millis  float64        `json:"latency_p95_ms,omitempty"`
	LatencyP99Millis  float64        `json:"latency_p99_ms,omitempty"`
	LatencyP999Millis float64        `json:"latency_p999_ms,omitempty"`
	Configuration     map[string]any `json:"configuration"`
	AdditionalResults map[string]any `json:"additional_results,omitempty"`
	EmitLaneVersion   string         `json:"emitlane_version"`
	EmitLaneCommit    string         `json:"emitlane_commit"`
	CPUCount          int            `json:"cpu_count"`
	MemoryBytes       uint64         `json:"memory_bytes"`
	PostgreSQL        map[string]any `json:"postgresql"`
	Kafka             map[string]any `json:"kafka"`
	PayloadSize       int            `json:"payload_size"`
}

func main() {
	var opts options
	flag.StringVar(&opts.scenario, "scenario", "steady-state", "enqueue-overhead, steady-state, backlog-drain, horizontal-scaling, idle-overhead, failure-recovery, ack-crash, ordered-many-streams, ordered-hot-stream, or unordered-regression")
	flag.IntVar(&opts.events, "events", 1000, "number of events")
	flag.IntVar(&opts.streams, "streams", 1000, "ordered streams for ordered-many-streams")
	flag.IntVar(&opts.relays, "relays", 2, "relay instances for horizontal scaling")
	flag.DurationVar(&opts.duration, "duration", 3*time.Second, "idle or failure duration")
	flag.StringVar(&opts.output, "output", "", "optional JSON output file")
	flag.IntVar(&opts.payloadSize, "payload-size", 1024, "payload size in bytes")
	flag.Parse()
	if opts.events < 1 || opts.streams < 1 || opts.relays < 1 || opts.duration <= 0 || opts.payloadSize < 1 {
		fatal(errors.New("events, streams, relays, duration and payload-size must be positive"))
	}
	if err := run(opts); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "emitlane-bench:", err)
	os.Exit(1)
}

func run(opts options) error {
	payload := makePayload(opts.payloadSize)
	databaseURL := strings.TrimSpace(os.Getenv("EMITLANE_DATABASE_URL"))
	brokers := splitCSV(os.Getenv("EMITLANE_KAFKA_BROKERS"))
	if databaseURL == "" {
		return errors.New("EMITLANE_DATABASE_URL is required")
	}
	if len(brokers) == 0 && opts.scenario != "enqueue-overhead" && opts.scenario != "idle-overhead" {
		return errors.New("EMITLANE_KAFKA_BROKERS is required for this scenario")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}
	version, err := postgres.SchemaVersion(ctx, pool)
	if err != nil {
		return err
	}
	if version != postgres.CurrentSchemaVersion() {
		return fmt.Errorf("schema version %d, want %d; run migrations first", version, postgres.CurrentSchemaVersion())
	}
	store, err := postgres.NewStore(pool)
	if err != nil {
		return err
	}

	started := time.Now().UTC()
	buildVersion, buildCommit := buildIdentity()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	pgMetadata, err := postgresMetadata(ctx, pool)
	if err != nil {
		return err
	}
	res := result{
		Scenario: opts.scenario, StartedAt: started, GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		Events: opts.events, RelayInstances: 1,
		Configuration: map[string]any{
			"database": redactDatabase(databaseURL), "requested_duration": opts.duration.String(),
			"batch_size": 200, "concurrency": 16, "poll_interval": "100ms", "lease_duration": "3s", "warmup_duration": "0s",
		},
		EmitLaneVersion: buildVersion,
		EmitLaneCommit:  buildCommit,
		CPUCount:        runtime.NumCPU(),
		MemoryBytes:     memory.Sys,
		PostgreSQL:      pgMetadata,
		Kafka: map[string]any{
			"version":      envOr("EMITLANE_BENCH_KAFKA_VERSION", "unknown"),
			"broker_count": len(brokers), "required_acks": "all",
		},
		PayloadSize: opts.payloadSize,
	}
	var elapsed time.Duration
	switch opts.scenario {
	case "enqueue-overhead":
		elapsed, res.AdditionalResults, err = benchmarkEnqueue(ctx, pool, opts.events, payload)
	case "steady-state":
		elapsed, res.AdditionalResults, err = benchmarkDelivery(ctx, pool, store, brokers, opts.events, payload, 1, false)
	case "backlog-drain":
		elapsed, res.AdditionalResults, err = benchmarkDelivery(ctx, pool, store, brokers, opts.events, payload, 1, true)
	case "horizontal-scaling":
		res.RelayInstances = opts.relays
		elapsed, res.AdditionalResults, err = benchmarkDelivery(ctx, pool, store, brokers, opts.events, payload, opts.relays, true)
	case "idle-overhead":
		elapsed, res.AdditionalResults, err = benchmarkIdle(ctx, pool, store, opts.duration)
	case "failure-recovery":
		elapsed, res.AdditionalResults, err = benchmarkFailureRecovery(ctx, pool, store, brokers, opts.events, payload, opts.duration)
	case "ack-crash":
		elapsed, res.AdditionalResults, err = benchmarkAckCrash(ctx, pool, store, brokers, opts.events, payload)
	case "ordered-many-streams":
		res.RelayInstances = opts.relays
		streamCount := min(opts.streams, opts.events)
		res.Configuration["ordered_streams"] = streamCount
		res.Configuration["virtual_partitions"] = 64
		elapsed, res.AdditionalResults, err = benchmarkOrderedDelivery(ctx, pool, store, brokers, opts.events, payload, opts.relays, streamCount)
	case "ordered-hot-stream":
		res.RelayInstances = opts.relays
		res.Configuration["ordered_streams"] = 1
		res.Configuration["virtual_partitions"] = 64
		elapsed, res.AdditionalResults, err = benchmarkOrderedDelivery(ctx, pool, store, brokers, opts.events, payload, opts.relays, 1)
	case "unordered-regression":
		res.RelayInstances = opts.relays
		before, txErr := databaseTransactions(ctx, pool)
		if txErr != nil {
			return txErr
		}
		elapsed, res.AdditionalResults, err = benchmarkDelivery(ctx, pool, store, brokers, opts.events, payload, opts.relays, true)
		if err == nil {
			after, txErr := databaseTransactions(ctx, pool)
			if txErr != nil {
				return txErr
			}
			res.AdditionalResults["database_transactions"] = max(int64(0), after-before-1)
			res.AdditionalResults["ordering_stream_operations_per_event"] = 0
			res.AdditionalResults["comparison_note"] = "compare with a v0.2.0 run using identical environment metadata"
		}
	default:
		return fmt.Errorf("unknown scenario %q", opts.scenario)
	}
	if err != nil {
		return err
	}
	res.DurationSeconds = elapsed.Seconds()
	if elapsed > 0 {
		res.EventsPerSecond = float64(opts.events) / elapsed.Seconds()
	}
	if latencies, ok := res.AdditionalResults["latencies_ms"].([]float64); ok && len(latencies) > 0 {
		res.LatencyP50Millis = percentile(latencies, 0.50)
		res.LatencyP95Millis = percentile(latencies, 0.95)
		res.LatencyP99Millis = percentile(latencies, 0.99)
		if len(latencies) >= 1000 {
			res.LatencyP999Millis = percentile(latencies, 0.999)
		}
		delete(res.AdditionalResults, "latencies_ms")
	}
	return outputResult(res, opts.output)
}

func benchmarkOrderedDelivery(
	ctx context.Context,
	pool *pgxpool.Pool,
	store *postgres.Store,
	brokers []string,
	count int,
	payload []byte,
	relayCount int,
	streamCount int,
) (time.Duration, map[string]any, error) {
	publisher, err := newPublisher(brokers)
	if err != nil {
		return 0, nil, err
	}
	defer publisher.Close()
	destination := "benchmark-ordered-" + uuid.NewString()
	beforeTransactions, err := databaseTransactions(ctx, pool)
	if err != nil {
		return 0, nil, err
	}
	if _, err := enqueueOrdered(ctx, pool, destination, count, streamCount, payload); err != nil {
		return 0, nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make([]<-chan error, 0, relayCount)
	for i := 0; i < relayCount; i++ {
		rly, err := newOrderedRelay(store, publisher, fmt.Sprintf("benchmark-ordered-%d", i))
		if err != nil {
			cancel()
			return 0, nil, err
		}
		done = append(done, startRelay(runCtx, rly))
	}
	defer stopRelays(cancel, done)
	start := time.Now()
	if err := waitDelivered(ctx, pool, destination, count); err != nil {
		return 0, nil, err
	}
	elapsed := time.Since(start)
	latencies, err := deliveryLatencies(ctx, pool, destination)
	if err != nil {
		return 0, nil, err
	}
	afterTransactions, err := databaseTransactions(ctx, pool)
	if err != nil {
		return 0, nil, err
	}
	distribution, err := orderingPartitionDistribution(ctx, pool, destination)
	if err != nil {
		return 0, nil, err
	}
	return elapsed, map[string]any{
		"latencies_ms": latencies, "committed_events": count,
		"delivered_unique_events": count, "lost_events": 0,
		"ordered_streams": streamCount, "partition_distribution": distribution,
		"database_transactions": max(int64(0), afterTransactions-beforeTransactions-1),
	}, nil
}

func benchmarkEnqueue(ctx context.Context, pool *pgxpool.Pool, count int, payload []byte) (time.Duration, map[string]any, error) {
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.emitlane_benchmark_work (id UUID PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		return 0, nil, err
	}
	baseline := make([]float64, 0, count)
	start := time.Now()
	for i := 0; i < count; i++ {
		began := time.Now()
		tx, err := pool.Begin(ctx)
		if err != nil {
			return 0, nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO public.emitlane_benchmark_work (id) VALUES ($1)`, uuid.New()); err != nil {
			_ = tx.Rollback(ctx)
			return 0, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, nil, err
		}
		baseline = append(baseline, float64(time.Since(began).Microseconds())/1000)
	}
	baselineDuration := time.Since(start)

	writer := outbox.NewWriter()
	withOutbox := make([]float64, 0, count)
	destination := "benchmark-enqueue-" + uuid.NewString()
	start = time.Now()
	for i := 0; i < count; i++ {
		began := time.Now()
		tx, err := pool.Begin(ctx)
		if err != nil {
			return 0, nil, err
		}
		id := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO public.emitlane_benchmark_work (id) VALUES ($1)`, id); err != nil {
			_ = tx.Rollback(ctx)
			return 0, nil, err
		}
		if _, err := writer.Enqueue(ctx, tx, outbox.Event{Destination: destination, Type: "benchmark.enqueue", Key: []byte(id.String()), Payload: payload}); err != nil {
			_ = tx.Rollback(ctx)
			return 0, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, nil, err
		}
		withOutbox = append(withOutbox, float64(time.Since(began).Microseconds())/1000)
	}
	elapsed := time.Since(start)
	return elapsed, map[string]any{
		"baseline_duration_seconds":  baselineDuration.Seconds(),
		"baseline_ops_per_second":    float64(count) / baselineDuration.Seconds(),
		"with_outbox_ops_per_second": float64(count) / elapsed.Seconds(),
		"baseline_p50_ms":            percentile(baseline, 0.50),
		"baseline_p95_ms":            percentile(baseline, 0.95),
		"baseline_p99_ms":            percentile(baseline, 0.99),
		"with_outbox_p50_ms":         percentile(withOutbox, 0.50),
		"enqueue_overhead_p50_ms":    percentile(withOutbox, 0.50) - percentile(baseline, 0.50),
		"enqueue_overhead_p95_ms":    percentile(withOutbox, 0.95) - percentile(baseline, 0.95),
		"enqueue_overhead_p99_ms":    percentile(withOutbox, 0.99) - percentile(baseline, 0.99),
		"latencies_ms":               withOutbox,
	}, nil
}

func benchmarkDelivery(ctx context.Context, pool *pgxpool.Pool, store *postgres.Store, brokers []string, count int, payload []byte, relayCount int, backlog bool) (time.Duration, map[string]any, error) {
	publisher, err := newPublisher(brokers)
	if err != nil {
		return 0, nil, err
	}
	defer publisher.Close()
	destination := "benchmark-delivery-" + uuid.NewString()
	if backlog {
		if _, err := enqueue(ctx, pool, destination, count, payload); err != nil {
			return 0, nil, err
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make([]<-chan error, 0, relayCount)
	for i := 0; i < relayCount; i++ {
		rly, err := newRelay(store, publisher, fmt.Sprintf("benchmark-%d", i), relay.FailureHooks{})
		if err != nil {
			cancel()
			return 0, nil, err
		}
		done = append(done, startRelay(runCtx, rly))
	}
	defer stopRelays(cancel, done)
	start := time.Now()
	if !backlog {
		if _, err := enqueue(ctx, pool, destination, count, payload); err != nil {
			return 0, nil, err
		}
	}
	if err := waitDelivered(ctx, pool, destination, count); err != nil {
		return 0, nil, err
	}
	elapsed := time.Since(start)
	latencies, err := deliveryLatencies(ctx, pool, destination)
	if err != nil {
		return 0, nil, err
	}
	return elapsed, map[string]any{
		"latencies_ms": latencies, "committed_events": count,
		"delivered_unique_events": count, "lost_events": 0,
	}, nil
}

func benchmarkIdle(ctx context.Context, pool *pgxpool.Pool, store *postgres.Store, duration time.Duration) (time.Duration, map[string]any, error) {
	var due int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events
WHERE available_at <= NOW() AND (status='pending' OR (status='inflight' AND lease_until <= NOW()))`).Scan(&due); err != nil {
		return 0, nil, err
	}
	if due != 0 {
		return 0, nil, fmt.Errorf("idle benchmark requires zero claimable events; found %d", due)
	}
	before, err := databaseTransactions(ctx, pool)
	if err != nil {
		return 0, nil, err
	}
	rly, err := newRelay(store, noopPublisher{}, "benchmark-idle", relay.FailureHooks{})
	if err != nil {
		return 0, nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := startRelay(runCtx, rly)
	start := time.Now()
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
		timer.Stop()
		cancel()
		return 0, nil, ctx.Err()
	case <-timer.C:
	}
	stopRelays(cancel, []<-chan error{done})
	elapsed := time.Since(start)
	after, err := databaseTransactions(ctx, pool)
	if err != nil {
		return 0, nil, err
	}
	transactions := max(int64(0), after-before-1)
	return elapsed, map[string]any{
		"database_transactions":                      transactions,
		"estimated_database_transactions_per_second": float64(transactions) / elapsed.Seconds(),
	}, nil
}

func benchmarkFailureRecovery(ctx context.Context, pool *pgxpool.Pool, store *postgres.Store, brokers []string, count int, payload []byte, restartDelay time.Duration) (time.Duration, map[string]any, error) {
	destination := "benchmark-recovery-" + uuid.NewString()
	committedIDs, err := enqueue(ctx, pool, destination, count, payload)
	if err != nil {
		return 0, nil, err
	}

	const crashCycles = 2
	failureStarted := time.Now()
	for cycle := 0; cycle < crashCycles; cycle++ {
		instancePrefix := fmt.Sprintf("benchmark-crash-%d", cycle+1)
		crashHooks := relay.FailureHooks{AfterClaimCommit: func(context.Context, relay.Event) error {
			return errors.New("benchmark injected crash after claim commit")
		}}
		crashingRelay, relayErr := newRelay(store, noopPublisher{}, instancePrefix, crashHooks)
		if relayErr != nil {
			return 0, nil, relayErr
		}
		crashCtx, crashCancel := context.WithCancel(ctx)
		crashDone := startRelay(crashCtx, crashingRelay)
		if err := waitOwnedLeases(ctx, pool, destination, instancePrefix+"-", count); err != nil {
			stopRelays(crashCancel, []<-chan error{crashDone})
			return 0, nil, err
		}
		stopRelays(crashCancel, []<-chan error{crashDone})
		if err := waitDuration(ctx, restartDelay); err != nil {
			return 0, nil, err
		}
		if err := waitExpiredLeases(ctx, pool, destination, count); err != nil {
			return 0, nil, err
		}
	}
	failurePhase := time.Since(failureStarted)

	publisher, err := newPublisher(brokers)
	if err != nil {
		return 0, nil, err
	}
	defer publisher.Close()
	recoveryRelay, err := newRelay(store, publisher, "benchmark-recovery", relay.FailureHooks{})
	if err != nil {
		return 0, nil, err
	}
	recoveryCtx, recoveryCancel := context.WithCancel(ctx)
	recoveryDone := startRelay(recoveryCtx, recoveryRelay)
	defer stopRelays(recoveryCancel, []<-chan error{recoveryDone})
	start := time.Now()
	if err := waitDelivered(ctx, pool, destination, count); err != nil {
		return 0, nil, err
	}
	observed, err := observeRecords(ctx, brokers, destination, committedIDs, count)
	if err != nil {
		return 0, nil, err
	}
	recoveryElapsed := time.Since(start)
	return recoveryElapsed, map[string]any{
		"crash_cycles": crashCycles, "restart_delay_seconds": restartDelay.Seconds(),
		"failure_phase_seconds": failurePhase.Seconds(), "recovery_time_ms": float64(recoveryElapsed.Microseconds()) / 1000,
		"committed_events": count, "delivered_unique_events": observed.unique,
		"total_broker_records": observed.total, "duplicate_records": observed.total - observed.unique,
		"lost_events": count - observed.unique,
	}, nil
}

func benchmarkAckCrash(ctx context.Context, pool *pgxpool.Pool, store *postgres.Store, brokers []string, count int, payload []byte) (time.Duration, map[string]any, error) {
	publisher, err := newPublisher(brokers)
	if err != nil {
		return 0, nil, err
	}
	defer publisher.Close()
	destination := "benchmark-ack-crash-" + uuid.NewString()
	committedIDs, err := enqueue(ctx, pool, destination, count, payload)
	if err != nil {
		return 0, nil, err
	}
	var injected atomic.Int64
	hooks := relay.FailureHooks{AfterPublishAck: func(context.Context, relay.Event) error {
		if injected.Add(1) <= int64(count) {
			return errors.New("benchmark injected ACK crash window")
		}
		return nil
	}}
	rly, err := newRelay(store, publisher, "benchmark-ack-crash", hooks)
	if err != nil {
		return 0, nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := startRelay(runCtx, rly)
	defer stopRelays(cancel, []<-chan error{done})
	start := time.Now()
	if err := waitDelivered(ctx, pool, destination, count); err != nil {
		return 0, nil, err
	}
	observed, err := observeRecords(ctx, brokers, destination, committedIDs, count*2)
	if err != nil {
		return 0, nil, err
	}
	return time.Since(start), map[string]any{
		"ack_crash_windows_injected": min(int(injected.Load()), count), "duplicates_expected": true,
		"committed_events": count, "delivered_unique_events": observed.unique,
		"total_broker_records": observed.total, "duplicate_records": observed.total - observed.unique,
		"lost_events": count - observed.unique,
	}, nil
}

func newPublisher(brokers []string) (*kafka.Publisher, error) {
	return kafka.NewPublisher(kafka.Config{Brokers: brokers, ClientID: "emitlane-benchmark", PublishTimeout: 5 * time.Second, AutoCreateTopics: true})
}

func newRelay(store relay.Store, publisher broker.Publisher, instance string, hooks relay.FailureHooks) (*relay.Relay, error) {
	cfg := relay.DefaultConfig()
	cfg.InstanceID = instance + "-" + uuid.NewString()
	cfg.BatchSize = 200
	cfg.Concurrency = 16
	cfg.PollInterval = 100 * time.Millisecond
	cfg.ControlInterval = 100 * time.Millisecond
	cfg.LeaseDuration = 3 * time.Second
	cfg.PublishTimeout = 2 * time.Second
	cfg.BaseDelay = 25 * time.Millisecond
	cfg.MaxDelay = 250 * time.Millisecond
	cfg.MaxAttempts = 1000
	cfg.Retention = 0
	cfg.CleanupInterval = 0
	cfg.CleanupBatch = 0
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return relay.New(cfg, store, publisher, relay.WithLogger(logger), relay.WithFailureHooks(hooks))
}

func newOrderedRelay(store relay.Store, publisher broker.Publisher, instance string) (*relay.Relay, error) {
	cfg := relay.DefaultConfig()
	cfg.InstanceID = instance + "-" + uuid.NewString()
	cfg.BatchSize = 200
	cfg.Concurrency = 16
	cfg.PollInterval = 100 * time.Millisecond
	cfg.ControlInterval = 100 * time.Millisecond
	cfg.HeartbeatInterval = 100 * time.Millisecond
	cfg.PresenceStaleAfter = time.Second
	cfg.LeaseDuration = 3 * time.Second
	cfg.PublishTimeout = 500 * time.Millisecond
	cfg.BaseDelay = 25 * time.Millisecond
	cfg.MaxDelay = 250 * time.Millisecond
	cfg.MaxAttempts = 1000
	cfg.Retention = 0
	cfg.CleanupInterval = 0
	cfg.CleanupBatch = 0
	cfg.OrderingRebalanceInterval = 100 * time.Millisecond
	cfg.OrderingLeaseDuration = 2 * time.Second
	cfg.OrderingSafetyMargin = 100 * time.Millisecond
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return relay.New(cfg, store, publisher, relay.WithLogger(logger))
}

func startRelay(ctx context.Context, rly *relay.Relay) <-chan error {
	done := make(chan error, 1)
	go func() { done <- rly.Run(ctx) }()
	return done
}

func stopRelays(cancel context.CancelFunc, done []<-chan error) {
	cancel()
	for _, ch := range done {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
		}
	}
}

func enqueue(ctx context.Context, pool *pgxpool.Pool, destination string, count int, payload []byte) ([]string, error) {
	writer := outbox.NewWriter()
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		id := uuid.NewString()
		eventID, err := writer.Enqueue(ctx, tx, outbox.Event{Destination: destination, Type: "benchmark.event", Key: []byte(id), Payload: payload})
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		ids = append(ids, eventID)
	}
	return ids, nil
}

func enqueueOrdered(ctx context.Context, pool *pgxpool.Pool, destination string, count, streamCount int, payload []byte) ([]string, error) {
	writer := outbox.NewWriter()
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		stream := i % streamCount
		sequence := int64(i/streamCount + 1)
		key := fmt.Sprintf("benchmark-stream-%d", stream)
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		eventID, err := writer.Enqueue(ctx, tx, outbox.Event{
			Destination: destination,
			Type:        "benchmark.ordered",
			Payload:     payload,
			OrderingKey: key,
			Sequence:    sequence,
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		ids = append(ids, eventID)
	}
	return ids, nil
}

func orderingPartitionDistribution(ctx context.Context, pool *pgxpool.Pool, destination string) (map[string]any, error) {
	rows, err := pool.Query(ctx, `
SELECT ordering_partition, COUNT(*)
FROM emitlane.outbox_events
WHERE destination=$1 AND ordering_partition IS NOT NULL
GROUP BY ordering_partition
ORDER BY ordering_partition`, destination)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64)
	var minimum, maximum int64
	for rows.Next() {
		var partition int16
		var count int64
		if err := rows.Scan(&partition, &count); err != nil {
			return nil, err
		}
		counts[fmt.Sprint(partition)] = count
		if minimum == 0 || count < minimum {
			minimum = count
		}
		if count > maximum {
			maximum = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"used_virtual_partitions": len(counts),
		"minimum_events":          minimum,
		"maximum_events":          maximum,
		"events_by_partition":     counts,
	}, nil
}

func waitDelivered(ctx context.Context, pool *pgxpool.Pool, destination string, want int) error {
	return waitEventState(ctx, pool, destination, "delivered", want)
}

func waitEventState(ctx context.Context, pool *pgxpool.Pool, destination, status string, want int) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE destination=$1 AND status=$2`, destination, status).Scan(&count); err != nil {
			return err
		}
		if count == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait %s %d/%d: %w", status, count, want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitExpiredLeases(ctx context.Context, pool *pgxpool.Pool, destination string, want int) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events
WHERE destination=$1 AND status='inflight' AND lease_until <= NOW()`, destination).Scan(&count); err != nil {
			return err
		}
		if count == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait expired leases %d/%d: %w", count, want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitOwnedLeases(ctx context.Context, pool *pgxpool.Pool, destination, ownerPrefix string, want int) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events
WHERE destination=$1 AND status='inflight' AND lease_owner LIKE $2`, destination, ownerPrefix+"%").Scan(&count); err != nil {
			return err
		}
		if count == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait leases owned by %s: %d/%d: %w", ownerPrefix, count, want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitDuration(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func deliveryLatencies(ctx context.Context, pool *pgxpool.Pool, destination string) ([]float64, error) {
	rows, err := pool.Query(ctx, `
SELECT EXTRACT(EPOCH FROM (delivered_at - created_at)) * 1000
FROM emitlane.outbox_events
WHERE destination = $1 AND status = 'delivered'
ORDER BY created_at, id`, destination)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	latencies := make([]float64, 0)
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		latencies = append(latencies, value)
	}
	return latencies, rows.Err()
}

func databaseTransactions(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var transactions int64
	err := pool.QueryRow(ctx, `
SELECT xact_commit + xact_rollback
FROM pg_stat_database
WHERE datname = current_database()`).Scan(&transactions)
	return transactions, err
}

type observation struct {
	total  int
	unique int
}

func observeRecords(ctx context.Context, brokers []string, topic string, expectedIDs []string, minimumRecords int) (observation, error) {
	expected := make(map[string]struct{}, len(expectedIDs))
	for _, id := range expectedIDs {
		expected[id] = struct{}{}
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("emitlane-benchmark-"+uuid.NewString()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		return observation{}, err
	}
	defer client.Close()
	observeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	seen := make(map[string]struct{}, len(expected))
	total := 0
	for {
		fetches := client.PollFetches(observeCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return observation{}, fmt.Errorf("consume benchmark records: %v", errs[0])
		}
		for _, record := range fetches.Records() {
			id := recordHeader(record, broker.HeaderEventID)
			if _, ok := expected[id]; !ok {
				continue
			}
			total++
			seen[id] = struct{}{}
		}
		if len(seen) == len(expected) && total >= minimumRecords {
			return observation{total: total, unique: len(seen)}, nil
		}
		if err := observeCtx.Err(); err != nil {
			return observation{}, fmt.Errorf("observe records: unique=%d/%d total=%d/%d: %w",
				len(seen), len(expected), total, minimumRecords, err)
		}
	}
}

func recordHeader(record *kgo.Record, name string) string {
	for _, header := range record.Headers {
		if header.Key == name {
			return string(header.Value)
		}
	}
	return ""
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, broker.Message) error { return nil }
func (noopPublisher) Close() error                                  { return nil }

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	index := int(float64(len(ordered)-1) * quantile)
	return ordered[index]
}

func outputResult(value result, path string) error {
	var writer io.Writer = os.Stdout
	var file *os.File
	if path != "" {
		var err error
		file, err = os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		writer = file
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func splitCSV(raw string) []string {
	var values []string
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func redactDatabase(raw string) string {
	if before, _, ok := strings.Cut(raw, "@"); ok {
		if scheme, _, found := strings.Cut(before, "://"); found {
			return scheme + "://***@" + strings.SplitN(raw, "@", 2)[1]
		}
	}
	return "configured"
}

func makePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

func buildIdentity() (string, string) {
	version, commit := "dev", "none"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version, commit
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			commit = setting.Value
		}
	}
	return version, commit
}

func postgresMetadata(ctx context.Context, pool *pgxpool.Pool) (map[string]any, error) {
	metadata := make(map[string]any, 5)
	for key, query := range map[string]string{
		"version":            `SHOW server_version`,
		"synchronous_commit": `SHOW synchronous_commit`,
		"fsync":              `SHOW fsync`,
		"full_page_writes":   `SHOW full_page_writes`,
	} {
		var value string
		if err := pool.QueryRow(ctx, query).Scan(&value); err != nil {
			return nil, fmt.Errorf("read PostgreSQL %s: %w", key, err)
		}
		metadata[key] = value
	}
	return metadata, nil
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
