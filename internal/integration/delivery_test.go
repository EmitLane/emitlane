//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"os"
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
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
	kafkapub "github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/inbox"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

type env struct {
	databaseURL string
	pool        *pgxpool.Pool
	brokers     []string
	store       *pgstore.Store
	writer      *outbox.Writer
	log         *slog.Logger
}

var (
	shared     *env
	sharedErr  error
	sharedOnce sync.Once
	cleanupEnv func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	if cleanupEnv != nil {
		cleanupEnv()
	}
	os.Exit(code)
}

func reserveLocalPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func startEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		pgC, err := postgres.Run(ctx,
			"postgres:16-alpine",
			postgres.WithDatabase("emitlane"),
			postgres.WithUsername("emitlane"),
			postgres.WithPassword("emitlane"),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			sharedErr = fmt.Errorf("postgres container: %w", err)
			return
		}
		kafkaPort, err := reserveLocalPort()
		if err != nil {
			_ = testcontainers.TerminateContainer(pgC)
			sharedErr = fmt.Errorf("reserve kafka port: %w", err)
			return
		}
		kafkaPortString := strconv.Itoa(kafkaPort)
		kafkaC, err := testcontainers.Run(ctx,
			"apache/kafka-native:4.3.1",
			testcontainers.WithExposedPorts("9092/tcp"),
			testcontainers.WithEnv(map[string]string{
				"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://127.0.0.1:" + kafkaPortString,
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			}),
			testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
				if hostConfig.PortBindings == nil {
					hostConfig.PortBindings = containernetwork.PortMap{}
				}
				hostConfig.PortBindings[containernetwork.MustParsePort("9092/tcp")] = []containernetwork.PortBinding{{
					HostIP:   netip.MustParseAddr("127.0.0.1"),
					HostPort: kafkaPortString,
				}}
			}),
			testcontainers.WithWaitStrategy(
				wait.ForListeningPort("9092/tcp").WithStartupTimeout(2*time.Minute),
			),
		)
		if err != nil {
			_ = testcontainers.TerminateContainer(pgC)
			sharedErr = fmt.Errorf("kafka container: %w", err)
			return
		}
		conn, err := pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			_ = testcontainers.TerminateContainer(kafkaC)
			_ = testcontainers.TerminateContainer(pgC)
			sharedErr = err
			return
		}
		brokers := []string{net.JoinHostPort("127.0.0.1", kafkaPortString)}
		pool, err := pgxpool.New(ctx, conn)
		if err != nil {
			_ = testcontainers.TerminateContainer(kafkaC)
			_ = testcontainers.TerminateContainer(pgC)
			sharedErr = err
			return
		}
		if err := pgstore.MigrateUp(ctx, pool); err != nil {
			pool.Close()
			_ = testcontainers.TerminateContainer(kafkaC)
			_ = testcontainers.TerminateContainer(pgC)
			sharedErr = err
			return
		}
		if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.business_orders (
            id TEXT PRIMARY KEY,
            amount INTEGER NOT NULL
        )`); err != nil {
			sharedErr = err
			return
		}
		if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.business_payments (
            order_id TEXT PRIMARY KEY,
            amount INTEGER NOT NULL
        )`); err != nil {
			sharedErr = err
			return
		}
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		store, err := pgstore.NewStore(pool)
		if err != nil {
			pool.Close()
			_ = testcontainers.TerminateContainer(kafkaC)
			_ = testcontainers.TerminateContainer(pgC)
			sharedErr = err
			return
		}
		shared = &env{
			databaseURL: conn,
			pool:        pool,
			brokers:     brokers,
			store:       store,
			writer:      outbox.NewWriter(),
			log:         log,
		}
		cleanupEnv = func() {
			pool.Close()
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			_ = testcontainers.TerminateContainer(kafkaC, testcontainers.StopContext(cleanupCtx))
			_ = testcontainers.TerminateContainer(pgC, testcontainers.StopContext(cleanupCtx))
		}
	})
	if sharedErr != nil {
		t.Fatalf("integration environment: %v", sharedErr)
	}
	reset := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := shared.pool.Exec(ctx, `
TRUNCATE public.business_orders, public.business_payments, emitlane.outbox_events,
         emitlane.inbox_events, emitlane.admin_audit_log, emitlane.relay_instances,
         emitlane.ordering_streams;
UPDATE emitlane.runtime_control
SET paused = FALSE, reason = NULL, updated_at = NOW(), updated_by = 'integration-reset'
WHERE singleton = TRUE;
UPDATE emitlane.ordering_partitions
SET lease_owner = NULL, lease_until = NULL, epoch = 0,
    handoff_not_before = NULL, publish_timeout_ms = NULL, updated_at = NOW()`)
		return err
	}
	if err := reset(); err != nil {
		t.Fatalf("reset integration database: %v", err)
	}
	t.Cleanup(func() {
		if err := reset(); err != nil {
			t.Errorf("clean integration database: %v", err)
		}
	})
	return shared
}

func (e *env) publisher(t *testing.T) *kafkapub.Publisher {
	t.Helper()
	pub, err := kafkapub.NewPublisher(kafkapub.Config{
		Brokers:          e.brokers,
		ClientID:         "emitlane-test",
		PublishTimeout:   8 * time.Second,
		AutoCreateTopics: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

func (e *env) newRelay(t *testing.T, cfg relay.Config, pub broker.Publisher, hooks relay.FailureHooks, opts ...relay.Option) *relay.Relay {
	t.Helper()
	if cfg.InstanceID == "" {
		cfg.InstanceID = relay.NewInstanceID()
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 10
	}
	if cfg.BaseDelay == 0 {
		cfg.BaseDelay = 20 * time.Millisecond
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = time.Second
	}
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = 8 * time.Second
	}
	if cfg.PublishTimeout >= cfg.LeaseDuration {
		cfg.PublishTimeout = cfg.LeaseDuration / 2
		if cfg.PublishTimeout < 50*time.Millisecond {
			cfg.PublishTimeout = 50 * time.Millisecond
		}
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.StatsInterval == 0 {
		cfg.StatsInterval = time.Hour
	}
	if cfg.ControlInterval == 0 {
		cfg.ControlInterval = 50 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 100 * time.Millisecond
	}
	if cfg.PresenceStaleAfter == 0 {
		cfg.PresenceStaleAfter = 500 * time.Millisecond
	}
	if cfg.OrderingRebalanceInterval == 0 {
		cfg.OrderingRebalanceInterval = 50 * time.Millisecond
	}
	if cfg.OrderingLeaseDuration == 0 {
		cfg.OrderingLeaseDuration = 15 * time.Second
	}
	if cfg.OrderingSafetyMargin == 0 {
		cfg.OrderingSafetyMargin = 100 * time.Millisecond
	}
	opts = append(opts,
		relay.WithLogger(e.log),
		relay.WithFailureHooks(hooks),
	)
	rly, err := relay.New(cfg, e.store, pub, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return rly
}

func (e *env) eventStatus(t *testing.T, id string) string {
	t.Helper()
	return e.getEvent(t, id).Status
}

func (e *env) getEvent(t *testing.T, id string) relay.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uid, err := uuid.Parse(id)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := e.store.Get(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func (e *env) waitStatus(t *testing.T, id, want string, timeout time.Duration) relay.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last relay.Event
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		uid, err := uuid.Parse(id)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		ev, err := e.store.Get(ctx, uid)
		cancel()
		if err == nil {
			last = ev
			if ev.Status == want {
				return ev
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("event %s status=%s attempts=%d last_error=%q, want %s", id, last.Status, last.Attempts, last.LastError, want)
	return last
}

func consumeRecords(t *testing.T, brokers []string, topic string, count int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	deadline := time.Now().Add(timeout)
	records := make([]*kgo.Record, 0, count)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		records = append(records, fetches.Records()...)
		if len(records) >= count {
			return records[:count]
		}
	}
	t.Fatalf("received %d/%d kafka records on topic %s", len(records), count, topic)
	return nil
}

type failPublisher struct {
	err error
}

func (f *failPublisher) Publish(context.Context, broker.Message) error {
	if f.err != nil {
		return f.err
	}
	return errors.New("forced publish failure")
}

func (f *failPublisher) Close() error { return nil }

func runRelay(t *testing.T, rly *relay.Relay) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rly.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("relay did not stop")
		}
	})
	return cancel
}

func enqueueOrder(t *testing.T, e *env, orderID, topic string, amount int, commit bool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO public.business_orders (id, amount) VALUES ($1, $2)`, orderID, amount); err != nil {
		t.Fatal(err)
	}
	payload, err := outbox.JSON(map[string]any{"order_id": orderID, "amount": amount})
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.writer.Enqueue(ctx, tx, outbox.Event{
		Destination: topic,
		Type:        "order.created",
		Key:         []byte(orderID),
		Payload:     payload,
		Headers:     map[string]string{"source": "test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if commit {
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func (e *env) ensureTopic(t *testing.T, topic string) {
	e.ensureTopicPartitions(t, topic, 1)
}

func (e *env) ensureTopicPartitions(t *testing.T, topic string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)
	res, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Error(); err != nil {
		t.Fatal(err)
	}
}

func topicName(t *testing.T, e *env) string {
	t.Helper()
	name := "orders-" + uuid.NewString()
	e.ensureTopic(t, name)
	return name
}

func TestAtomicRollback(t *testing.T) {
	e := startEnv(t)
	id := enqueueOrder(t, e, "ord-rollback", topicName(t, e), 10, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_orders WHERE id = 'ord-rollback'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("business row present after rollback: %d", n)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("outbox row present after rollback: %d", n)
	}
}

func TestAtomicCommit(t *testing.T) {
	e := startEnv(t)
	id := enqueueOrder(t, e, "ord-commit", topicName(t, e), 11, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_orders WHERE id = 'ord-commit'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("business row missing: %d", n)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("outbox row missing: %d", n)
	}
}

func TestRelayPublishes(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-pub", topic, 12, true)
	rly := e.newRelay(t, relay.Config{}, e.publisher(t), relay.FailureHooks{})
	runRelay(t, rly)
	ev := e.waitStatus(t, id, "delivered", 20*time.Second)
	rec := consumeRecords(t, e.brokers, topic, 1, 15*time.Second)[0]
	if string(rec.Key) != "ord-pub" {
		t.Fatalf("key %s", rec.Key)
	}
	if !strings.Contains(string(rec.Value), "ord-pub") {
		t.Fatalf("payload %s", rec.Value)
	}
	header := map[string]string{}
	for _, h := range rec.Headers {
		header[h.Key] = string(h.Value)
	}
	if header[broker.HeaderEventID] != id {
		t.Fatalf("event id header %s", header[broker.HeaderEventID])
	}
	if header[broker.HeaderEventType] != "order.created" {
		t.Fatalf("type header %s", header[broker.HeaderEventType])
	}
	if header["source"] != "test" {
		t.Fatalf("user header %s", header["source"])
	}
	if ev.Attempts != 1 {
		t.Fatalf("attempts %d", ev.Attempts)
	}
}

func TestMultipleWorkers(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	const n = 20
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = enqueueOrder(t, e, fmt.Sprintf("ord-mw-%d", i), topic, i, true)
	}
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "relay-a", Concurrency: 2}, e.publisher(t), relay.FailureHooks{}))
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "relay-b", Concurrency: 2}, e.publisher(t), relay.FailureHooks{}))
	for _, id := range ids {
		e.waitStatus(t, id, "delivered", 30*time.Second)
	}
	records := consumeRecords(t, e.brokers, topic, n, 20*time.Second)
	seen := make(map[string]struct{}, n)
	for _, rec := range records {
		seen[headerValue(rec, broker.HeaderEventID)] = struct{}{}
	}
	if len(seen) != n {
		t.Fatalf("received %d unique event ids from %d records", len(seen), n)
	}
}

func TestConcurrentClaimersDoNotClaimSameRow(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	const n = 20
	for i := range n {
		enqueueOrder(t, e, fmt.Sprintf("ord-claim-%d", i), topic, i+1, true)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan []relay.Event, 2)
	errs := make(chan error, 2)
	for _, owner := range []string{"claimer-a", "claimer-b"} {
		owner := owner
		go func() {
			<-start
			events, err := e.store.Claim(ctx, owner, n, 10*time.Second)
			results <- events
			errs <- err
		}()
	}
	close(start)
	seen := make(map[uuid.UUID]string, n)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		for _, ev := range <-results {
			if prior, exists := seen[ev.ID]; exists {
				t.Fatalf("event %s claimed by both %s and %s", ev.ID, prior, ev.LeaseOwner)
			}
			seen[ev.ID] = ev.LeaseOwner
			if ev.Attempts != 0 {
				t.Fatalf("claim started an attempt for %s: %d", ev.ID, ev.Attempts)
			}
		}
	}
	if len(seen) != n {
		t.Fatalf("claimed %d unique rows, want %d", len(seen), n)
	}
}

func TestLeaseRecovery(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-lease", topic, 1, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	claimed, err := e.store.Claim(ctx, "dead-worker", 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d", len(claimed))
	}
	if st := e.eventStatus(t, id); st != "inflight" {
		t.Fatalf("status %s", st)
	}
	time.Sleep(1500 * time.Millisecond)
	runRelay(t, e.newRelay(t, relay.Config{LeaseDuration: 5 * time.Second}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, id, "delivered", 20*time.Second)
}

func TestBrokerUnavailable(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-down", topic, 1, true)
	bad, err := kafkapub.NewPublisher(kafkapub.Config{
		Brokers:        []string{"127.0.0.1:1"},
		ClientID:       "emitlane-bad",
		PublishTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bad.Close() })
	cancelRelay := runRelay(t, e.newRelay(t, relay.Config{
		PublishTimeout: time.Second,
		LeaseDuration:  5 * time.Second,
		MaxAttempts:    10,
		BaseDelay:      50 * time.Millisecond,
		MaxDelay:       200 * time.Millisecond,
	}, bad, relay.FailureHooks{}))
	deadline := time.Now().Add(10 * time.Second)
	sawRetry := false
	for time.Now().Before(deadline) {
		ev := e.getEvent(t, id)
		if ev.Status == "delivered" {
			t.Fatal("must not deliver while broker is down")
		}
		if ev.Attempts >= 1 && ev.LastError != "" && (ev.Status == "pending" || ev.Status == "inflight") {
			sawRetry = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawRetry {
		t.Fatal("expected durable retry state while broker is unavailable")
	}
	cancelRelay()
	runRelay(t, e.newRelay(t, relay.Config{}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, id, "delivered", 20*time.Second)
	rec := consumeRecords(t, e.brokers, topic, 1, 15*time.Second)[0]
	if headerValue(rec, broker.HeaderEventID) != id {
		t.Fatalf("recovered delivery event id %q", headerValue(rec, broker.HeaderEventID))
	}
}

func TestDeadEvent(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-dead", topic, 1, true)
	runRelay(t, e.newRelay(t, relay.Config{
		MaxAttempts:    3,
		BaseDelay:      time.Millisecond,
		MaxDelay:       10 * time.Millisecond,
		PublishTimeout: time.Second,
		LeaseDuration:  5 * time.Second,
	}, &failPublisher{err: errors.New("poison")}, relay.FailureHooks{}))
	ev := e.waitStatus(t, id, "dead", 15*time.Second)
	if !strings.Contains(ev.LastError, "poison") {
		t.Fatalf("last_error %q", ev.LastError)
	}
	if ev.Attempts < 3 {
		t.Fatalf("attempts %d", ev.Attempts)
	}
}

func TestInboxDuplicate(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	eventID := uuid.Must(uuid.NewV7()).String()
	calls := 0
	handler := func(ctx context.Context, tx pgx.Tx) error {
		calls++
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`, "ord-inbox", 5)
		return err
	}
	tx1, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Process(ctx, tx1, "billing-service", eventID, handler); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx2, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Process(ctx, tx2, "billing-service", eventID, handler); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("handler calls %d", calls)
	}
	txStrict, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = inbox.ProcessStrict(ctx, txStrict, "billing-service", eventID, func(context.Context, pgx.Tx) error {
		t.Fatal("strict duplicate callback must not run")
		return nil
	})
	if !errors.Is(err, inbox.ErrAlreadyProcessed) {
		t.Fatalf("strict duplicate error %v", err)
	}
	if err := txStrict.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_payments WHERE order_id = 'ord-inbox'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("payments %d", n)
	}
	tx3, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Process(ctx, tx3, "email-service", eventID, func(context.Context, pgx.Tx) error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx3.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("second consumer should run, calls=%d", calls)
	}
}

func TestInboxConcurrentDuplicateRace(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	eventID := uuid.Must(uuid.NewV7()).String()
	start := make(chan struct{})
	results := make(chan error, 2)

	for range 2 {
		go func() {
			<-start
			tx, err := e.pool.Begin(ctx)
			if err != nil {
				results <- err
				return
			}
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			err = inbox.Process(ctx, tx, "billing-race", eventID, func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ('ord-inbox-race', 5)`)
				return err
			})
			if err == nil {
				err = tx.Commit(ctx)
			}
			results <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	var payments, markers int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_payments WHERE order_id = 'ord-inbox-race'`).Scan(&payments); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer = 'billing-race' AND event_id = $1`, eventID).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if payments != 1 || markers != 1 {
		t.Fatalf("concurrent duplicate produced payments=%d markers=%d, want 1/1", payments, markers)
	}
}

func TestInboxCallbackErrorRollsBackSavepoint(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	eventID := uuid.Must(uuid.NewV7()).String()

	tx1, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("business mutation failed")
	err = inbox.Process(ctx, tx1, "billing-service", eventID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ('ord-savepoint', 5)`); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("callback error %v", err)
	}
	// Deliberately commit to prove Process itself removed the marker and callback
	// writes instead of depending only on caller discipline.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx2, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := inbox.Process(ctx, tx2, "billing-service", eventID, func(ctx context.Context, tx pgx.Tx) error {
		called = true
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ('ord-savepoint', 5)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("callback was skipped after prior callback rollback")
	}
}

func TestMigrationRoundTrip(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := e.pool.Exec(ctx, `CREATE TABLE emitlane.application_sentinel (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := pgstore.MigrateUp(cleanupCtx, e.pool); err != nil {
			t.Errorf("restore schema after migration test: %v", err)
			return
		}
		if _, err := e.pool.Exec(cleanupCtx, `DROP TABLE IF EXISTS emitlane.application_sentinel`); err != nil {
			t.Errorf("drop migration test sentinel: %v", err)
		}
	})
	if err := pgstore.MigrateDown(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.MigrateDown(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.MigrateDown(ctx, e.pool); err != nil {
		t.Fatal(err)
	}
	version, err := pgstore.SchemaVersion(ctx, e.pool)
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Fatalf("schema version after down = %d, want 0", version)
	}
	var applicationTablePreserved bool
	if err := e.pool.QueryRow(ctx, `SELECT to_regclass('emitlane.application_sentinel') IS NOT NULL`).Scan(&applicationTablePreserved); err != nil {
		t.Fatal(err)
	}
	if !applicationTablePreserved {
		t.Fatal("down migration removed an application-owned table")
	}
	const migrators = 8
	errCh := make(chan error, migrators)
	var wg sync.WaitGroup
	for range migrators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- pgstore.MigrateUp(ctx, e.pool)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent migrate up: %v", err)
		}
	}
	version, err = pgstore.SchemaVersion(ctx, e.pool)
	if err != nil {
		t.Fatal(err)
	}
	if version != pgstore.CurrentSchemaVersion() {
		t.Fatalf("schema version after up = %d, want %d", version, pgstore.CurrentSchemaVersion())
	}
	for _, index := range pgstore.RequiredIndexes() {
		exists, err := pgstore.IndexExists(ctx, e.pool, index)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("required index %s is missing after migration round trip", index)
		}
	}
	if _, err := e.pool.Exec(ctx, `DROP TABLE emitlane.application_sentinel`); err != nil {
		t.Fatal(err)
	}
}

func TestWriterNotifyCommitAndRollback(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, e.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `LISTEN emitlane_events`); err != nil {
		t.Fatal(err)
	}

	topic := topicName(t, e)
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.writer.Enqueue(ctx, tx, outbox.Event{Destination: topic, Type: "order.created"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	notifyCtx, notifyCancel := context.WithTimeout(ctx, 2*time.Second)
	n, err := conn.WaitForNotification(notifyCtx)
	notifyCancel()
	if err != nil {
		t.Fatal(err)
	}
	if n.Payload != id {
		t.Fatalf("notification payload %q, want %q", n.Payload, id)
	}

	tx, err = e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.Enqueue(ctx, tx, outbox.Event{Destination: topic, Type: "order.created"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	rollbackCtx, rollbackCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	_, err = conn.WaitForNotification(rollbackCtx)
	rollbackCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rollback produced notification: %v", err)
	}
}

func TestListenerWakeAndPollingFallback(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	parsedURL, err := url.Parse(e.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsedURL.Query()
	query.Set("application_name", "emitlane-listener-reconnect-test")
	parsedURL.RawQuery = query.Encode()
	wake := make(chan struct{}, 1)
	go pgstore.NewListener(parsedURL.String(), e.log).Run(ctx, wake)
	for {
		if _, err := e.pool.Exec(ctx, `SELECT pg_notify('emitlane_events', 'wake-test')`); err != nil {
			t.Fatal(err)
		}
		select {
		case <-wake:
			goto listenerReady
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("listener did not wake")
		}
	}

listenerReady:
	var terminated bool
	if err := e.pool.QueryRow(ctx, `
SELECT COALESCE(bool_or(pg_terminate_backend(pid)), FALSE)
FROM pg_stat_activity
WHERE application_name = 'emitlane-listener-reconnect-test'
  AND pid <> pg_backend_pid()
`).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatal("listener backend was not found")
	}
	for {
		select {
		case <-wake:
		default:
			goto wakeDrained
		}
	}

wakeDrained:
	reconnectDeadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(reconnectDeadline) {
		if _, err := e.pool.Exec(ctx, `SELECT pg_notify('emitlane_events', 'reconnect-test')`); err != nil {
			t.Fatal(err)
		}
		select {
		case <-wake:
			goto listenerReconnected
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("listener did not reconnect")

listenerReconnected:
	topic := topicName(t, e)
	writer := outbox.NewWriter(outbox.WithoutNotify())
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := writer.Enqueue(ctx, tx, outbox.Event{Destination: topic, Type: "order.created"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	runRelay(t, e.newRelay(t, relay.Config{PollInterval: 50 * time.Millisecond}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, id, "delivered", 5*time.Second)
}

func TestCrashAfterClaimBeforePublish(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-crash-claim", topic, 1, true)
	cancel := runRelay(t, e.newRelay(t, relay.Config{
		InstanceID:    "crasher",
		LeaseDuration: 2 * time.Second,
	}, e.publisher(t), relay.FailureHooks{
		AfterClaimCommit: func(context.Context, relay.Event) error {
			return errors.New("simulated crash after claim")
		},
	}))
	time.Sleep(400 * time.Millisecond)
	if st := e.eventStatus(t, id); st != "inflight" {
		t.Fatalf("status %s", st)
	}
	cancel()
	time.Sleep(2200 * time.Millisecond)
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "survivor", LeaseDuration: 10 * time.Second}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, id, "delivered", 20*time.Second)
}

func TestCrashAfterKafkaAckBeforeDelivered(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrder(t, e, "ord-crash-ack", topic, 1, true)
	cancel := runRelay(t, e.newRelay(t, relay.Config{
		InstanceID:    "ack-crasher",
		LeaseDuration: 2 * time.Second,
	}, e.publisher(t), relay.FailureHooks{
		AfterPublishAck: func(context.Context, relay.Event) error {
			return errors.New("simulated crash after kafka ack")
		},
	}))
	deadline := time.Now().Add(10 * time.Second)
	sawInflight := false
	for time.Now().Before(deadline) {
		st := e.eventStatus(t, id)
		if st == "inflight" {
			sawInflight = true
			break
		}
		if st == "delivered" {
			t.Fatal("must not mark delivered when ack hook fails")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawInflight {
		t.Fatal("event never became inflight after ack-crash")
	}
	cancel()
	time.Sleep(2200 * time.Millisecond)
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "ack-survivor", LeaseDuration: 10 * time.Second}, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, id, "delivered", 20*time.Second)
	records := consumeRecords(t, e.brokers, topic, 2, 15*time.Second)
	for i, rec := range records {
		if headerValue(rec, broker.HeaderEventID) != id {
			t.Fatalf("delivery %d has event id %q, want %q", i+1, headerValue(rec, broker.HeaderEventID), id)
		}
	}
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
