//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkaadapter "github.com/emitlane/emitlane/broker/kafka"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/inbox"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestManagedConsumerDBCommitBeforeOffsetCrashIsDuplicateSafe(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-crash-"+uuid.NewString(), "managed-crash-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("crash-window"))

	baseFactory := newManagedFactory(t, e, topic, group)
	failure := make(chan struct{}, 1)
	failingFactory := &failFirstCommitFactory{inner: baseFactory, failed: failure}
	store := newManagedInboxStore(t, e)
	var calls atomic.Int32
	handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		calls.Add(1)
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
		return err
	}
	first := launchManaged(t, e, managedTestConfig("commit-crash-v1", "commit-crash-a"), store, failingFactory, handler)
	select {
	case <-failure:
	case <-time.After(15 * time.Second):
		t.Fatal("did not reach DB-commit / offset-commit crash window")
	}
	first.stop(t)

	event, err := store.Get(context.Background(), "commit-crash-v1", eventID)
	if err != nil || event.Status != inbox.StatusProcessed {
		t.Fatalf("durable Inbox state before restart=%+v err=%v", event, err)
	}
	if got := committedOffset(t, e, group, topic, 0); got > 0 {
		t.Fatalf("offset advanced before simulated crash: %d", got)
	}

	secondFactory := newManagedFactory(t, e, topic, group)
	second := launchManaged(t, e, managedTestConfig("commit-crash-v1", "commit-crash-b"), store, secondFactory, handler)
	defer second.stop(t)
	waitCommittedOffset(t, e, group, topic, 0, 1, 15*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("protected handler calls=%d, want 1", calls.Load())
	}
	var effects int
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM public.business_payments WHERE order_id=$1`, eventID.String()).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || event.Attempts != 1 {
		t.Fatalf("protected effects=%d attempts=%d", effects, event.Attempts)
	}
}

func TestManagedConsumerOffsetCommitFailureRecoversWithoutRerunningHandler(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-commit-fail-"+uuid.NewString(), "managed-commit-fail-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("commit-failure"))
	baseFactory := newManagedFactory(t, e, topic, group)
	failure := make(chan struct{}, 1)
	factory := &failFirstCommitFactory{inner: baseFactory, failed: failure}
	store := newManagedInboxStore(t, e)
	var calls atomic.Int32
	run := launchManaged(t, e, managedTestConfig("commit-recovery-v1", "commit-recovery"), store, factory,
		func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			calls.Add(1)
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	select {
	case <-failure:
	case <-time.After(15 * time.Second):
		t.Fatal("offset commit failure was not injected")
	}
	waitCommittedOffset(t, e, group, topic, 0, 1, 15*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("handler reran after offset commit failure: calls=%d", calls.Load())
	}
}

func TestManagedConsumerRecoversClaimLeftByCrash(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-claim-crash-"+uuid.NewString(), "managed-claim-crash-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("claimed"))
	store := newManagedInboxStore(t, e)
	claim, err := store.Claim(context.Background(), inbox.ClaimRequest{
		Consumer: "claim-crash-v1", EventID: eventID,
		Source:     inbox.Source{Topic: topic, Partition: 0, Offset: 0},
		LeaseOwner: "crashed-process", LeaseDuration: 150 * time.Millisecond,
	})
	if err != nil || claim.Disposition != inbox.Claimed {
		t.Fatalf("pre-crash claim=%+v err=%v", claim, err)
	}
	factory := newManagedFactory(t, e, topic, group)
	run := launchManaged(t, e, managedTestConfig("claim-crash-v1", "claim-recovery"), store, factory,
		func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 2)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	event := waitManagedInboxStatus(t, store, "claim-crash-v1", eventID, inbox.StatusProcessed, 15*time.Second)
	if event.Attempts != 2 {
		t.Fatalf("reclaimed attempts=%d, want 2", event.Attempts)
	}
}

func TestManagedConsumerSameEventAtLaterOffsetIsDuplicate(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-later-duplicate-"+uuid.NewString(), "managed-later-duplicate-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("first"))
	produceManagedRecord(t, e, topic, eventID, []byte("later-offset"))
	store := newManagedInboxStore(t, e)
	var calls atomic.Int32
	run := launchManaged(t, e, managedTestConfig("later-duplicate-v1", "later-duplicate"), store,
		newManagedFactory(t, e, topic, group), func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			calls.Add(1)
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	waitCommittedOffset(t, e, group, topic, 0, 2, 15*time.Second)
	event, err := store.Get(context.Background(), "later-duplicate-v1", eventID)
	if err != nil || event.Status != inbox.StatusProcessed || event.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("duplicate state=%+v calls=%d err=%v", event, calls.Load(), err)
	}
}

func TestManagedConsumerRetryPreservesPartitionOrder(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-order-"+uuid.NewString(), "managed-order-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	ids := map[string]uuid.UUID{"A": uuid.New(), "B": uuid.New(), "C": uuid.New()}
	for _, name := range []string{"A", "B", "C"} {
		produceManagedRecord(t, e, topic, ids[name], []byte(name))
	}
	store := newManagedInboxStore(t, e)
	config := managedTestConfig("partition-order-v1", "partition-order")
	config.BaseDelay, config.MaxDelay, config.Jitter = 100*time.Millisecond, 100*time.Millisecond, 0
	run := launchManaged(t, e, config, store, newManagedFactory(t, e, topic, group),
		func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			name := string(message.Payload)
			if name == "B" && message.Attempt == 1 {
				if _, err := tx.Exec(ctx, `INSERT INTO public.business_orders (id, amount) VALUES ($1, 99)`, "order-sequence-B-failed"); err != nil {
					return err
				}
				return errors.New("retry B")
			}
			_, err := tx.Exec(ctx, `
INSERT INTO public.business_orders (id, amount)
SELECT $1, COALESCE(MAX(amount), 0)+1 FROM public.business_orders
WHERE id LIKE 'order-sequence-%'`, "order-sequence-"+name)
			return err
		})
	defer run.stop(t)
	waitManagedInboxStatus(t, store, "partition-order-v1", ids["C"], inbox.StatusProcessed, 15*time.Second)
	rows, err := e.pool.Query(context.Background(), `SELECT id FROM public.business_orders WHERE id LIKE 'order-sequence-%' ORDER BY amount`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var order []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		order = append(order, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"order-sequence-A", "order-sequence-B", "order-sequence-C"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("committed order=%v want=%v", order, want)
	}
	b, err := store.Get(context.Background(), "partition-order-v1", ids["B"])
	if err != nil || b.Attempts != 2 {
		t.Fatalf("B lifecycle=%+v err=%v", b, err)
	}
}

func TestManagedConsumerInvalidIDBlocksOnlyItsPartition(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-invalid-"+uuid.NewString(), "managed-invalid-group-"+uuid.NewString()
	e.ensureTopicPartitions(t, topic, 2)
	blockedID, healthyID := uuid.New(), uuid.New()
	produceManagedRecordAtPartition(t, e, topic, 0, uuid.Nil, []byte("invalid"))
	produceManagedRecordAtPartition(t, e, topic, 0, blockedID, []byte("blocked"))
	produceManagedRecordAtPartition(t, e, topic, 1, healthyID, []byte("healthy"))
	store := newManagedInboxStore(t, e)
	config := managedTestConfig("partition-isolation-v1", "partition-isolation")
	run := launchManaged(t, e, config, store, newManagedFactory(t, e, topic, group),
		func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	waitManagedInboxStatus(t, store, "partition-isolation-v1", healthyID, inbox.StatusProcessed, 15*time.Second)
	time.Sleep(300 * time.Millisecond)
	if _, err := store.Get(context.Background(), "partition-isolation-v1", blockedID); !errors.Is(err, inbox.ErrNotFound) {
		t.Fatalf("record behind invalid ID was not blocked: %v", err)
	}
	if offset := committedOffset(t, e, group, topic, 0); offset > 0 {
		t.Fatalf("blocked partition committed offset=%d", offset)
	}
}

func TestManagedConsumerDeadBlocksSamePartitionWhileOtherContinues(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-dead-isolation-"+uuid.NewString(), "managed-dead-isolation-group-"+uuid.NewString()
	e.ensureTopicPartitions(t, topic, 2)
	deadID, blockedID, healthyID := uuid.New(), uuid.New(), uuid.New()
	produceManagedRecordAtPartition(t, e, topic, 0, deadID, []byte("dead"))
	produceManagedRecordAtPartition(t, e, topic, 0, blockedID, []byte("blocked"))
	produceManagedRecordAtPartition(t, e, topic, 1, healthyID, []byte("healthy"))
	store := newManagedInboxStore(t, e)
	run := launchManaged(t, e, managedTestConfig("dead-isolation-v1", "dead-isolation"), store,
		newManagedFactory(t, e, topic, group), func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			if string(message.Payload) == "dead" {
				return inbox.Permanent(errors.New("poison value"))
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	waitManagedInboxStatus(t, store, "dead-isolation-v1", deadID, inbox.StatusDead, 15*time.Second)
	waitManagedInboxStatus(t, store, "dead-isolation-v1", healthyID, inbox.StatusProcessed, 15*time.Second)
	time.Sleep(300 * time.Millisecond)
	if _, err := store.Get(context.Background(), "dead-isolation-v1", blockedID); !errors.Is(err, inbox.ErrNotFound) {
		t.Fatalf("record behind dead event was not blocked: %v", err)
	}
	if got := committedOffset(t, e, group, topic, 1); got != 1 {
		t.Fatalf("healthy partition offset=%d, want 1", got)
	}
}

func TestManagedConsumerKafkaStopRestartRecovers(t *testing.T) {
	e := startEnv(t)
	t.Cleanup(func() { e.restoreKafka(t) })
	topic, group := "managed-kafka-restart-"+uuid.NewString(), "managed-kafka-restart-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("broker-restart"))
	e.stopKafka(t)
	store := newManagedInboxStore(t, e)
	run := launchManaged(t, e, managedTestConfig("kafka-restart-v1", "kafka-restart"), store,
		newManagedFactory(t, e, topic, group), func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	assertManagedStillRunning(t, run, 3*time.Second)
	e.startKafka(t)
	waitManagedInboxStatus(t, store, "kafka-restart-v1", eventID, inbox.StatusProcessed, 30*time.Second)
}

func TestManagedConsumerPostgresOutageRecovers(t *testing.T) {
	e := startEnv(t)
	t.Cleanup(func() { e.restorePostgres(t) })
	topic, group := "managed-postgres-restart-"+uuid.NewString(), "managed-postgres-restart-group-"+uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("database-restart"))
	e.stopPostgres(t)
	store := newManagedInboxStore(t, e)
	run := launchManaged(t, e, managedTestConfig("postgres-restart-v1", "postgres-restart"), store,
		newManagedFactory(t, e, topic, group), func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	assertManagedStillRunning(t, run, 500*time.Millisecond)
	e.startPostgres(t)
	waitManagedInboxStatus(t, store, "postgres-restart-v1", eventID, inbox.StatusProcessed, 30*time.Second)
}

func TestManagedConsumerRebalanceDuringHandlerRecovers(t *testing.T) {
	e := startEnv(t)
	topic, group := "managed-rebalance-"+uuid.NewString(), "managed-rebalance-group-"+uuid.NewString()
	e.ensureTopicPartitions(t, topic, 2)
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	produceManagedRecordAtPartition(t, e, topic, 0, ids[0], []byte("zero"))
	produceManagedRecordAtPartition(t, e, topic, 1, ids[1], []byte("one"))
	store := newManagedInboxStore(t, e)
	configA := managedTestConfig("rebalance-v1", "rebalance-a")
	configA.BaseDelay, configA.MaxDelay, configA.Jitter = 50*time.Millisecond, 50*time.Millisecond, 0
	started := make(chan struct{})
	var blockFirst atomic.Bool
	handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		if message.Attempt == 1 && blockFirst.CompareAndSwap(false, true) {
			close(started)
			if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 99)`, message.EventID.String()); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`, message.EventID.String(), message.Attempt)
		return err
	}
	first := launchManaged(t, e, configA, store, newManagedFactory(t, e, topic, group), handler)
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("inflight handler did not start")
	}
	configB := managedTestConfig("rebalance-v1", "rebalance-b")
	configB.BaseDelay, configB.MaxDelay, configB.Jitter = 50*time.Millisecond, 50*time.Millisecond, 0
	second := launchManaged(t, e, configB, store, newManagedFactory(t, e, topic, group), handler)
	defer second.stop(t)
	for _, id := range ids {
		waitManagedInboxStatus(t, store, "rebalance-v1", id, inbox.StatusProcessed, 30*time.Second)
	}
	first.stop(t)
	afterGraceful := uuid.New()
	produceManagedRecordAtPartition(t, e, topic, 0, afterGraceful, []byte("after-graceful-loss"))
	waitManagedInboxStatus(t, store, "rebalance-v1", afterGraceful, inbox.StatusProcessed, 20*time.Second)
	var effects int
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM public.business_payments WHERE order_id=ANY($1)`,
		[]string{ids[0].String(), ids[1].String(), afterGraceful.String()}).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 3 {
		t.Fatalf("protected effects after rebalance and graceful loss=%d, want 3", effects)
	}
}

func TestManagedConsumerAbruptProcessLossRecovers(t *testing.T) {
	if os.Getenv("EMITLANE_CONSUMER_CRASH_HELPER") == "1" {
		t.Skip("parent-only test")
	}
	e := startEnv(t)
	topic, group := "managed-abrupt-"+uuid.NewString(), "managed-abrupt-group-"+uuid.NewString()
	consumerName := "abrupt-loss-v1"
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("abrupt-loss"))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if tcp, ok := listener.(*net.TCPListener); ok {
		_ = tcp.SetDeadline(time.Now().Add(20 * time.Second))
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestManagedConsumerAbruptProcessHelper$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(),
		"EMITLANE_CONSUMER_CRASH_HELPER=1",
		"EMITLANE_CRASH_DATABASE_URL="+e.databaseURL,
		"EMITLANE_CRASH_BROKERS="+strings.Join(e.brokers, ","),
		"EMITLANE_CRASH_TOPIC="+topic,
		"EMITLANE_CRASH_GROUP="+group,
		"EMITLANE_CRASH_CONSUMER="+consumerName,
		"EMITLANE_CRASH_SIGNAL="+listener.Addr().String(),
	)
	var childOutput bytes.Buffer
	cmd.Stdout, cmd.Stderr = &childOutput, &childOutput
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	childStopped := false
	t.Cleanup(func() {
		if !childStopped && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	connection, err := listener.Accept()
	if err != nil {
		t.Fatalf("wait for child inflight transaction: %v; child output: %s", err, childOutput.String())
	}
	_ = connection.Close()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	childStopped = true

	store := newManagedInboxStore(t, e)
	run := launchManaged(t, e, managedTestConfig(consumerName, "abrupt-replacement"), store,
		newManagedFactory(t, e, topic, group), func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 2)`, message.EventID.String())
			return err
		})
	defer run.stop(t)
	event := waitManagedInboxStatus(t, store, consumerName, eventID, inbox.StatusProcessed, 25*time.Second)
	waitCommittedOffset(t, e, group, topic, 0, 1, 15*time.Second)
	if event.Attempts != 2 {
		t.Fatalf("attempts after abrupt process loss=%d, want 2", event.Attempts)
	}
	var effects int
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM public.business_payments WHERE order_id=$1`, eventID.String()).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("protected effects after abrupt loss=%d, want 1", effects)
	}
}

func TestManagedConsumerAbruptProcessHelper(t *testing.T) {
	if os.Getenv("EMITLANE_CONSUMER_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("EMITLANE_CRASH_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.NewInboxStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{
		Brokers: strings.Split(os.Getenv("EMITLANE_CRASH_BROKERS"), ","),
		Group:   os.Getenv("EMITLANE_CRASH_GROUP"), Topics: []string{os.Getenv("EMITLANE_CRASH_TOPIC")},
		SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	config := managedTestConfig(os.Getenv("EMITLANE_CRASH_CONSUMER"), "abrupt-child")
	config.LeaseDuration = 800 * time.Millisecond
	config.LeaseRenewInterval = 200 * time.Millisecond
	runtime, err := managed.New(config, pool, store, factory, func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 1)`, message.EventID.String()); err != nil {
			return err
		}
		connection, err := net.DialTimeout("tcp", os.Getenv("EMITLANE_CRASH_SIGNAL"), 5*time.Second)
		if err != nil {
			return err
		}
		_ = connection.Close()
		select {}
	}, managed.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxKafkaInboxEndToEndSurvivesRedelivery(t *testing.T) {
	e := startEnv(t)
	inputTopic := "e2e-input-" + uuid.NewString()
	outputTopic := "e2e-output-" + uuid.NewString()
	group := "e2e-group-" + uuid.NewString()
	e.ensureTopic(t, inputTopic)
	e.ensureTopic(t, outputTopic)
	sourceID := enqueueOrder(t, e, "e2e-order", inputTopic, 42, true)
	runRelay(t, e.newRelay(t, relay.Config{InstanceID: "e2e-relay"}, e.publisher(t), relay.FailureHooks{}))

	store := newManagedInboxStore(t, e)
	baseFactory := newManagedFactory(t, e, inputTopic, group)
	commitFailed := make(chan struct{}, 1)
	failingFactory := &failFirstCommitFactory{inner: baseFactory, failed: commitFailed}
	var calls atomic.Int32
	handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		calls.Add(1)
		if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ('e2e-order', 42)`); err != nil {
			return err
		}
		_, err := e.writer.Enqueue(ctx, tx, outbox.Event{
			Destination: outputTopic,
			Type:        "payment.recorded",
			Key:         []byte("e2e-order"),
			Payload:     []byte(`{"order_id":"e2e-order"}`),
			CausationID: message.EventID.String(),
		})
		return err
	}
	first := launchManaged(t, e, managedTestConfig("e2e-consumer-v1", "e2e-consumer-a"), store, failingFactory, handler)
	select {
	case <-commitFailed:
	case <-time.After(20 * time.Second):
		t.Fatal("E2E did not reach post-DB-commit redelivery window")
	}
	first.stop(t)
	e.waitStatus(t, sourceID, "delivered", 20*time.Second)

	second := launchManaged(t, e, managedTestConfig("e2e-consumer-v1", "e2e-consumer-b"), store,
		newManagedFactory(t, e, inputTopic, group), handler)
	defer second.stop(t)
	waitCommittedOffset(t, e, group, inputTopic, 0, 1, 20*time.Second)
	eventID, err := uuid.Parse(sourceID)
	if err != nil {
		t.Fatal(err)
	}
	event := waitManagedInboxStatus(t, store, "e2e-consumer-v1", eventID, inbox.StatusProcessed, 10*time.Second)
	if event.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("E2E Inbox attempts=%d handler calls=%d", event.Attempts, calls.Load())
	}
	var businessEffects, derivedEvents int
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM public.business_payments WHERE order_id='e2e-order'`).Scan(&businessEffects); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM emitlane.outbox_events WHERE destination=$1 AND causation_id=$2`, outputTopic, sourceID).Scan(&derivedEvents); err != nil {
		t.Fatal(err)
	}
	if businessEffects != 1 || derivedEvents != 1 {
		t.Fatalf("E2E effects=%d derived_outbox=%d, want 1/1", businessEffects, derivedEvents)
	}
	var derivedID string
	if err := e.pool.QueryRow(context.Background(), `SELECT id::TEXT FROM emitlane.outbox_events WHERE destination=$1 AND causation_id=$2`, outputTopic, sourceID).Scan(&derivedID); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(t, derivedID, "delivered", 20*time.Second)
	records := consumeRecords(t, e.brokers, outputTopic, 1, 20*time.Second)
	if got := headerValue(records[0], "emitlane-event-id"); got != derivedID {
		t.Fatalf("derived Kafka event ID=%q want %q", got, derivedID)
	}
}

type runningManaged struct {
	cancel  context.CancelFunc
	done    chan error
	stopped atomic.Bool
}

func launchManaged(t *testing.T, e *env, config managed.Config, store *pgstore.InboxStore, factory managed.SourceFactory, handler managed.Handler) *runningManaged {
	t.Helper()
	runtime, err := managed.New(config, e.pool, store, factory, handler, managed.WithLogger(e.log))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &runningManaged{cancel: cancel, done: make(chan error, 1)}
	go func() { run.done <- runtime.Run(ctx) }()
	t.Cleanup(func() { run.stop(t) })
	return run
}

func (r *runningManaged) stop(t *testing.T) {
	t.Helper()
	if !r.stopped.CompareAndSwap(false, true) {
		return
	}
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("managed consumer stop: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Error("managed consumer did not stop")
	}
}

func assertManagedStillRunning(t *testing.T, run *runningManaged, duration time.Duration) {
	t.Helper()
	select {
	case err := <-run.done:
		run.stopped.Store(true)
		t.Fatalf("managed consumer stopped during recoverable outage: %v", err)
	case <-time.After(duration):
	}
}

func managedTestConfig(consumerName, instanceID string) managed.Config {
	config := managed.DefaultConfig()
	config.Consumer = consumerName
	config.InstanceID = instanceID
	config.HandlerTimeout = 2 * time.Second
	config.LeaseDuration = 2 * time.Second
	config.LeaseRenewInterval = 500 * time.Millisecond
	config.MaintenancePoll = 25 * time.Millisecond
	config.ShutdownTimeout = 10 * time.Second
	return config
}

func newManagedFactory(t *testing.T, e *env, topic, group string) managed.SourceFactory {
	t.Helper()
	factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{
		Brokers: e.brokers, Group: group, Topics: []string{topic},
		SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return factory
}

func newManagedInboxStore(t *testing.T, e *env) *pgstore.InboxStore {
	t.Helper()
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type failFirstCommitFactory struct {
	inner  managed.SourceFactory
	failed chan<- struct{}
	done   atomic.Bool
}

func (f *failFirstCommitFactory) NewSource(workerID string, listener managed.RebalanceListener) (managed.Source, error) {
	source, err := f.inner.NewSource(workerID, listener)
	if err != nil {
		return nil, err
	}
	return &failFirstCommitSource{Source: source, factory: f}, nil
}

type failFirstCommitSource struct {
	managed.Source
	factory *failFirstCommitFactory
}

func (s *failFirstCommitSource) Commit(ctx context.Context, record managed.SourceRecord) error {
	if s.factory.done.CompareAndSwap(false, true) {
		select {
		case s.factory.failed <- struct{}{}:
		default:
		}
		return errors.New("injected offset commit failure")
	}
	return s.Source.Commit(ctx, record)
}

func produceManagedRecordAtPartition(t *testing.T, e *env, topic string, partition int32, eventID uuid.UUID, payload []byte) {
	t.Helper()
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(e.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	headers := []kgo.RecordHeader{{Key: managed.EventIDHeader, Value: []byte(eventID.String())}}
	if eventID == uuid.Nil {
		headers[0].Value = []byte("not-a-uuid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: payload, Headers: headers})
	if err := result.FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func committedOffset(t *testing.T, e *env, group, topic string, partition int32) int64 {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	offsets, err := kadm.NewClient(client).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	offset, ok := offsets.Lookup(topic, partition)
	if !ok || offset.Err != nil {
		return -1
	}
	return offset.At
}

func waitCommittedOffset(t *testing.T, e *env, group, topic string, partition int32, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := int64(-1)
	for time.Now().Before(deadline) {
		last = committedOffset(t, e, group, topic, partition)
		if last == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("committed offset %s[%d]=%d, want %d", topic, partition, last, want)
}
