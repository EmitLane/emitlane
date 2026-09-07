//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkaadapter "github.com/emitlane/emitlane/broker/kafka"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/inbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestManagedConsumerCommitsOnlyAfterDurableProcessing(t *testing.T) {
	e := startEnv(t)
	topic := "managed-basic-" + uuid.NewString()
	group := "managed-group-" + uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	producer, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	produceCtx, produceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	result := producer.ProduceSync(produceCtx, &kgo.Record{
		Topic: topic, Value: []byte("42"),
		Headers: []kgo.RecordHeader{{Key: managed.EventIDHeader, Value: []byte(eventID.String())}},
	})
	produceCancel()
	producer.Close()
	if err := result.FirstErr(); err != nil {
		t.Fatal(err)
	}

	factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{
		Brokers: e.brokers, ClientID: "managed-integration", Group: group, Topics: []string{topic},
		SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	inboxStore, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	config := managed.DefaultConfig()
	config.Consumer = "billing-v1"
	config.InstanceID = "billing-instance"
	config.HandlerTimeout = 2 * time.Second
	config.LeaseDuration = 4 * time.Second
	config.LeaseRenewInterval = time.Second
	config.MaintenancePoll = 50 * time.Millisecond
	runtime, err := managed.New(config, e.pool, inboxStore, factory, func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		if message.EventID != eventID || message.Attempt != 1 {
			return fmt.Errorf("unexpected message identity=%s attempt=%d", message.EventID, message.Attempt)
		}
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 42)`, message.EventID.String())
		return err
	}, managed.WithLogger(e.log))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	runtimeStopped := false
	t.Cleanup(func() {
		if runtimeStopped {
			return
		}
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("managed consumer stop: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("managed consumer did not stop")
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case runtimeErr := <-done:
			runtimeStopped = true
			t.Fatalf("managed consumer stopped early: %v", runtimeErr)
		default:
		}
		ctx, queryCancel := context.WithTimeout(context.Background(), time.Second)
		event, eventErr := inboxStore.Get(ctx, config.Consumer, eventID)
		var effects int
		queryErr := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.business_payments WHERE order_id=$1`, eventID.String()).Scan(&effects)
		queryCancel()
		if eventErr == nil && queryErr == nil && event.Status == inbox.StatusProcessed && effects == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	event, err := inboxStore.Get(context.Background(), config.Consumer, eventID)
	if err != nil || event.Status != inbox.StatusProcessed {
		t.Fatalf("Inbox event=%+v err=%v", event, err)
	}

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	offsets, err := kadm.NewClient(adminClient).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	committed, ok := offsets.Lookup(topic, 0)
	if !ok || committed.Err != nil || committed.At != 1 {
		t.Fatalf("committed offset=%+v exists=%t, want 1", committed, ok)
	}
}

func TestManagedConsumerRenewsHandlerLease(t *testing.T) {
	e := startEnv(t)
	topic := "managed-renew-" + uuid.NewString()
	group := "managed-renew-group-" + uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	producer, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	result := producer.ProduceSync(context.Background(), &kgo.Record{
		Topic: topic, Value: []byte("renew"),
		Headers: []kgo.RecordHeader{{Key: managed.EventIDHeader, Value: []byte(eventID.String())}},
	})
	producer.Close()
	if err := result.FirstErr(); err != nil {
		t.Fatal(err)
	}
	factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{
		Brokers: e.brokers, Group: group, Topics: []string{topic},
		SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	config := managed.DefaultConfig()
	config.Consumer = "renew-v1"
	config.InstanceID = "renew-instance"
	config.HandlerTimeout = 3 * time.Second
	config.LeaseDuration = 600 * time.Millisecond
	config.LeaseRenewInterval = 150 * time.Millisecond
	config.MaintenancePoll = 50 * time.Millisecond
	runtime, err := managed.New(config, e.pool, store, factory, func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		close(started)
		select {
		case <-release:
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 7)`, message.EventID.String())
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}, managed.WithLogger(e.log))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	defer func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("managed consumer stop: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("managed consumer did not stop")
		}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start")
	}
	first, err := store.Get(context.Background(), config.Consumer, eventID)
	if err != nil || first.LeaseUntil == nil {
		t.Fatalf("initial lease event=%+v err=%v", first, err)
	}
	time.Sleep(400 * time.Millisecond)
	second, err := store.Get(context.Background(), config.Consumer, eventID)
	if err != nil || second.LeaseUntil == nil {
		t.Fatalf("renewed lease event=%+v err=%v", second, err)
	}
	if !second.LeaseUntil.After(*first.LeaseUntil) {
		t.Fatalf("lease was not renewed: first=%s second=%s", first.LeaseUntil, second.LeaseUntil)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		event, getErr := store.Get(context.Background(), config.Consumer, eventID)
		if getErr == nil && event.Status == inbox.StatusProcessed {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("renewed event did not reach processed")
}

func TestManagedConsumerDurableRetryRollsBackFailedAttempt(t *testing.T) {
	e := startEnv(t)
	topic := "managed-retry-" + uuid.NewString()
	group := "managed-retry-group-" + uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("retry"))
	var calls atomic.Int32
	store := startManagedTestRuntime(t, e, topic, group, "retry-handler-v1", func(config *managed.Config) {
		config.MaxAttempts = 3
		config.BaseDelay = 50 * time.Millisecond
		config.MaxDelay = 100 * time.Millisecond
		config.Jitter = 0
	}, func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		attempt := calls.Add(1)
		if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`, message.EventID.String(), attempt); err != nil {
			return err
		}
		if attempt == 1 {
			return errors.New("temporary database rule")
		}
		return nil
	})
	event := waitManagedInboxStatus(t, store, "retry-handler-v1", eventID, inbox.StatusProcessed, 10*time.Second)
	if event.Attempts != 2 || calls.Load() != 2 {
		t.Fatalf("retry event attempts=%d handler calls=%d", event.Attempts, calls.Load())
	}
	var effects int
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM public.business_payments WHERE order_id=$1`, eventID.String()).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("protected effects=%d, want 1", effects)
	}
}

func TestManagedConsumerPermanentErrorBlocksPartition(t *testing.T) {
	e := startEnv(t)
	topic := "managed-dead-" + uuid.NewString()
	group := "managed-dead-group-" + uuid.NewString()
	e.ensureTopic(t, topic)
	deadID := uuid.New()
	laterID := uuid.New()
	produceManagedRecord(t, e, topic, deadID, []byte("dead"))
	produceManagedRecord(t, e, topic, laterID, []byte("later"))
	var calls atomic.Int32
	store := startManagedTestRuntime(t, e, topic, group, "dead-handler-v1", nil,
		func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
			attempt := calls.Add(1)
			if attempt == 1 {
				return inbox.Permanent(errors.New("unsupported domain value"))
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`, message.EventID.String(), attempt)
			return err
		})
	event := waitManagedInboxStatus(t, store, "dead-handler-v1", deadID, inbox.StatusDead, 10*time.Second)
	if event.Attempts != 1 || event.LastError != "permanent handler failure" {
		t.Fatalf("dead event=%+v", event)
	}
	time.Sleep(500 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("handler calls=%d, later same-partition record ran", calls.Load())
	}
	if _, err := store.Get(context.Background(), "dead-handler-v1", laterID); !errors.Is(err, inbox.ErrNotFound) {
		t.Fatalf("later same-partition Inbox state error=%v", err)
	}
	if err := store.RetryDead(context.Background(), inbox.RetryRequest{
		Consumer: "dead-handler-v1", EventID: deadID,
		Actor: "integration-test", Reason: "domain value mapping fixed",
	}); err != nil {
		t.Fatal(err)
	}
	retried := waitManagedInboxStatus(t, store, "dead-handler-v1", deadID, inbox.StatusProcessed, 10*time.Second)
	if retried.Attempts != 2 {
		t.Fatalf("retried attempts=%d, want 2", retried.Attempts)
	}
	waitManagedInboxStatus(t, store, "dead-handler-v1", laterID, inbox.StatusProcessed, 10*time.Second)
	if calls.Load() != 3 {
		t.Fatalf("handler calls=%d, want dead retry then later record", calls.Load())
	}
	var audits int
	if err := e.pool.QueryRow(context.Background(), `
SELECT COUNT(*) FROM emitlane.admin_audit_log
WHERE action='inbox.retry' AND target_event_id=$1 AND reason='domain value mapping fixed'`, deadID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("retry audit rows=%d, want 1", audits)
	}
}

func TestManagedConsumerHandlerTimeoutRetriesTransaction(t *testing.T) {
	e := startEnv(t)
	topic := "managed-timeout-" + uuid.NewString()
	group := "managed-timeout-group-" + uuid.NewString()
	e.ensureTopic(t, topic)
	eventID := uuid.New()
	produceManagedRecord(t, e, topic, eventID, []byte("timeout"))
	var calls atomic.Int32
	store := startManagedTestRuntime(t, e, topic, group, "timeout-handler-v1", func(config *managed.Config) {
		config.HandlerTimeout = 120 * time.Millisecond
		config.LeaseDuration = 600 * time.Millisecond
		config.LeaseRenewInterval = 150 * time.Millisecond
		config.BaseDelay = 40 * time.Millisecond
		config.MaxDelay = 80 * time.Millisecond
		config.Jitter = 0
	}, func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		attempt := calls.Add(1)
		if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`, message.EventID.String(), attempt); err != nil {
			return err
		}
		if attempt == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	event := waitManagedInboxStatus(t, store, "timeout-handler-v1", eventID, inbox.StatusProcessed, 10*time.Second)
	if event.Attempts != 2 || calls.Load() != 2 {
		t.Fatalf("timeout event attempts=%d calls=%d", event.Attempts, calls.Load())
	}
	var amount int
	if err := e.pool.QueryRow(context.Background(), `SELECT amount FROM public.business_payments WHERE order_id=$1`, eventID.String()).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	if amount != 2 {
		t.Fatalf("committed attempt amount=%d, want 2", amount)
	}
}

func produceManagedRecord(t *testing.T, e *env, topic string, eventID uuid.UUID, payload []byte) {
	t.Helper()
	producer, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := producer.ProduceSync(ctx, &kgo.Record{
		Topic: topic, Value: payload,
		Headers: []kgo.RecordHeader{{Key: managed.EventIDHeader, Value: []byte(eventID.String())}},
	})
	if err := result.FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func startManagedTestRuntime(
	t *testing.T,
	e *env,
	topic, group, consumerName string,
	configure func(*managed.Config),
	handler managed.Handler,
) *pgstore.InboxStore {
	t.Helper()
	factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{
		Brokers: e.brokers, Group: group, Topics: []string{topic},
		SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := pgstore.NewInboxStore(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	config := managed.DefaultConfig()
	config.Consumer = consumerName
	config.InstanceID = consumerName + "-instance"
	config.HandlerTimeout = 2 * time.Second
	config.LeaseDuration = 2 * time.Second
	config.LeaseRenewInterval = 500 * time.Millisecond
	config.MaintenancePoll = 25 * time.Millisecond
	if configure != nil {
		configure(&config)
	}
	runtime, err := managed.New(config, e.pool, store, factory, handler, managed.WithLogger(e.log))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("managed consumer stop: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("managed consumer did not stop")
		}
	})
	return store
}

func waitManagedInboxStatus(t *testing.T, store *pgstore.InboxStore, consumerName string, eventID uuid.UUID, status inbox.Status, timeout time.Duration) inbox.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last inbox.Event
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = store.Get(context.Background(), consumerName, eventID)
		if lastErr == nil && last.Status == status {
			return last
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Inbox event %s status=%s error=%v, want %s", eventID, last.Status, lastErr, status)
	return inbox.Event{}
}
