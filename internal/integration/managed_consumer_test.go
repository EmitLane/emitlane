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
	}, managed.WithLogger(e.log))
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
	config.MaintenancePoll = 50 * time.Millisecond
	runtime, err := managed.New(config, e.pool, inboxStore, factory, func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		if message.EventID != eventID || message.Attempt != 1 {
			return fmt.Errorf("unexpected message identity=%s attempt=%d", message.EventID, message.Attempt)
		}
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, 42)`, message.EventID.String())
		return err
	})
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
