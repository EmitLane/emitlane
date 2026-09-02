//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
)

func orderedRelayConfig(instance string) relay.Config {
	return relay.Config{
		InstanceID:                instance,
		Concurrency:               4,
		LeaseDuration:             2 * time.Second,
		PublishTimeout:            500 * time.Millisecond,
		OrderingLeaseDuration:     2 * time.Second,
		OrderingRebalanceInterval: 50 * time.Millisecond,
		OrderingSafetyMargin:      100 * time.Millisecond,
	}
}

func enqueueOrderedInTx(
	t *testing.T,
	e *env,
	tx pgx.Tx,
	topic, key string,
	sequence, start int64,
) string {
	t.Helper()
	id, err := e.writer.Enqueue(context.Background(), tx, outbox.Event{
		Destination:           topic,
		Type:                  "order.changed",
		Payload:               []byte(strconv.FormatInt(sequence, 10)),
		Headers:               map[string]string{broker.HeaderSequence: "user-conflict"},
		OrderingKey:           key,
		Sequence:              sequence,
		OrderingStartSequence: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func enqueueOrdered(t *testing.T, e *env, topic, key string, sequence int64) string {
	t.Helper()
	tx, err := e.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	id := enqueueOrderedInTx(t, e, tx, topic, key, sequence, 0)
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return id
}

func consumeTopicRecords(t *testing.T, brokers []string, topic string, count int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(records) < count && ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("consume ordered records: %v", errs[0])
		}
		records = append(records, fetches.Records()...)
	}
	if len(records) < count {
		t.Fatalf("received %d/%d ordered records", len(records), count)
	}
	return records[:count]
}

func recordSequence(t *testing.T, record *kgo.Record) int64 {
	t.Helper()
	sequence, err := strconv.ParseInt(headerValue(record, broker.HeaderSequence), 10, 64)
	if err != nil {
		t.Fatalf("record sequence header: %v", err)
	}
	return sequence
}

func TestOrderedCommitInversionPublishesSequenceOneThenTwo(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sequenceOneTx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sequenceOneTx.Rollback(ctx)

	sequenceTwoTx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sequenceTwoID := enqueueOrderedInTx(t, e, sequenceTwoTx, topic, "order:inversion", 2, 0)
	if err := sequenceTwoTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	runRelay(t, e.newRelay(t, orderedRelayConfig("inversion-relay"), e.publisher(t), relay.FailureHooks{}))
	time.Sleep(500 * time.Millisecond)
	future := e.getEvent(t, sequenceTwoID)
	if future.Status != "pending" || future.Attempts != 0 {
		t.Fatalf("sequence 2 before sequence 1: status=%s attempts=%d", future.Status, future.Attempts)
	}

	sequenceOneID := enqueueOrderedInTx(t, e, sequenceOneTx, topic, "order:inversion", 1, 0)
	if err := sequenceOneTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(t, sequenceOneID, "delivered", 20*time.Second)
	e.waitStatus(t, sequenceTwoID, "delivered", 20*time.Second)
	records := consumeTopicRecords(t, e.brokers, topic, 2, 15*time.Second)
	if first, second := recordSequence(t, records[0]), recordSequence(t, records[1]); first != 1 || second != 2 {
		t.Fatalf("broker sequence = %d,%d; want 1,2", first, second)
	}
	for _, record := range records {
		if string(record.Key) != "order:inversion" {
			t.Fatalf("ordered Kafka key = %q", record.Key)
		}
		if headerValue(record, broker.HeaderOrderingKey) != "order:inversion" ||
			headerValue(record, broker.HeaderPartition) == "" {
			t.Fatalf("ordered headers missing: %+v", record.Headers)
		}
	}
}

func TestBlockedOrderedStreamDoesNotBlockIndependentStream(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	blocked := enqueueOrdered(t, e, topic, "order:A", 2)
	ready := enqueueOrdered(t, e, topic, "order:B", 1)
	runRelay(t, e.newRelay(t, orderedRelayConfig("independent-relay"), e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, ready, "delivered", 15*time.Second)
	time.Sleep(300 * time.Millisecond)
	event := e.getEvent(t, blocked)
	if event.Status != "pending" || event.Attempts != 0 {
		t.Fatalf("blocked stream event status=%s attempts=%d", event.Status, event.Attempts)
	}
}

type sequencePublisher struct {
	mu              sync.Mutex
	failuresLeft    int
	alwaysFail      bool
	successful      []int64
	acknowledged    chan struct{}
	acknowledgeOnce sync.Once
}

func (p *sequencePublisher) Publish(_ context.Context, message broker.Message) error {
	sequence, _ := strconv.ParseInt(message.Headers[broker.HeaderSequence], 10, 64)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.alwaysFail || p.failuresLeft > 0 {
		if p.failuresLeft > 0 {
			p.failuresLeft--
		}
		return errors.New("injected ordered publish failure")
	}
	p.successful = append(p.successful, sequence)
	if p.acknowledged != nil {
		p.acknowledgeOnce.Do(func() { close(p.acknowledged) })
	}
	return nil
}

func (*sequencePublisher) Close() error { return nil }

func (p *sequencePublisher) sequences() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.successful...)
}

func TestOrderedRetryAndDeadBlockLaterSequence(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		e := startEnv(t)
		topic := topicName(t, e)
		first := enqueueOrdered(t, e, topic, "order:retry", 1)
		second := enqueueOrdered(t, e, topic, "order:retry", 2)
		publisher := &sequencePublisher{failuresLeft: 1}
		cfg := orderedRelayConfig("retry-relay")
		cfg.BaseDelay = 100 * time.Millisecond
		cfg.MaxDelay = 100 * time.Millisecond
		runRelay(t, e.newRelay(t, cfg, publisher, relay.FailureHooks{}))
		e.waitStatus(t, first, "delivered", 15*time.Second)
		e.waitStatus(t, second, "delivered", 15*time.Second)
		got := publisher.sequences()
		if fmt.Sprint(got) != "[1 2]" {
			t.Fatalf("successful publish sequence = %v", got)
		}
	})

	t.Run("dead retry", func(t *testing.T) {
		e := startEnv(t)
		topic := topicName(t, e)
		first := enqueueOrdered(t, e, topic, "order:dead", 1)
		second := enqueueOrdered(t, e, topic, "order:dead", 2)
		failing := &sequencePublisher{alwaysFail: true}
		cfg := orderedRelayConfig("dead-relay")
		cfg.MaxAttempts = 2
		cfg.BaseDelay = time.Millisecond
		cfg.MaxDelay = 5 * time.Millisecond
		stop := runRelay(t, e.newRelay(t, cfg, failing, relay.FailureHooks{}))
		e.waitStatus(t, first, "dead", 15*time.Second)
		time.Sleep(200 * time.Millisecond)
		blocked := e.getEvent(t, second)
		if blocked.Status != "pending" || blocked.Attempts != 0 {
			t.Fatalf("later sequence bypassed dead event: status=%s attempts=%d", blocked.Status, blocked.Attempts)
		}
		stop()
		if err := e.store.RetryDead(context.Background(), mustUUID(t, first)); err != nil {
			t.Fatal(err)
		}
		publisher := &sequencePublisher{}
		runRelay(t, e.newRelay(t, orderedRelayConfig("dead-recovery"), publisher, relay.FailureHooks{}))
		e.waitStatus(t, first, "delivered", 15*time.Second)
		e.waitStatus(t, second, "delivered", 15*time.Second)
		if got := publisher.sequences(); fmt.Sprint(got) != "[1 2]" {
			t.Fatalf("dead retry publish sequence = %v", got)
		}
	})
}

func TestOrderedAckCrashDuplicatesCurrentBeforeNext(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	first := enqueueOrdered(t, e, topic, "order:ack-crash", 1)
	second := enqueueOrdered(t, e, topic, "order:ack-crash", 2)
	ack := make(chan struct{})
	stop := runRelay(t, e.newRelay(t, orderedRelayConfig("ordered-ack-crasher"), e.publisher(t), relay.FailureHooks{
		AfterPublishAck: func(_ context.Context, event relay.Event) error {
			if event.OrderingSequence == 1 {
				select {
				case <-ack:
				default:
					close(ack)
				}
				return errors.New("injected ordered ACK crash")
			}
			return nil
		},
	}))
	select {
	case <-ack:
	case <-time.After(10 * time.Second):
		t.Fatal("ordered ACK hook did not run")
	}
	stop()
	time.Sleep(2200 * time.Millisecond)
	runRelay(t, e.newRelay(t, orderedRelayConfig("ordered-ack-recovery"), e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, first, "delivered", 20*time.Second)
	e.waitStatus(t, second, "delivered", 20*time.Second)
	records := consumeTopicRecords(t, e.brokers, topic, 3, 15*time.Second)
	got := []int64{recordSequence(t, records[0]), recordSequence(t, records[1]), recordSequence(t, records[2])}
	if fmt.Sprint(got) != "[1 1 2]" {
		t.Fatalf("ACK crash broker sequence = %v, want [1 1 2]", got)
	}
}

func TestStaleOrderedOwnerCannotMutateOrAdvance(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrdered(t, e, topic, "order:stale", 1)
	registerOrderingRelay(t, e, "stale-a")
	partitions := reconcileOrdering(t, e, "stale-a")
	_ = partitions
	claimed, err := e.store.ClaimOrdered(context.Background(), "stale-a", 1,
		2*time.Second, testPublishWindow+testOrderingSafety)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ordered claim: count=%d err=%v", len(claimed), err)
	}
	event := claimed[0]
	if event.ID.String() != id {
		t.Fatalf("claimed event %s, want %s", event.ID, id)
	}
	if _, err := e.store.BeginOrderedAttempt(context.Background(), event.ID, "stale-a",
		event.OrderingEpoch, 10, testPublishWindow+testOrderingSafety); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ReleaseOrderingPartitions(context.Background(), "stale-a", testOrderingSafety); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkRelayStopped(context.Background(), "stale-a"); err != nil {
		t.Fatal(err)
	}
	registerOrderingRelay(t, e, "stale-b")
	reconcileOrdering(t, e, "stale-b")

	if _, err := e.store.BeginOrderedAttempt(context.Background(), event.ID, "stale-a", event.OrderingEpoch, 10, time.Millisecond); err == nil {
		t.Fatal("stale owner began publish attempt")
	}
	if err := e.store.MarkOrderedRetry(context.Background(), event, "stale-a", time.Second, "stale"); err == nil {
		t.Fatal("stale owner marked retry")
	}
	if err := e.store.MarkOrderedDead(context.Background(), event, "stale-a", "stale"); err == nil {
		t.Fatal("stale owner marked dead")
	}
	if err := e.store.MarkOrderedDelivered(context.Background(), event, "stale-a"); err == nil {
		t.Fatal("stale owner marked delivered")
	}
	var next int64
	if err := e.pool.QueryRow(context.Background(), `
SELECT next_sequence FROM emitlane.ordering_streams
WHERE destination=$1 AND ordering_key='order:stale'`, topic).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next != 1 {
		t.Fatalf("stale owner advanced stream to %d", next)
	}
}

func mustUUID(t *testing.T, value string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
