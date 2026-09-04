//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
	adminapi "github.com/emitlane/emitlane/internal/admin"
	internalordering "github.com/emitlane/emitlane/internal/ordering"
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

func consumeTopicUntilEventIDs(
	t *testing.T,
	brokers []string,
	topic string,
	expected map[string]struct{},
	timeout time.Duration,
) []*kgo.Record {
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
	remaining := make(map[string]struct{}, len(expected))
	for id := range expected {
		remaining[id] = struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(remaining) > 0 && ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 && ctx.Err() == nil {
			t.Fatalf("consume ordered records: %v", errs[0])
		}
		for _, record := range fetches.Records() {
			records = append(records, record)
			delete(remaining, headerValue(record, broker.HeaderEventID))
		}
	}
	if len(remaining) > 0 {
		missing := make([]string, 0, len(remaining))
		for id := range remaining {
			missing = append(missing, id)
		}
		sort.Strings(missing)
		t.Fatalf("did not observe %d/%d committed event IDs before timeout: %v", len(remaining), len(expected), missing)
	}
	return records
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

func TestOrderedFutureSequenceRemainsDurableUntilGapCloses(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	future := enqueueOrdered(t, e, topic, "order:future", 3)
	runRelay(t, e.newRelay(t, orderedRelayConfig("future-relay"), e.publisher(t), relay.FailureHooks{}))
	time.Sleep(400 * time.Millisecond)
	blocked := e.getEvent(t, future)
	if blocked.Status != "pending" || blocked.Attempts != 0 {
		t.Fatalf("future sequence was not durably blocked: status=%s attempts=%d", blocked.Status, blocked.Attempts)
	}
	second := enqueueOrdered(t, e, topic, "order:future", 2)
	first := enqueueOrdered(t, e, topic, "order:future", 1)
	e.waitStatus(t, first, "delivered", 20*time.Second)
	e.waitStatus(t, second, "delivered", 20*time.Second)
	e.waitStatus(t, future, "delivered", 20*time.Second)
	records := consumeTopicRecords(t, e.brokers, topic, 3, 15*time.Second)
	for i, record := range records {
		if got, want := recordSequence(t, record), int64(i+1); got != want {
			t.Fatalf("future-gap sequence[%d]=%d, want %d", i, got, want)
		}
	}
}

type sequencePublisher struct {
	mu              sync.Mutex
	failuresLeft    int
	alwaysFail      bool
	successful      []int64
	acknowledged    chan struct{}
	acknowledgeOnce sync.Once
	failed          chan struct{}
	failureOnce     sync.Once
}

type transientKafkaPublisher struct {
	inner    broker.Publisher
	mu       sync.Mutex
	failures int
	delay    time.Duration
}

func (p *transientKafkaPublisher) Publish(ctx context.Context, message broker.Message) error {
	if p.delay > 0 {
		timer := time.NewTimer(p.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.mu.Lock()
	if p.failures > 0 {
		p.failures--
		p.mu.Unlock()
		return errors.New("injected randomized transient failure")
	}
	p.mu.Unlock()
	return p.inner.Publish(ctx, message)
}

func (*transientKafkaPublisher) Close() error { return nil }

func (p *sequencePublisher) Publish(_ context.Context, message broker.Message) error {
	sequence, _ := strconv.ParseInt(message.Headers[broker.HeaderSequence], 10, 64)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.alwaysFail || p.failuresLeft > 0 {
		if p.failuresLeft > 0 {
			p.failuresLeft--
		}
		if p.failed != nil {
			p.failureOnce.Do(func() { close(p.failed) })
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
		publisher := &sequencePublisher{failuresLeft: 1, failed: make(chan struct{})}
		cfg := orderedRelayConfig("retry-relay")
		cfg.BaseDelay = 750 * time.Millisecond
		cfg.MaxDelay = 750 * time.Millisecond
		runRelay(t, e.newRelay(t, cfg, publisher, relay.FailureHooks{}))
		select {
		case <-publisher.failed:
		case <-time.After(5 * time.Second):
			t.Fatal("ordered failure did not occur")
		}
		service := adminService(t, e, time.Second)
		if _, err := service.SetPaused(context.Background(), true, adminapi.Mutation{Actor: "integration", Reason: "inspect retry wait"}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			event := e.getEvent(t, first)
			if event.Status == "pending" && event.Attempts == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("first ordered event did not enter retry wait: %+v", event)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, err := e.pool.Exec(context.Background(), `UPDATE emitlane.outbox_events SET available_at=NOW()+INTERVAL '1 minute' WHERE id=$1`, mustUUID(t, first)); err != nil {
			t.Fatal(err)
		}
		stream, err := service.InspectOrderingStream(context.Background(), topic, "order:retry")
		if err != nil || stream.State != "retry_wait" || stream.NextSequence != 1 || stream.NextEventAttempts != 1 {
			t.Fatalf("retry-wait stream = %+v err=%v", stream, err)
		}
		if _, err := service.SetPaused(context.Background(), false, adminapi.Mutation{Actor: "integration", Reason: "retry inspected"}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.pool.Exec(context.Background(), `UPDATE emitlane.outbox_events SET available_at=NOW() WHERE id=$1`, mustUUID(t, first)); err != nil {
			t.Fatal(err)
		}
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
		stream, err := adminService(t, e, time.Second).InspectOrderingStream(context.Background(), topic, "order:dead")
		if err != nil || stream.State != "dead_blocked" || stream.NextSequence != 1 {
			t.Fatalf("dead-blocked stream = %+v err=%v", stream, err)
		}
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

func TestOrderedExplicitStartAndRetentionPreserveProgress(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := enqueueOrderedInTx(t, e, tx, topic, "order:start-50", 50, 50)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	second := enqueueOrdered(t, e, topic, "order:start-50", 51)
	runRelay(t, e.newRelay(t, orderedRelayConfig("start-retention-relay"), e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, first, "delivered", 20*time.Second)
	e.waitStatus(t, second, "delivered", 20*time.Second)
	records := consumeTopicRecords(t, e.brokers, topic, 2, 15*time.Second)
	if recordSequence(t, records[0]) != 50 || recordSequence(t, records[1]) != 51 {
		t.Fatalf("explicit-start broker sequence = %d,%d", recordSequence(t, records[0]), recordSequence(t, records[1]))
	}

	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET delivered_at=NOW()-INTERVAL '1 hour' WHERE id=ANY($1)`,
		[]uuid.UUID{mustUUID(t, first), mustUUID(t, second)}); err != nil {
		t.Fatal(err)
	}
	deleted, err := e.store.CleanupDelivered(ctx, time.Minute, 10)
	if err != nil || deleted != 2 {
		t.Fatalf("cleanup deleted=%d err=%v", deleted, err)
	}
	var next int64
	if err := e.pool.QueryRow(ctx, `SELECT next_sequence FROM emitlane.ordering_streams WHERE destination=$1 AND ordering_key=$2`,
		topic, "order:start-50").Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next != 52 {
		t.Fatalf("retention changed stream next sequence to %d", next)
	}
	tx, err = e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, err = e.writer.Enqueue(ctx, tx, outbox.Event{
		Destination: topic, Type: "order.old", OrderingKey: "order:start-50", Sequence: 49,
	})
	if !errors.Is(err, outbox.ErrSequenceAlreadyPassed) {
		t.Fatalf("old sequence after retention error = %v", err)
	}
}

func TestOrderedPauseRenewsPartitionLeaseAndBlocksClaims(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrdered(t, e, topic, "order:paused", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service := adminService(t, e, time.Second)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration", Reason: "ordered maintenance"}); err != nil {
		t.Fatal(err)
	}
	cfg := orderedRelayConfig("ordered-paused-relay")
	cfg.OrderingLeaseDuration = 900 * time.Millisecond
	runRelay(t, e.newRelay(t, cfg, e.publisher(t), relay.FailureHooks{}))
	partition := internalordering.Partition(topic, "order:paused")
	readLease := func() time.Time {
		t.Helper()
		var lease *time.Time
		if err := e.pool.QueryRow(ctx, `SELECT lease_until FROM emitlane.ordering_partitions WHERE partition_id=$1`, partition).Scan(&lease); err != nil {
			t.Fatal(err)
		}
		if lease == nil {
			t.Fatal("ordered partition has no lease")
		}
		return *lease
	}
	deadline := time.Now().Add(5 * time.Second)
	var before time.Time
	for time.Now().Before(deadline) {
		var owner *string
		if err := e.pool.QueryRow(ctx, `SELECT lease_owner FROM emitlane.ordering_partitions WHERE partition_id=$1`, partition).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if owner != nil && *owner == cfg.InstanceID {
			before = readLease()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if before.IsZero() {
		t.Fatal("paused Relay did not acquire ordered partition")
	}
	time.Sleep(200 * time.Millisecond)
	after := readLease()
	if !after.After(before) {
		t.Fatalf("partition lease did not renew while paused: before=%s after=%s", before, after)
	}
	event := e.getEvent(t, id)
	if event.Status != "pending" || event.Attempts != 0 {
		t.Fatalf("ordered event claimed while paused: status=%s attempts=%d", event.Status, event.Attempts)
	}
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "integration", Reason: "ordered maintenance complete"}); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(t, id, "delivered", 20*time.Second)
}

func TestMultipleRelaysPreserveStreamsAndKafkaPartitionAffinity(t *testing.T) {
	e := startEnv(t)
	topic := "ordered-multipart-" + uuid.NewString()
	e.ensureTopicPartitions(t, topic, 8)
	const streams = 24
	const perStream = 3
	committed := make(map[string]struct{}, streams*perStream)
	for stream := 0; stream < streams; stream++ {
		key := fmt.Sprintf("order:multi:%02d", stream)
		for sequence := int64(1); sequence <= perStream; sequence++ {
			committed[enqueueOrdered(t, e, topic, key, sequence)] = struct{}{}
		}
	}
	for _, id := range []string{"multi-relay-a", "multi-relay-b", "multi-relay-c"} {
		runRelay(t, e.newRelay(t, orderedRelayConfig(id), e.publisher(t), relay.FailureHooks{}))
	}
	records := consumeTopicUntilEventIDs(t, e.brokers, topic, committed, 30*time.Second)
	type observed struct {
		partition int32
		offset    int64
		sequence  int64
	}
	byKey := make(map[string][]observed, streams)
	partitionByKey := make(map[string]int32, streams)
	usedPartitions := make(map[int32]struct{})
	seenIDs := make(map[string]struct{}, len(records))
	for _, record := range records {
		key := string(record.Key)
		sequence := recordSequence(t, record)
		byKey[key] = append(byKey[key], observed{partition: record.Partition, offset: record.Offset, sequence: sequence})
		if prior, ok := partitionByKey[key]; ok && prior != record.Partition {
			t.Fatalf("stream %s reached Kafka partitions %d and %d", key, prior, record.Partition)
		}
		partitionByKey[key] = record.Partition
		usedPartitions[record.Partition] = struct{}{}
		seenIDs[headerValue(record, broker.HeaderEventID)] = struct{}{}
	}
	for id := range committed {
		if _, ok := seenIDs[id]; !ok {
			t.Fatalf("committed event %s was not observed", id)
		}
	}
	for key, observations := range byKey {
		sort.Slice(observations, func(i, j int) bool { return observations[i].offset < observations[j].offset })
		var previous int64
		for _, item := range observations {
			if item.sequence < previous || item.sequence > previous+1 {
				t.Fatalf("stream %s sequence at offset %d advanced from %d to %d", key, item.offset, previous, item.sequence)
			}
			previous = item.sequence
		}
		if previous != perStream {
			t.Fatalf("stream %s ended at sequence %d, want %d", key, previous, perStream)
		}
	}
	if len(byKey) != streams || len(usedPartitions) < 2 {
		t.Fatalf("stream/partition distribution: streams=%d Kafka partitions=%d", len(byKey), len(usedPartitions))
	}
}

func TestOrderedGracefulRebalanceMovesPartitionWithoutRegression(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	const ownerA = "graceful-a"
	const ownerB = "graceful-b"
	key := ""
	var partition int16
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("order:graceful:%d", i)
		candidatePartition := internalordering.Partition(topic, candidate)
		if internalordering.DesiredOwner(candidatePartition, []string{ownerA, ownerB}) == ownerB {
			key, partition = candidate, candidatePartition
			break
		}
	}
	if key == "" {
		t.Fatal("could not find stream assigned to joining Relay")
	}
	first := enqueueOrdered(t, e, topic, key, 1)
	runRelay(t, e.newRelay(t, orderedRelayConfig(ownerA), e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, first, "delivered", 20*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service := adminService(t, e, time.Second)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration", Reason: "rebalance observation"}); err != nil {
		t.Fatal(err)
	}
	second := enqueueOrdered(t, e, topic, key, 2)
	runRelay(t, e.newRelay(t, orderedRelayConfig(ownerB), e.publisher(t), relay.FailureHooks{}))
	deadline := time.Now().Add(10 * time.Second)
	for {
		var actual *string
		var handoffPassed bool
		if err := e.pool.QueryRow(ctx, `
SELECT lease_owner, COALESCE(handoff_not_before <= NOW(), TRUE)
FROM emitlane.ordering_partitions WHERE partition_id=$1`, partition).Scan(&actual, &handoffPassed); err != nil {
			t.Fatal(err)
		}
		if actual != nil && *actual == ownerB && handoffPassed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("partition %d did not move to %s after handoff", partition, ownerB)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if event := e.getEvent(t, second); event.Status != "pending" || event.Attempts != 0 {
		t.Fatalf("paused sequence 2 was claimed during rebalance: %+v", event)
	}
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "integration", Reason: "rebalance complete"}); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(t, second, "delivered", 20*time.Second)
	records := consumeTopicRecords(t, e.brokers, topic, 2, 15*time.Second)
	if recordSequence(t, records[0]) != 1 || recordSequence(t, records[1]) != 2 {
		t.Fatalf("graceful rebalance sequence = %d,%d", recordSequence(t, records[0]), recordSequence(t, records[1]))
	}
}

func TestOrderedCrashTakeoverResumesDeliveryAfterHandoff(t *testing.T) {
	e := startEnv(t)
	topic := topicName(t, e)
	id := enqueueOrdered(t, e, topic, "order:takeover", 1)
	registerOrderingRelay(t, e, "crashed-owner")
	_, err := e.store.ReconcileOrderingPartitions(context.Background(), "crashed-owner",
		350*time.Millisecond, 100*time.Millisecond, 5*time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkRelayStopped(context.Background(), "crashed-owner"); err != nil {
		t.Fatal(err)
	}
	cfg := orderedRelayConfig("takeover-relay")
	cfg.OrderingLeaseDuration = 600 * time.Millisecond
	cfg.PublishTimeout = 100 * time.Millisecond
	cfg.OrderingSafetyMargin = 50 * time.Millisecond
	runRelay(t, e.newRelay(t, cfg, e.publisher(t), relay.FailureHooks{}))
	e.waitStatus(t, id, "delivered", 20*time.Second)
	records := consumeTopicRecords(t, e.brokers, topic, 1, 10*time.Second)
	if got := recordSequence(t, records[0]); got != 1 {
		t.Fatalf("takeover sequence = %d", got)
	}
}

func TestOrderedRandomizedReliabilityScenario(t *testing.T) {
	e := startEnv(t)
	topic := "ordered-random-" + uuid.NewString()
	e.ensureTopicPartitions(t, topic, 6)
	const streams = 12
	const perStream = 5
	type plannedEvent struct {
		key      string
		sequence int64
	}
	planned := make([]plannedEvent, 0, streams*perStream)
	for stream := 0; stream < streams; stream++ {
		for sequence := int64(1); sequence <= perStream; sequence++ {
			planned = append(planned, plannedEvent{
				key: fmt.Sprintf("order:random:%02d", stream), sequence: sequence,
			})
		}
	}
	random := rand.New(rand.NewSource(303))
	random.Shuffle(len(planned), func(i, j int) { planned[i], planned[j] = planned[j], planned[i] })
	committed := make(map[string]struct{}, len(planned))
	for _, item := range planned {
		committed[enqueueOrdered(t, e, topic, item.key, item.sequence)] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	service := adminService(t, e, time.Second)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration", Reason: "randomized startup"}); err != nil {
		t.Fatal(err)
	}
	newPublisher := func(failures int) broker.Publisher {
		return &transientKafkaPublisher{inner: e.publisher(t), failures: failures, delay: 5 * time.Millisecond}
	}
	stopA := runRelay(t, e.newRelay(t, orderedRelayConfig("random-relay-a"), newPublisher(3), relay.FailureHooks{}))
	runRelay(t, e.newRelay(t, orderedRelayConfig("random-relay-b"), newPublisher(2), relay.FailureHooks{}))
	time.Sleep(250 * time.Millisecond)
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "integration", Reason: "randomized run"}); err != nil {
		t.Fatal(err)
	}

	waitDeliveredAtLeast := func(want int) {
		t.Helper()
		for {
			var delivered int
			if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE destination=$1 AND status='delivered'`, topic).Scan(&delivered); err != nil {
				t.Fatal(err)
			}
			if delivered >= want {
				return
			}
			if ctx.Err() != nil {
				t.Fatalf("delivered %d/%d before timeout", delivered, want)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	waitDeliveredAtLeast(8)
	stopA()
	runRelay(t, e.newRelay(t, orderedRelayConfig("random-relay-c"), newPublisher(2), relay.FailureHooks{}))
	waitDeliveredAtLeast(20)
	if _, err := service.SetPaused(ctx, true, adminapi.Mutation{Actor: "integration", Reason: "randomized pause"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := service.SetPaused(ctx, false, adminapi.Mutation{Actor: "integration", Reason: "randomized resume"}); err != nil {
		t.Fatal(err)
	}
	waitDeliveredAtLeast(len(planned))

	records := consumeTopicUntilEventIDs(t, e.brokers, topic, committed, 30*time.Second)
	type observed struct {
		offset   int64
		sequence int64
	}
	byKey := make(map[string][]observed, streams)
	seenIDs := make(map[string]struct{}, len(records))
	for _, record := range records {
		byKey[string(record.Key)] = append(byKey[string(record.Key)], observed{
			offset: record.Offset, sequence: recordSequence(t, record),
		})
		seenIDs[headerValue(record, broker.HeaderEventID)] = struct{}{}
	}
	for id := range committed {
		if _, ok := seenIDs[id]; !ok {
			t.Fatalf("randomized run lost committed event %s", id)
		}
	}
	for key, observations := range byKey {
		sort.Slice(observations, func(i, j int) bool { return observations[i].offset < observations[j].offset })
		var previous int64
		for _, item := range observations {
			if item.sequence < previous {
				t.Fatalf("randomized stream %s regressed from %d to %d", key, previous, item.sequence)
			}
			previous = item.sequence
		}
	}
	if len(byKey) != streams {
		t.Fatalf("randomized run observed %d/%d streams", len(byKey), streams)
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
