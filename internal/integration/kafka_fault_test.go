//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	mobyclient "github.com/moby/moby/client"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
	kafkapub "github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/relay"
)

func (e *env) setKafkaPaused(t *testing.T, paused bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if paused {
		if _, err := client.ContainerPause(ctx, e.kafka.GetContainerID(), mobyclient.ContainerPauseOptions{}); err != nil {
			t.Fatalf("pause Kafka: %v", err)
		}
		return
	}
	if _, err := client.ContainerUnpause(ctx, e.kafka.GetContainerID(), mobyclient.ContainerUnpauseOptions{}); err != nil {
		t.Fatalf("unpause Kafka: %v", err)
	}
	e.waitKafka(t)
}

func (e *env) stopKafka(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	timeout := 10 * time.Second
	if err := e.kafka.Stop(ctx, &timeout); err != nil {
		t.Fatalf("stop Kafka: %v", err)
	}
}

func (e *env) startKafka(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := e.kafka.Start(ctx); err != nil {
		t.Fatalf("start Kafka: %v", err)
	}
	e.waitKafka(t)
}

func (e *env) waitKafka(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = client.Ping(ctx)
			cancel()
			client.Close()
		}
		if err == nil {
			return
		}
		last = err
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("Kafka did not become ready: %v", last)
}

func (e *env) restoreKafka(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	state, err := e.kafka.State(ctx)
	if err != nil {
		t.Errorf("inspect Kafka during cleanup: %v", err)
		return
	}
	if state.Paused {
		e.setKafkaPaused(t, false)
		return
	}
	if !state.Running {
		e.startKafka(t)
	}
}

func TestKafkaPublisherBoundedAmbiguousRetryPreservesDistinctRecords(t *testing.T) {
	e := startEnv(t)
	t.Cleanup(func() { e.restoreKafka(t) })
	topic := topicName(t, e)
	const publishTimeout = time.Second
	publisher, err := kafkapub.NewPublisher(kafkapub.Config{
		Brokers: e.brokers, ClientID: "bounded-ambiguous", PublishTimeout: publishTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	warmup := broker.Message{Destination: topic, Key: []byte("warmup"), Payload: []byte("warmup")}
	if err := publisher.Publish(context.Background(), warmup); err != nil {
		t.Fatalf("establish healthy publisher: %v", err)
	}

	e.setKafkaPaused(t, true)
	first := broker.Message{Destination: topic, Key: []byte("A"), Payload: []byte("event-A")}
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	started := time.Now()
	err = publisher.Publish(ctx, first)
	elapsed := time.Since(started)
	cancel()
	if err == nil {
		t.Fatal("publish unexpectedly succeeded while Kafka responses were blocked")
	}
	if elapsed > publishTimeout+2*time.Second {
		t.Fatalf("publish exceeded bound: elapsed=%s timeout=%s error=%v", elapsed, publishTimeout, err)
	}
	if elapsed < publishTimeout/2 {
		t.Fatalf("publish failed before exercising the configured bound: elapsed=%s error=%v", elapsed, err)
	}

	e.setKafkaPaused(t, false)
	if err := publisher.Publish(context.Background(), first); err != nil {
		t.Fatalf("retry A after recovery: %v", err)
	}
	second := broker.Message{Destination: topic, Key: []byte("B"), Payload: []byte("event-B")}
	if err := publisher.Publish(context.Background(), second); err != nil {
		t.Fatalf("publish distinct B after recovery: %v", err)
	}

	counts := consumeValues(t, e.brokers, topic, map[string]struct{}{"event-A": {}, "event-B": {}}, 20*time.Second)
	if counts["event-A"] < 1 || counts["event-B"] < 1 {
		t.Fatalf("independent Kafka audit counts=%v", counts)
	}
}

func TestKafkaActualStopStartDrainsCommittedOutbox(t *testing.T) {
	e := startEnv(t)
	t.Cleanup(func() { e.restoreKafka(t) })
	topic := topicName(t, e)
	const (
		count   = 40
		streams = 4
	)
	committed := make(map[string]struct{}, count)
	for i := range count {
		key := fmt.Sprintf("restart-stream-%d", i%streams)
		sequence := int64(i/streams + 1)
		committed[enqueueOrdered(t, e, topic, key, sequence)] = struct{}{}
	}

	claimed := make(chan string, 1)
	releaseClaim := make(chan struct{})
	var once sync.Once
	hooks := relay.FailureHooks{AfterClaimCommit: func(ctx context.Context, event relay.Event) error {
		once.Do(func() { claimed <- event.ID.String() })
		select {
		case <-releaseClaim:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	cfg := orderedRelayConfig("actual-restart")
	cfg.Concurrency = 1
	cfg.BatchSize = 1
	cfg.PublishTimeout = time.Second
	cfg.LeaseDuration = 5 * time.Second
	cfg.OrderingLeaseDuration = 3 * time.Second
	cfg.OrderingSafetyMargin = 250 * time.Millisecond
	cfg.MaxAttempts = 100
	cfg.BaseDelay = 50 * time.Millisecond
	cfg.MaxDelay = 500 * time.Millisecond
	runRelay(t, e.newRelay(t, cfg, e.publisher(t), hooks))
	var claimedID string
	select {
	case claimedID = <-claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("relay did not claim committed work before Kafka stop")
	}
	e.stopKafka(t)
	close(releaseClaim)

	deadline := time.Now().Add(10 * time.Second)
	sawFailure := false
	for time.Now().Before(deadline) {
		event := e.getEvent(t, claimedID)
		if event.Attempts > 0 || event.LastError != "" {
			sawFailure = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawFailure {
		t.Fatal("relay did not record a publish attempt while Kafka was stopped")
	}
	e.startKafka(t)
	for id := range committed {
		e.waitStatus(t, id, "delivered", 60*time.Second)
	}
	records := consumeTopicUntilEventIDs(t, e.brokers, topic, committed, 30*time.Second)
	seen := make(map[string]struct{}, len(committed))
	for _, record := range records {
		seen[headerValue(record, broker.HeaderEventID)] = struct{}{}
	}
	if len(seen) != len(committed) {
		t.Fatalf("independent Kafka audit saw %d/%d committed IDs", len(seen), len(committed))
	}
	lastByStream := make(map[string]int64, streams)
	for _, record := range records {
		key := string(record.Key)
		sequence, err := strconv.ParseInt(headerValue(record, broker.HeaderSequence), 10, 64)
		if err != nil {
			t.Fatalf("sequence for %s: %v", key, err)
		}
		previous := lastByStream[key]
		if sequence < previous || sequence > previous+1 {
			t.Fatalf("ordered output regressed or skipped for %s: previous=%d observed=%d", key, previous, sequence)
		}
		lastByStream[key] = sequence
	}
	for stream := range streams {
		key := fmt.Sprintf("restart-stream-%d", stream)
		if lastByStream[key] != count/streams {
			t.Fatalf("stream %s ended at %d, want %d", key, lastByStream[key], count/streams)
		}
	}
	var pending, inflight, dead int
	if err := e.pool.QueryRow(context.Background(), `
SELECT COUNT(*) FILTER (WHERE status='pending'),
       COUNT(*) FILTER (WHERE status='inflight'),
       COUNT(*) FILTER (WHERE status='dead')
FROM emitlane.outbox_events WHERE destination=$1`, topic).Scan(&pending, &inflight, &dead); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || inflight != 0 || dead != 0 {
		t.Fatalf("queue did not drain after actual restart: pending=%d inflight=%d dead=%d", pending, inflight, dead)
	}
}

func TestOrderedStalePublisherReturnsBeforeTakeoverBoundary(t *testing.T) {
	e := startEnv(t)
	t.Cleanup(func() { e.restoreKafka(t) })
	topic := topicName(t, e)
	key := "order:bounded-handoff"
	firstID := enqueueOrdered(t, e, topic, key, 1)
	secondID := enqueueOrdered(t, e, topic, key, 2)
	const (
		oldOwner       = "bounded-old-owner"
		newOwner       = "bounded-new-owner"
		publishTimeout = time.Second
		safetyMargin   = 250 * time.Millisecond
	)

	registerOrderingRelay(t, e, oldOwner)
	if _, err := e.store.ReconcileOrderingPartitions(context.Background(), oldOwner, 3*time.Second, publishTimeout, 5*time.Second, safetyMargin); err != nil {
		t.Fatal(err)
	}
	claimed, err := e.store.ClaimOrdered(context.Background(), oldOwner, 1, 2*time.Second, publishTimeout+safetyMargin)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("old owner claim: count=%d err=%v", len(claimed), err)
	}
	event := claimed[0]
	if event.ID.String() != firstID {
		t.Fatalf("claimed %s, want sequence-one event %s", event.ID, firstID)
	}
	if _, err := e.store.BeginOrderedAttempt(context.Background(), event.ID, oldOwner, event.OrderingEpoch, 10, publishTimeout+safetyMargin); err != nil {
		t.Fatal(err)
	}

	oldPublisher, err := kafkapub.NewPublisher(kafkapub.Config{Brokers: e.brokers, ClientID: oldOwner, PublishTimeout: publishTimeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldPublisher.Close() })
	warmTopic := topicName(t, e)
	if err := oldPublisher.Publish(context.Background(), broker.Message{Destination: warmTopic, Payload: []byte("warm")}); err != nil {
		t.Fatalf("warm old publisher: %v", err)
	}
	e.setKafkaPaused(t, true)

	oldStarted := time.Now()
	oldReturned := make(chan struct {
		at  time.Time
		err error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
		defer cancel()
		err := oldPublisher.Publish(ctx, orderedMessage(event))
		oldReturned <- struct {
			at  time.Time
			err error
		}{time.Now(), err}
	}()

	if err := e.store.ReleaseOrderingPartitions(context.Background(), oldOwner, safetyMargin); err != nil {
		t.Fatal(err)
	}
	leaseLostAt := time.Now()
	if err := e.store.MarkRelayStopped(context.Background(), oldOwner); err != nil {
		t.Fatal(err)
	}
	registerOrderingRelay(t, e, newOwner)
	if _, err := e.store.ReconcileOrderingPartitions(context.Background(), newOwner, 3*time.Second, publishTimeout, 5*time.Second, safetyMargin); err != nil {
		t.Fatal(err)
	}
	var handoffNotBefore time.Time
	if err := e.pool.QueryRow(context.Background(), `SELECT handoff_not_before FROM emitlane.ordering_partitions WHERE partition_id=$1`, *event.OrderingPartition).Scan(&handoffNotBefore); err != nil {
		t.Fatal(err)
	}

	var oldResult struct {
		at  time.Time
		err error
	}
	select {
	case oldResult = <-oldReturned:
	case <-time.After(publishTimeout + 3*time.Second):
		t.Fatal("old ordered publisher remained live beyond its configured bound")
	}
	if oldResult.err == nil {
		t.Fatal("old ordered publish unexpectedly succeeded while Kafka was paused")
	}
	if oldResult.at.After(handoffNotBefore) {
		t.Fatalf("old publish returned after handoff boundary: return=%s handoff=%s", oldResult.at.Format(time.RFC3339Nano), handoffNotBefore.Format(time.RFC3339Nano))
	}
	e.setKafkaPaused(t, false)

	newInner, err := kafkapub.NewPublisher(kafkapub.Config{Brokers: e.brokers, ClientID: newOwner, PublishTimeout: publishTimeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newInner.Close() })
	newPublisher := &firstPublishRecorder{Publisher: newInner, first: make(chan time.Time, 1)}
	cfg := orderedRelayConfig(newOwner)
	cfg.PublishTimeout = publishTimeout
	cfg.OrderingLeaseDuration = 3 * time.Second
	cfg.OrderingSafetyMargin = safetyMargin
	cfg.LeaseDuration = 3 * time.Second
	cfg.MaxAttempts = 20
	runRelay(t, e.newRelay(t, cfg, newPublisher, relay.FailureHooks{}))
	e.waitStatus(t, firstID, "delivered", 30*time.Second)
	e.waitStatus(t, secondID, "delivered", 30*time.Second)

	var newStarted time.Time
	select {
	case newStarted = <-newPublisher.first:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement publisher did not record a publish")
	}
	if newStarted.Before(handoffNotBefore) {
		t.Fatalf("replacement published before handoff boundary: start=%s handoff=%s", newStarted.Format(time.RFC3339Nano), handoffNotBefore.Format(time.RFC3339Nano))
	}

	time.Sleep(500 * time.Millisecond)
	records := consumeTopicSnapshot(t, e.brokers, topic, 20*time.Second)
	var previous int64
	seen := map[int64]bool{}
	for _, record := range records {
		if string(record.Key) != key {
			continue
		}
		sequence, err := strconv.ParseInt(headerValue(record, broker.HeaderSequence), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if sequence < previous {
			t.Fatalf("forbidden stale ordering regression in Kafka: previous=%d observed=%d", previous, sequence)
		}
		previous = sequence
		seen[sequence] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("Kafka order missing sequence: seen=%v", seen)
	}
	t.Logf("stale-owner timing: old_start=%s old_return=%s lease_loss=%s handoff_not_before=%s replacement_first_publish=%s",
		oldStarted.Format(time.RFC3339Nano), oldResult.at.Format(time.RFC3339Nano), leaseLostAt.Format(time.RFC3339Nano), handoffNotBefore.Format(time.RFC3339Nano), newStarted.Format(time.RFC3339Nano))
}

type firstPublishRecorder struct {
	broker.Publisher
	once  sync.Once
	first chan time.Time
}

func (p *firstPublishRecorder) Publish(ctx context.Context, message broker.Message) error {
	p.once.Do(func() { p.first <- time.Now() })
	return p.Publisher.Publish(ctx, message)
}

func orderedMessage(event relay.Event) broker.Message {
	return broker.Message{
		Destination: event.Destination,
		Key:         []byte(event.OrderingKey),
		Payload:     event.Payload,
		Headers: map[string]string{
			broker.HeaderEventID:     event.ID.String(),
			broker.HeaderEventType:   event.Type,
			broker.HeaderOrderingKey: event.OrderingKey,
			broker.HeaderSequence:    strconv.FormatInt(event.OrderingSequence, 10),
			broker.HeaderPartition:   strconv.Itoa(int(*event.OrderingPartition)),
		},
	}
}

func consumeTopicSnapshot(t *testing.T, brokers []string, topic string, timeout time.Duration) []*kgo.Record {
	t.Helper()
	metadata, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ends, err := kadm.NewClient(metadata).ListEndOffsets(ctx, topic)
	metadata.Close()
	if err != nil {
		t.Fatal(err)
	}
	partitions := ends[topic]
	starts := make(map[int32]kgo.Offset, len(partitions))
	targets := make(map[int32]int64, len(partitions))
	remaining := 0
	for partition, end := range partitions {
		if end.Err != nil {
			t.Fatal(end.Err)
		}
		starts[partition] = kgo.NewOffset().AtStart()
		targets[partition] = end.Offset
		if end.Offset > 0 {
			remaining++
		}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: starts}), kgo.FetchMaxWait(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	completed := make(map[int32]bool)
	var records []*kgo.Record
	for remaining > 0 && ctx.Err() == nil {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 && ctx.Err() == nil {
			t.Fatal(errs[0])
		}
		fetches.EachPartition(func(fetch kgo.FetchTopicPartition) {
			if completed[fetch.Partition] {
				return
			}
			for _, record := range fetch.Records {
				if record.Offset >= targets[fetch.Partition] {
					continue
				}
				records = append(records, record)
				if record.Offset+1 >= targets[fetch.Partition] {
					completed[fetch.Partition] = true
					remaining--
				}
			}
		})
	}
	if remaining != 0 {
		t.Fatalf("Kafka snapshot audit timed out with %d partitions remaining", remaining)
	}
	return records
}

func consumeValues(t *testing.T, brokers []string, topic string, expected map[string]struct{}, timeout time.Duration) map[string]int {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	counts := make(map[string]int, len(expected))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 && ctx.Err() == nil {
			t.Fatalf("consume values: %v", errs[0])
		}
		for _, record := range fetches.Records() {
			value := string(record.Value)
			if _, ok := expected[value]; ok {
				counts[value]++
			}
		}
		complete := true
		for value := range expected {
			complete = complete && counts[value] > 0
		}
		if complete {
			return counts
		}
	}
	missing := make([]string, 0)
	for value := range expected {
		if counts[value] == 0 {
			missing = append(missing, value)
		}
	}
	sort.Strings(missing)
	t.Fatalf("missing Kafka values after timeout: %v", missing)
	return nil
}
