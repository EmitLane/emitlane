//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	managed "github.com/emitlane/emitlane/consumer"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This is a qualification assertion budget, not a production consumer setting.
// A broker restart can require coordinator discovery and another group join.
const qualificationRebalanceAssignmentTimeout = time.Minute

func validateQualificationRebalanceCapacity(partitions, workers int) error {
	// With more members than partitions, sticky balancing may legitimately keep
	// every partition on the incumbents. A joined peer can then remain idle.
	// This particular fault requires real work on the peer, so provision enough
	// partitions for both full worker pools rather than relaxing the assertion.
	if partitions < 2*workers {
		return fmt.Errorf("qualification positive-assignment fault needs at least %d partitions for %d incumbent and %d peer workers; configured %d", 2*workers, workers, workers, partitions)
	}
	return nil
}

func TestQualificationRebalanceCapacityRejectsIdlePeerAssumption(t *testing.T) {
	if err := validateQualificationRebalanceCapacity(8, 8); err == nil {
		t.Fatal("oversubscribed group accepted as a guaranteed positive peer-assignment test")
	}
	for _, pair := range [][2]int{{8, 4}, {16, 4}, {16, 8}} {
		if err := validateQualificationRebalanceCapacity(pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQualificationRebalanceCapacityAndOversubscribedHandoff(t *testing.T) {
	e := startEnv(t)
	for _, partitions := range []int{8, 16} {
		t.Run(fmt.Sprintf("partitions%d", partitions), func(t *testing.T) {
			topic, group, name := "rebalance-capacity-"+uuid.NewString(), "rebalance-capacity-group-"+uuid.NewString(), "rebalance-capacity-"+uuid.NewString()
			e.ensureTopicPartitions(t, topic, int32(partitions))
			logs := &qualificationRebalanceLogs{}
			logger := managed.WithLogger(slog.New(slog.NewTextHandler(logs, nil)))
			defer func() {
				if t.Failed() {
					t.Logf("bounded consumer diagnostics:\n%s", logs.snapshot())
				}
			}()
			store := newManagedInboxStore(t, e)
			handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
				_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id,amount) VALUES ($1,1)`, message.EventID.String())
				return err
			}
			config := qualificationConsumerConfig(name, "qualification-capacity-main")
			config.Concurrency = 8
			mainFactory := &qualificationCommitFactory{inner: newManagedFactory(t, e, topic, group)}
			main := launchManaged(t, e, config, store, mainFactory, handler, logger)
			defer main.stop(t)
			client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			admin := kadm.NewClient(client)
			waitGroup := func(memberCount int) kadm.DescribedGroup {
				ctx, cancel := context.WithTimeout(context.Background(), qualificationRebalanceAssignmentTimeout)
				defer cancel()
				for ctx.Err() == nil {
					groups, err := admin.DescribeGroups(ctx, group)
					g := groups[group]
					if err == nil && g.Err == nil && g.State == "Stable" && len(g.Members) == memberCount {
						seen := make(map[int32]bool)
						for _, m := range g.Members {
							assignment, ok := m.Assigned.AsConsumer()
							if !ok {
								t.Fatal("non-consumer assignment")
							}
							for _, topicAssignment := range assignment.Topics {
								if topicAssignment.Topic != topic {
									t.Fatal("unexpected assigned topic")
								}
								for _, partition := range topicAssignment.Partitions {
									if seen[partition] || partition < 0 || partition >= int32(partitions) {
										t.Fatal("duplicate or invalid partition ownership")
									}
									seen[partition] = true
								}
							}
						}
						if len(seen) == partitions {
							return g
						}
					}
					time.Sleep(50 * time.Millisecond)
				}
				t.Fatalf("group did not stabilize with %d members and complete partition ownership within %s", memberCount, qualificationRebalanceAssignmentTimeout)
				return kadm.DescribedGroup{}
			}
			waitGroup(8)
			config.InstanceID = "qualification-capacity-peer"
			peerFactory := &qualificationCommitFactory{inner: newManagedFactory(t, e, topic, group)}
			peer := launchManaged(t, e, config, store, peerFactory, handler, logger)
			defer peer.stop(t)
			joined := waitGroup(16)
			idle := 0
			for _, m := range joined.Members {
				assignment, _ := m.Assigned.AsConsumer()
				count := 0
				for _, topicAssignment := range assignment.Topics {
					count += len(topicAssignment.Partitions)
				}
				if count == 0 {
					idle++
				}
			}
			if partitions == 8 && idle != 8 || partitions == 16 && idle != 0 {
				t.Fatalf("partitions=%d idle members=%d", partitions, idle)
			}
			if partitions == 16 {
				ctx, cancel := context.WithTimeout(context.Background(), qualificationRebalanceAssignmentTimeout)
				err = waitQualificationAssignment(ctx, peerFactory)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
			ids := make([]string, 0, 2*partitions)
			produceAndWait := func(phase string) {
				for partition := range partitions {
					id := uuid.New()
					ids = append(ids, id.String())
					produceManagedRecordAtPartition(t, e, topic, int32(partition), id, []byte("capacity-handoff"))
				}
				ctx, cancel := context.WithTimeout(context.Background(), qualificationRebalanceAssignmentTimeout)
				defer cancel()
				for ctx.Err() == nil {
					var effects, processed int
					err := e.pool.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM public.business_payments WHERE order_id=ANY($1::text[])),(SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$2 AND event_id::text=ANY($1::text[]) AND status='processed')`, ids, name).Scan(&effects, &processed)
					if err != nil {
						t.Fatalf("%s progress query: %v", phase, err)
					}
					if effects == len(ids) && processed == len(ids) {
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
				diagCtx, stopDiag := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopDiag()
				var state []byte
				err := e.pool.QueryRow(diagCtx, `SELECT COALESCE(json_agg(json_build_object('event_id',event_id,'status',status,'attempts',attempts,'last_error',last_error,'lease_owner',lease_owner,'lease_until',lease_until)),'[]') FROM emitlane.inbox_events WHERE consumer=$1`, name).Scan(&state)
				t.Logf("%s Inbox states=%s query_error=%v", phase, state, err)
				kafka, kafkaErr := qualificationKafkaSnapshot(diagCtx, e.brokers, topic, group)
				t.Fatalf("%s did not process every protected effect: Kafka=%v error=%v", phase, kafka, kafkaErr)
			}
			produceAndWait("both pools active")
			main.stop(t)
			waitGroup(8)
			produceAndWait("peer after incumbent shutdown")
			for partition := range partitions {
				waitCommittedOffset(t, e, group, topic, int32(partition), 2, qualificationRebalanceAssignmentTimeout)
			}
			peer.stop(t)
			t.Logf("QUALIFICATION_CAPACITY_HANDOFF partitions=%d members_at_join=16 idle=%d protected_effects=%d final_lag=0", partitions, idle, len(ids))
		})
	}
}

// Consumer warnings must remain available on a failed assertion even when the
// shared fixture discards ordinary logs. Bound retained output across workers.
type qualificationRebalanceLogs struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (l *qualificationRebalanceLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if remaining := 64*1024 - l.buffer.Len(); remaining > 0 {
		_, _ = l.buffer.Write(p[:min(len(p), remaining)])
	}
	return len(p), nil
}

func (l *qualificationRebalanceLogs) snapshot() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.String()
}

func waitQualificationAssignment(ctx context.Context, factory *qualificationCommitFactory) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if factory.assignments.Load() > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func TestQualificationAssignmentWaitRequiresRealAssignment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	factory := &qualificationCommitFactory{}
	if err := waitQualificationAssignment(ctx, factory); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unassigned consumer: %v", err)
	}
	// An assignment appearing after cancellation must not turn a timeout into PASS.
	factory.assignments.Store(1)
	if err := waitQualificationAssignment(ctx, factory); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("assignment after deadline: %v", err)
	}
}

func TestQualificationRebalanceAfterKafkaRestartWithDelayedPeer(t *testing.T) {
	e := startEnv(t)
	topic, group := "rebalance-"+uuid.NewString(), "rebalance-group-"+uuid.NewString()
	e.ensureTopicPartitions(t, topic, 8)
	store := newManagedInboxStore(t, e)
	handler := func(context.Context, pgx.Tx, managed.Message) error { return nil }
	config := qualificationConsumerConfig("qualification-rebalance", "qualification-rebalance-main")
	config.Concurrency = 1
	mainFactory := &qualificationCommitFactory{inner: newManagedFactory(t, e, topic, group)}
	main := launchManaged(t, e, config, store, mainFactory, handler)
	defer main.stop(t)
	initialCtx, cancelInitial := context.WithTimeout(context.Background(), qualificationRebalanceAssignmentTimeout)
	err := waitQualificationAssignment(initialCtx, mainFactory)
	cancelInitial()
	if err != nil {
		t.Fatalf("initial consumer assignment: %v", err)
	}
	e.stopKafka(t)
	e.startKafka(t)
	// Delay the real source creation beyond the old 12-second assertion. Only the
	// actual Kafka Assigned callback increments the qualification proof counter.
	const delay = 15 * time.Second
	peerFactory := &qualificationCommitFactory{inner: &qualificationDelayedSourceFactory{
		inner: newManagedFactory(t, e, topic, group), delay: delay,
	}}
	config.InstanceID = "qualification-rebalance-peer"
	started := time.Now()
	peer := launchManaged(t, e, config, store, peerFactory, handler)
	defer peer.stop(t)
	ctx, cancel := context.WithTimeout(context.Background(), qualificationRebalanceAssignmentTimeout)
	err = waitQualificationAssignment(ctx, peerFactory)
	cancel()
	if err != nil {
		t.Fatalf("real delayed peer assignment after restart: %v", err)
	}
	if elapsed := time.Since(started); elapsed < delay {
		t.Fatalf("assignment was accepted before the real delayed source joined: %s", elapsed)
	}
	proof, err := json.Marshal(map[string]any{"kind": "rebalance_regression", "delay": delay.String(),
		"timeout": qualificationRebalanceAssignmentTimeout.String(), "wait_seconds": time.Since(started).Seconds(),
		"assignments": peerFactory.assignments.Load(), "kafka_restarts": 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("QUALIFICATION_REBALANCE_REGRESSION %s", proof)
}

type qualificationDelayedSourceFactory struct {
	inner managed.SourceFactory
	delay time.Duration
}

func (f *qualificationDelayedSourceFactory) NewSource(workerID string, listener managed.RebalanceListener) (managed.Source, error) {
	time.Sleep(f.delay)
	return f.inner.NewSource(workerID, listener)
}
