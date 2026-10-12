//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	managed "github.com/emitlane/emitlane/consumer"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// This is a qualification assertion budget, not a production consumer setting.
// A broker restart can require coordinator discovery and another group join.
const qualificationRebalanceAssignmentTimeout = time.Minute

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
