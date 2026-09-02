//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
	"github.com/emitlane/emitlane/relay"
)

const (
	testPartitionLease = 500 * time.Millisecond
	testPublishWindow  = 100 * time.Millisecond
	testOrderingSafety = 50 * time.Millisecond
	testPresenceStale  = 5 * time.Second
)

func registerOrderingRelay(t *testing.T, e *env, id string) {
	t.Helper()
	if err := e.store.RegisterRelay(context.Background(), relay.RelayPresence{
		InstanceID: id, Hostname: "integration", Version: "v0.3-test",
		StartedAt: time.Now(), OrderingCapable: true,
	}); err != nil {
		t.Fatal(err)
	}
}

func reconcileOrdering(t *testing.T, e *env, id string) []relay.OrderingPartition {
	t.Helper()
	partitions, err := e.store.ReconcileOrderingPartitions(
		context.Background(), id, testPartitionLease, testPublishWindow,
		testPresenceStale, testOrderingSafety,
	)
	if err != nil {
		t.Fatal(err)
	}
	return partitions
}

func TestOrderingPartitionOwnershipRaceHasOneValidOwner(t *testing.T) {
	e := startEnv(t)
	owners := []string{"partition-a", "partition-b", "partition-c"}
	for _, owner := range owners {
		registerOrderingRelay(t, e, owner)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, owner := range owners {
		owner := owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = e.store.ReconcileOrderingPartitions(
				context.Background(), owner, testPartitionLease, testPublishWindow,
				testPresenceStale, testOrderingSafety,
			)
		}()
	}
	close(start)
	wg.Wait()
	// A second deterministic pass lets desired owners release/acquire rows that
	// were validly held during the first serialized reconciliation.
	for range 2 {
		for _, owner := range owners {
			reconcileOrdering(t, e, owner)
		}
	}

	rows, err := e.pool.Query(context.Background(), `
SELECT partition_id, lease_owner, epoch
FROM emitlane.ordering_partitions ORDER BY partition_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	countByOwner := make(map[string]int)
	count := 0
	for rows.Next() {
		var partition int16
		var owner string
		var epoch int64
		if err := rows.Scan(&partition, &owner, &epoch); err != nil {
			t.Fatal(err)
		}
		want := internalordering.DesiredOwner(partition, owners)
		if owner != want || epoch < 1 {
			t.Fatalf("partition %d owner=%q want=%q epoch=%d", partition, owner, want, epoch)
		}
		countByOwner[owner]++
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != internalordering.PartitionCount || len(countByOwner) != len(owners) {
		t.Fatalf("partition count=%d distribution=%v", count, countByOwner)
	}
}

func TestOrderingPartitionGracefulReleaseAndHandoff(t *testing.T) {
	e := startEnv(t)
	registerOrderingRelay(t, e, "handoff-a")
	reconcileOrdering(t, e, "handoff-a")

	var beforeEpoch int64
	if err := e.pool.QueryRow(context.Background(), `
SELECT epoch FROM emitlane.ordering_partitions WHERE partition_id=0`).Scan(&beforeEpoch); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ReleaseOrderingPartitions(context.Background(), "handoff-a", testOrderingSafety); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkRelayStopped(context.Background(), "handoff-a"); err != nil {
		t.Fatal(err)
	}
	registerOrderingRelay(t, e, "handoff-b")
	reconcileOrdering(t, e, "handoff-b")

	var owner string
	var epoch int64
	var handoff time.Time
	var handoffPassed bool
	if err := e.pool.QueryRow(context.Background(), `
SELECT lease_owner, epoch, handoff_not_before, handoff_not_before <= NOW()
FROM emitlane.ordering_partitions WHERE partition_id=0`).Scan(&owner, &epoch, &handoff, &handoffPassed); err != nil {
		t.Fatal(err)
	}
	if owner != "handoff-b" || epoch < beforeEpoch+2 || handoffPassed {
		t.Fatalf("owner=%q epoch=%d before=%d handoff=%s passed=%t", owner, epoch, beforeEpoch, handoff, handoffPassed)
	}
}

func TestOrderingPartitionCrashTakeoverPersistsPriorPublishWindow(t *testing.T) {
	e := startEnv(t)
	registerOrderingRelay(t, e, "crash-owner")
	reconcileOrdering(t, e, "crash-owner")
	if err := e.store.MarkRelayStopped(context.Background(), "crash-owner"); err != nil {
		t.Fatal(err)
	}
	registerOrderingRelay(t, e, "takeover-owner")
	time.Sleep(testPartitionLease + 100*time.Millisecond)
	reconcileOrdering(t, e, "takeover-owner")

	var owner string
	var handoffDelayMS float64
	if err := e.pool.QueryRow(context.Background(), `
SELECT lease_owner,
       EXTRACT(EPOCH FROM (handoff_not_before - updated_at)) * 1000
FROM emitlane.ordering_partitions WHERE partition_id=0`).Scan(&owner, &handoffDelayMS); err != nil {
		t.Fatal(err)
	}
	minimum := float64((testPublishWindow + testOrderingSafety) / time.Millisecond)
	if owner != "takeover-owner" || handoffDelayMS < minimum-2 {
		t.Fatalf("owner=%q handoff delay=%.2fms want >= %.2fms", owner, handoffDelayMS, minimum)
	}
}
