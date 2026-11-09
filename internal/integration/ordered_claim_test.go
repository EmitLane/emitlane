//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Use explicit ownership to isolate claim predicates from membership timing.
func orderedClaimFixture(t *testing.T) (*env, string) {
	t.Helper()
	e := startEnv(t)
	owner := "head-claim-relay"
	registerOrderingRelay(t, e, owner)
	if _, err := e.pool.Exec(context.Background(), `
UPDATE emitlane.ordering_partitions
SET lease_owner=$1, lease_until=NOW()+INTERVAL '1 minute', epoch=7,
    handoff_not_before=NULL, publish_timeout_ms=100`, owner); err != nil {
		t.Fatal(err)
	}
	return e, owner
}

func TestOrderedClaimHeadEligibility(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup string
		owner bool
		want  bool
	}{
		{name: "due pending", want: true},
		{name: "expired inflight", owner: true, want: true, setup: `
UPDATE emitlane.outbox_events SET status='inflight',lease_owner=$2,
lease_until=NOW()-INTERVAL '1 second' WHERE id=$1`},
		{name: "active inflight", owner: true, setup: `
UPDATE emitlane.outbox_events SET status='inflight',lease_owner=$2,
lease_until=NOW()+INTERVAL '1 minute' WHERE id=$1`},
		{name: "retry wait", setup: `
UPDATE emitlane.outbox_events SET available_at=NOW()+INTERVAL '1 minute' WHERE id=$1`},
		{name: "dead head", setup: `
UPDATE emitlane.outbox_events SET status='dead' WHERE id=$1`},
		{name: "missing head", setup: `
DELETE FROM emitlane.outbox_events WHERE id=$1`},
		{name: "other owner", setup: `
UPDATE emitlane.ordering_partitions SET lease_owner='other-relay'
WHERE partition_id=(SELECT ordering_partition FROM emitlane.outbox_events WHERE id=$1)`},
		{name: "expired partition", setup: `
UPDATE emitlane.ordering_partitions SET lease_until=NOW()-INTERVAL '1 second'
WHERE partition_id=(SELECT ordering_partition FROM emitlane.outbox_events WHERE id=$1)`},
		{name: "insufficient partition lease", setup: `
UPDATE emitlane.ordering_partitions SET lease_until=NOW()+INTERVAL '100 milliseconds'
WHERE partition_id=(SELECT ordering_partition FROM emitlane.outbox_events WHERE id=$1)`},
		{name: "handoff barrier", setup: `
UPDATE emitlane.ordering_partitions SET handoff_not_before=NOW()+INTERVAL '1 minute'
WHERE partition_id=(SELECT ordering_partition FROM emitlane.outbox_events WHERE id=$1)`},
		{name: "paused", setup: `
UPDATE emitlane.runtime_control SET paused=TRUE WHERE singleton=TRUE AND $1::uuid IS NOT NULL`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, owner := orderedClaimFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			head := enqueueOrdered(t, e, "head-eligibility", "stream", 1)
			future := enqueueOrdered(t, e, "head-eligibility", "stream", 2)
			if tc.setup != "" {
				args := []any{mustUUID(t, head)}
				if tc.owner {
					args = append(args, owner)
				}
				if _, err := e.pool.Exec(ctx, tc.setup, args...); err != nil {
					t.Fatal(err)
				}
			}
			before := time.Now()
			claimed, err := e.store.ClaimOrdered(ctx, owner, 8, 2*time.Second, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.want {
				want = 1
			}
			if len(claimed) != want {
				t.Fatalf("claimed %d events, want %d: %+v", len(claimed), want, claimed)
			}
			if tc.want {
				event := claimed[0]
				if event.ID.String() != head || event.OrderingSequence != 1 || event.OrderingEpoch != 7 ||
					event.LeaseOwner != owner || event.Status != "inflight" || event.LeaseUntil == nil || !event.LeaseUntil.After(before) {
					t.Fatalf("head or lease/epoch changed: %+v", event)
				}
			}
			if event := e.getEvent(t, future); event.Status != "pending" || event.Attempts != 0 {
				t.Fatalf("future sequence bypassed head: %+v", event)
			}
			var next int64
			if err := e.pool.QueryRow(ctx, `SELECT next_sequence FROM emitlane.ordering_streams
WHERE destination='head-eligibility' AND ordering_key='stream'`).Scan(&next); err != nil || next != 1 {
				t.Fatalf("claim advanced cursor: next=%d err=%v", next, err)
			}
		})
	}
}

func TestOrderedClaimSkipsLockedHeadsBeforeBatchLimit(t *testing.T) {
	e, owner := orderedClaimFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	locked := mustUUID(t, enqueueOrdered(t, e, "head-batch", "locked", 1))
	var ready []uuid.UUID
	for i := range 3 {
		key := fmt.Sprintf("ready-%d", i)
		ready = append(ready, mustUUID(t, enqueueOrdered(t, e, "head-batch", key, 1)))
		enqueueOrdered(t, e, "head-batch", key, 2)
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].String() < ready[j].String() })
	// All heads are due. The locked one is oldest; the next two tie on both
	// timestamps so the ID decides which is picked first with a one-row batch.
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.outbox_events SET available_at=NOW()-INTERVAL '2 minutes',created_at=NOW()-INTERVAL '2 minutes';
UPDATE emitlane.outbox_events SET available_at=NOW()-INTERVAL '3 minutes'
WHERE ordering_key='locked'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `
UPDATE emitlane.outbox_events SET created_at=created_at+INTERVAL '1 second' WHERE id=$1`, ready[2]); err != nil {
		t.Fatal(err)
	}
	locker, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	var id uuid.UUID
	if err := locker.QueryRow(ctx, `SELECT id FROM emitlane.outbox_events WHERE id=$1 FOR UPDATE`, locked).Scan(&id); err != nil {
		t.Fatal(err)
	}
	for i, limit := range []int{1, 2} {
		claimed, err := e.store.ClaimOrdered(ctx, owner, limit, 2*time.Second, time.Second)
		if err != nil || len(claimed) != limit {
			t.Fatalf("claim %d: got %d/%d events: %v", i, len(claimed), limit, err)
		}
		got := make(map[uuid.UUID]bool)
		for _, event := range claimed {
			got[event.ID] = true
		}
		if i == 0 && !got[ready[0]] || i == 1 && (!got[ready[1]] || !got[ready[2]]) {
			t.Fatalf("claim %d picked %v, want ready heads in timestamp/ID order %v", i, got, ready)
		}
	}
	claimed, err := e.store.ClaimOrdered(ctx, owner, 8, 2*time.Second, time.Second)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("claimed locked head, active lease or future sequence: %+v err=%v", claimed, err)
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err = e.store.ClaimOrdered(ctx, owner, 8, 2*time.Second, time.Second)
	if err != nil || len(claimed) != 1 || claimed[0].ID != locked {
		t.Fatalf("unlocked head not recoverable: %+v err=%v", claimed, err)
	}
}
