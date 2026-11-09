//go:build integration

package integration_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func TestStatsPollingPreservesExactReadsAndPublicStreamDetails(t *testing.T) {
	e, owner := orderedClaimFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ready := enqueueOrdered(t, e, "stats-heads", "ready", 1)
	enqueueOrdered(t, e, "stats-heads", "ready", 2)
	inflight := enqueueOrdered(t, e, "stats-heads", "inflight", 1)
	dead := enqueueOrdered(t, e, "stats-heads", "dead", 1)
	retry := enqueueOrdered(t, e, "stats-heads", "retry", 1)
	enqueueOrdered(t, e, "stats-heads", "gap", 2)
	empty := enqueueOrdered(t, e, "stats-heads", "empty", 1)
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET status='inflight',lease_owner=$2,lease_until=NOW()+INTERVAL '1 minute' WHERE id=$1`, mustUUID(t, inflight), owner); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET status='dead' WHERE id=$1`, mustUUID(t, dead)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET available_at=NOW()+INTERVAL '1 minute' WHERE id=$1`, mustUUID(t, retry)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `DELETE FROM emitlane.outbox_events WHERE id=$1`, mustUUID(t, empty)); err != nil {
		t.Fatal(err)
	}
	stats, captured, err := e.store.CachedStatsSnapshotWithPresence(ctx, time.Second)
	if err != nil || captured.IsZero() || stats.Pending != 4 || stats.Inflight != 1 || stats.Dead != 1 || stats.OrderedStreams != 6 || stats.BlockedOrderedStreams != 3 || stats.GapStreams != 1 || stats.DeadBlockedStreams != 1 {
		t.Fatalf("mixed-state snapshot=%+v at=%s err=%v", stats, captured, err)
	}
	detail, err := e.store.InspectOrderingStream(ctx, "stats-heads", "ready")
	if err != nil || detail.LowestFutureSequence == nil || *detail.LowestFutureSequence != 2 || detail.GapSize != 1 {
		t.Fatalf("aggregate optimization removed public future-sequence details: %+v %v", detail, err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.outbox_events SET status='delivered',delivered_at=NOW() WHERE id=$1`, mustUUID(t, ready)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE emitlane.runtime_control SET paused=TRUE WHERE singleton=TRUE;
UPDATE emitlane.relay_instances SET last_heartbeat_at=NOW()-INTERVAL '1 minute';
UPDATE emitlane.ordering_partitions SET handoff_not_before=NOW()+INTERVAL '1 minute' WHERE partition_id=0`); err != nil {
		t.Fatal(err)
	}
	fresh, err := e.store.StatsSnapshotWithPresence(ctx, time.Second)
	if err != nil || fresh.Pending != 3 {
		t.Fatalf("exact method was cached: %+v %v", fresh, err)
	}
	cached, at, err := e.store.CachedStatsSnapshotWithPresence(ctx, time.Second)
	if err != nil || cached.Pending != 4 || !at.Equal(captured) || !cached.Paused || cached.RelaysActive != 0 || cached.RelaysStale != 1 || cached.HandoffPartitions != 1 || cached.OwnedPartitions != 63 {
		t.Fatalf("cached data lost its capture time or live controls were cached: %+v at=%s err=%v", cached, at, err)
	}
}

func TestCachedStatsUseOneDatabaseSnapshotAcrossAggregations(t *testing.T) {
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	gate := &statsReadGate{reached: make(chan struct{}), resume: make(chan struct{})}
	config, err := pgxpool.ParseConfig(e.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = gate
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	type observation struct {
		pending, streams, gaps int64
		err                    error
	}
	result := make(chan observation, 1)
	go func() {
		stats, _, err := store.CachedStatsSnapshotWithPresence(ctx, time.Second)
		result <- observation{stats.Pending, stats.OrderedStreams, stats.GapStreams, err}
	}()
	var release sync.Once
	resume := func() { release.Do(func() { close(gate.resume) }) }
	completed := false
	defer func() {
		resume()
		cancel()
		if !completed {
			<-result
		}
	}()
	select {
	case <-gate.reached:
	case <-ctx.Done():
		t.Fatal("queue aggregate never reached the controlled read boundary")
	}
	// The queue aggregate has read the empty database. Commit a new stream
	// and pending gap before letting the ordering aggregate run.
	enqueueOrdered(t, e, "stats-snapshot", "new-stream", 2)
	resume()
	got := <-result
	completed = true
	if got.err != nil || got.pending != 0 || got.streams != 0 || got.gaps != 0 {
		t.Fatalf("counts combined different database snapshots: %+v", got)
	}
	fresh, err := store.StatsSnapshotWithPresence(ctx, time.Second)
	if err != nil || fresh.Pending != 1 || fresh.OrderedStreams != 1 || fresh.GapStreams != 1 {
		t.Fatalf("explicit read did not observe the concurrent commit: %+v %v", fresh, err)
	}
}

type statsReadGate struct {
	reached, resume chan struct{}
	once            sync.Once
}

type statsReadGateKey struct{}

func (*statsReadGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, statsReadGateKey{}, strings.Contains(data.SQL, "WITH queue AS"))
}

func (g *statsReadGate) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if match, _ := ctx.Value(statsReadGateKey{}).(bool); match {
		g.once.Do(func() {
			close(g.reached)
			select {
			case <-g.resume:
			case <-ctx.Done():
			}
		})
	}
}
