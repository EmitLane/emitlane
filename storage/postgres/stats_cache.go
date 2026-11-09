package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/emitlane/emitlane/relay"
)

// This is a reuse window, not a query or recovery timeout. Explicit snapshot
// calls bypass it; only automatic Relay polling opts into cached counts.
const statsSnapshotReuse = 30 * time.Second

type statsQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type statsSnapshotCache struct {
	mu         sync.Mutex
	loading    chan struct{}
	valid      bool
	value      relay.Stats
	capturedAt time.Time
	expiresAt  time.Time
}

func (c *statsSnapshotCache) snapshot(
	ctx context.Context,
	now func() time.Time,
	load func(context.Context) (relay.Stats, error),
) (relay.Stats, time.Time, error) {
	for {
		if err := ctx.Err(); err != nil {
			return relay.Stats{}, time.Time{}, err
		}
		c.mu.Lock()
		if c.valid && now().Before(c.expiresAt) {
			value, capturedAt := c.value, c.capturedAt
			c.mu.Unlock()
			return value, capturedAt, nil
		}
		if c.loading != nil {
			ready := c.loading
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return relay.Stats{}, time.Time{}, ctx.Err()
			case <-ready:
				continue
			}
		}
		ready := make(chan struct{})
		c.loading = ready
		capturedAt := now()
		c.mu.Unlock()
		value, err := load(ctx)
		c.mu.Lock()
		if err == nil {
			c.value, c.capturedAt = value, capturedAt
			c.expiresAt = capturedAt.Add(statsSnapshotReuse)
			c.valid = true
		}
		c.loading = nil
		close(ready)
		c.mu.Unlock()
		if err != nil {
			return relay.Stats{}, time.Time{}, err
		}
		return value, capturedAt, nil
	}
}

// CachedStatsSnapshotWithPresence reuses exact captured queue and stream
// counts for up to 30 seconds per Store. It never substitutes estimates.
// Control, presence classification and partition ownership are read live.
// Neither cache contents nor statistics are an audit/completion predicate.
func (s *Store) CachedStatsSnapshotWithPresence(ctx context.Context, staleAfter time.Duration) (relay.Stats, time.Time, error) {
	stats, capturedAt, err := s.statsCache.snapshot(ctx, time.Now, func(ctx context.Context) (relay.Stats, error) {
		// Queue and stream counts must describe one database snapshot even
		// when writers commit between the two aggregation statements.
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return relay.Stats{}, err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		stats, err := readStatsSnapshotFrom(ctx, tx, staleAfter)
		if err != nil {
			return relay.Stats{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return relay.Stats{}, err
		}
		return stats, nil
	})
	if err != nil {
		return relay.Stats{}, time.Time{}, fmt.Errorf("cached stats snapshot: %w", err)
	}
	// All relations in this poll are small control/membership tables. Caller
	// thresholds are applied afresh, so the cache is independent of staleAfter.
	err = s.pool.QueryRow(ctx, `
SELECT control.paused,
 (SELECT COUNT(*) FROM emitlane.relay_instances WHERE stopped_at IS NULL
  AND last_heartbeat_at >= NOW()-($1*INTERVAL '1 millisecond')),
 (SELECT COUNT(*) FROM emitlane.relay_instances WHERE stopped_at IS NULL
  AND last_heartbeat_at < NOW()-($1*INTERVAL '1 millisecond')),
 (SELECT COUNT(*) FROM emitlane.ordering_partitions WHERE lease_owner IS NOT NULL
  AND lease_until>NOW() AND COALESCE(handoff_not_before,'-infinity'::timestamptz)<=NOW()),
 (SELECT COUNT(*) FROM emitlane.ordering_partitions WHERE lease_owner IS NOT NULL
  AND lease_until>NOW() AND handoff_not_before>NOW())
FROM emitlane.runtime_control AS control WHERE singleton=TRUE`, intervalMS(staleAfter)).Scan(
		&stats.Paused, &stats.RelaysActive, &stats.RelaysStale, &stats.OwnedPartitions, &stats.HandoffPartitions,
	)
	if err != nil {
		return relay.Stats{}, time.Time{}, fmt.Errorf("poll stats controls: %w", err)
	}
	return stats, capturedAt, nil
}
