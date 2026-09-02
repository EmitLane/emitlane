package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
	"github.com/emitlane/emitlane/relay"
)

type partitionRow struct {
	id               int16
	leaseOwner       *string
	leaseUntil       *time.Time
	epoch            int64
	handoffNotBefore *time.Time
	publishTimeoutMS *int
}

// ReconcileOrderingPartitions calculates desired ownership from active Relay
// presence and atomically reconciles the caller's authoritative leases.
func (s *Store) ReconcileOrderingPartitions(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
	publishTimeout time.Duration,
	presenceStaleAfter time.Duration,
	safetyMargin time.Duration,
) ([]relay.OrderingPartition, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconcile ordering partitions begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now, members, err := activeOrderingMembers(ctx, tx, presenceStaleAfter)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
SELECT partition_id, lease_owner, lease_until, epoch,
       handoff_not_before, publish_timeout_ms
FROM emitlane.ordering_partitions
ORDER BY partition_id
FOR UPDATE`)
	if err != nil {
		return nil, fmt.Errorf("lock ordering partitions: %w", err)
	}
	var locked []partitionRow
	for rows.Next() {
		var row partitionRow
		if err := rows.Scan(&row.id, &row.leaseOwner, &row.leaseUntil, &row.epoch,
			&row.handoffNotBefore, &row.publishTimeoutMS); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan ordering partition: %w", err)
		}
		locked = append(locked, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("list ordering partitions: %w", err)
	}
	rows.Close()
	if len(locked) != internalordering.PartitionCount {
		return nil, fmt.Errorf("ordering partition seed has %d rows, want %d", len(locked), internalordering.PartitionCount)
	}

	leaseUntil := now.Add(leaseDuration)
	publishMS := int(intervalMS(publishTimeout))
	for i := range locked {
		row := &locked[i]
		desired := internalordering.DesiredOwner(row.id, members)
		actual := deref(row.leaseOwner)
		valid := actual != "" && row.leaseUntil != nil && row.leaseUntil.After(now)

		switch {
		case actual == owner && desired != owner:
			barrier := releaseBarrier(now, row.handoffNotBefore, row.publishTimeoutMS, safetyMargin)
			if _, err := tx.Exec(ctx, `
UPDATE emitlane.ordering_partitions
SET lease_owner=NULL, lease_until=NULL, epoch=epoch+1,
    handoff_not_before=$2, updated_at=$3
WHERE partition_id=$1 AND lease_owner=$4`, row.id, barrier, now, owner); err != nil {
				return nil, fmt.Errorf("release ordering partition %d: %w", row.id, err)
			}
			row.leaseOwner, row.leaseUntil = nil, nil
			row.epoch++
			row.handoffNotBefore = &barrier

		case desired == owner && actual == owner && valid:
			if _, err := tx.Exec(ctx, `
UPDATE emitlane.ordering_partitions
SET lease_until=$2, publish_timeout_ms=$3, updated_at=$4
WHERE partition_id=$1 AND lease_owner=$5 AND epoch=$6`,
				row.id, leaseUntil, publishMS, now, owner, row.epoch); err != nil {
				return nil, fmt.Errorf("renew ordering partition %d: %w", row.id, err)
			}
			row.leaseUntil = &leaseUntil
			row.publishTimeoutMS = &publishMS

		case desired == owner && (actual == "" || !valid):
			barrier := row.handoffNotBefore
			if actual != "" {
				takeover := releaseBarrier(now, barrier, row.publishTimeoutMS, safetyMargin)
				barrier = &takeover
			}
			if _, err := tx.Exec(ctx, `
UPDATE emitlane.ordering_partitions
SET lease_owner=$2, lease_until=$3, epoch=epoch+1,
    handoff_not_before=$4, publish_timeout_ms=$5, updated_at=$6
WHERE partition_id=$1
  AND (lease_owner IS NULL OR lease_until <= $6)`,
				row.id, owner, leaseUntil, barrier, publishMS, now); err != nil {
				return nil, fmt.Errorf("acquire ordering partition %d: %w", row.id, err)
			}
			ownerCopy := owner
			row.leaseOwner = &ownerCopy
			row.leaseUntil = &leaseUntil
			row.epoch++
			row.handoffNotBefore = barrier
			row.publishTimeoutMS = &publishMS
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("reconcile ordering partitions commit: %w", err)
	}
	result := make([]relay.OrderingPartition, 0, len(locked))
	for _, row := range locked {
		result = append(result, relay.OrderingPartition{
			PartitionID:      row.id,
			DesiredOwner:     internalordering.DesiredOwner(row.id, members),
			LeaseOwner:       deref(row.leaseOwner),
			LeaseUntil:       row.leaseUntil,
			Epoch:            row.epoch,
			HandoffNotBefore: row.handoffNotBefore,
		})
	}
	return result, nil
}

func activeOrderingMembers(ctx context.Context, tx pgx.Tx, staleAfter time.Duration) (time.Time, []string, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&now); err != nil {
		return time.Time{}, nil, fmt.Errorf("read ordering database time: %w", err)
	}
	rows, err := tx.Query(ctx, `
SELECT instance_id
FROM emitlane.relay_instances
WHERE ordering_capable = TRUE
  AND stopped_at IS NULL
  AND last_heartbeat_at >= $1::timestamptz - ($2 * INTERVAL '1 millisecond')
ORDER BY instance_id`, now, intervalMS(staleAfter))
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("list active ordering relays: %w", err)
	}
	defer rows.Close()
	var members []string
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			return time.Time{}, nil, fmt.Errorf("scan active ordering relay: %w", err)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, nil, fmt.Errorf("list active ordering relays: %w", err)
	}
	sort.Strings(members)
	return now, members, nil
}

func releaseBarrier(now time.Time, existing *time.Time, publishTimeoutMS *int, safetyMargin time.Duration) time.Time {
	publishWindow := time.Duration(0)
	if publishTimeoutMS != nil {
		publishWindow = time.Duration(*publishTimeoutMS) * time.Millisecond
	}
	barrier := now.Add(publishWindow + safetyMargin)
	if existing != nil && existing.After(barrier) {
		barrier = *existing
	}
	return barrier
}

// ReleaseOrderingPartitions gracefully relinquishes all partitions held by an
// owner and establishes a handoff barrier from the persisted publish window.
func (s *Store) ReleaseOrderingPartitions(ctx context.Context, owner string, safetyMargin time.Duration) error {
	const query = `
UPDATE emitlane.ordering_partitions
SET lease_owner=NULL,
    lease_until=NULL,
    epoch=epoch+1,
    handoff_not_before=GREATEST(
        COALESCE(handoff_not_before, '-infinity'::timestamptz),
        NOW() + ((COALESCE(publish_timeout_ms, 0) + $2) * INTERVAL '1 millisecond')
    ),
    updated_at=NOW()
WHERE lease_owner=$1`
	if _, err := s.pool.Exec(ctx, query, owner, intervalMS(safetyMargin)); err != nil {
		return fmt.Errorf("release ordering partitions for %s: %w", owner, err)
	}
	return nil
}
