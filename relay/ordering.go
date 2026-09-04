package relay

import (
	"context"
	"time"
)

// OrderingPartition is one authoritative virtual-partition lease snapshot.
type OrderingPartition struct {
	PartitionID      int16
	DesiredOwner     string
	LeaseOwner       string
	LeaseUntil       *time.Time
	Epoch            int64
	HandoffNotBefore *time.Time
}

// OrderingPartitionStore is an optional schema-v3 capability. Its PostgreSQL
// implementation serializes reconciliation and uses database time for leases.
type OrderingPartitionStore interface {
	ReconcileOrderingPartitions(
		ctx context.Context,
		owner string,
		leaseDuration time.Duration,
		publishTimeout time.Duration,
		presenceStaleAfter time.Duration,
		safetyMargin time.Duration,
	) ([]OrderingPartition, error)
	ReleaseOrderingPartitions(ctx context.Context, owner string, safetyMargin time.Duration) error
}

func (r *Relay) orderingLoop(ctx context.Context, store OrderingPartitionStore) {
	r.reconcileOrdering(ctx, store)
	ticker := time.NewTicker(r.cfg.OrderingRebalanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileOrdering(ctx, store)
		}
	}
}

func (r *Relay) reconcileOrdering(ctx context.Context, store OrderingPartitionStore) {
	partitions, err := store.ReconcileOrderingPartitions(
		ctx,
		r.cfg.InstanceID,
		r.cfg.OrderingLeaseDuration,
		r.cfg.PublishTimeout,
		r.cfg.PresenceStaleAfter,
		r.cfg.OrderingSafetyMargin,
	)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("ordered partition reconciliation failed",
				"error", err,
				"relay_instance", r.cfg.InstanceID,
			)
		}
		return
	}
	r.orderingMu.Lock()
	previous := make(map[int16]OrderingPartition, len(r.orderingPartitions))
	for _, partition := range r.orderingPartitions {
		previous[partition.PartitionID] = partition
	}
	acquisitions := 0
	rebalanced := false
	for _, partition := range partitions {
		prior, existed := previous[partition.PartitionID]
		if partition.LeaseOwner == r.cfg.InstanceID && (!existed || prior.LeaseOwner != r.cfg.InstanceID) {
			acquisitions++
		}
		if existed && prior.DesiredOwner != partition.DesiredOwner {
			rebalanced = true
		}
	}
	r.orderingPartitions = partitions
	r.orderingMu.Unlock()
	r.metrics.AddOrderingAcquisitions(acquisitions)
	if rebalanced {
		r.metrics.IncOrderingRebalance()
	}
}

func (r *Relay) releaseOrderingPartitions(store OrderingPartitionStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.ReleaseOrderingPartitions(ctx, r.cfg.InstanceID, r.cfg.OrderingSafetyMargin); err != nil {
		r.log.Warn("release ordered partitions failed",
			"error", err,
			"relay_instance", r.cfg.InstanceID,
		)
	}
}

func (r *Relay) currentOrderingPartitions() []OrderingPartition {
	r.orderingMu.RLock()
	defer r.orderingMu.RUnlock()
	return append([]OrderingPartition(nil), r.orderingPartitions...)
}
