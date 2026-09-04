package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsExposeRequiredFamilies(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	metrics, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	metrics.IncEnqueued()
	metrics.IncDelivered()
	metrics.RecordPublishFailure(false)
	metrics.RecordPublishFailure(true)
	metrics.IncRetried()
	metrics.IncDead()
	metrics.SetQueueDepth(1, 2, 3)
	metrics.ObserveDelivery(0.25)
	metrics.ObservePublish(0.05)
	metrics.SetOldestPending(4)
	metrics.SetRelayPaused(true)
	metrics.SetRelayInstances(2, 1)
	metrics.RecordReplay(3)
	metrics.IncAdminMutation("event.replay", "success")
	metrics.IncControlFailure()
	metrics.IncPresenceFailure("heartbeat")
	metrics.SetOrderingState(10, 3, 2, 1, 64, 4, 12)
	metrics.AddOrderingAcquisitions(4)
	metrics.IncOrderingRebalance()
	metrics.ObserveOrderingDeliveryWait(0.2)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(families))
	for _, family := range families {
		got[family.GetName()] = true
	}
	for _, name := range []string{
		"emitlane_events_enqueued_total",
		"emitlane_events_delivered_total",
		"emitlane_events_failed_total",
		"emitlane_events_retried_total",
		"emitlane_events_dead_total",
		"emitlane_pending_events",
		"emitlane_inflight_events",
		"emitlane_dead_events",
		"emitlane_delivery_duration_seconds",
		"emitlane_publish_duration_seconds",
		"emitlane_oldest_pending_seconds",
		"emitlane_relay_paused",
		"emitlane_relay_instances_active",
		"emitlane_relay_instances_stale",
		"emitlane_replay_batches_total",
		"emitlane_replayed_events_total",
		"emitlane_admin_mutations_total",
		"emitlane_control_read_failures_total",
		"emitlane_relay_presence_failures_total",
		"emitlane_ordering_streams",
		"emitlane_ordering_streams_blocked",
		"emitlane_ordering_streams_gap",
		"emitlane_ordering_streams_dead_blocked",
		"emitlane_ordering_partitions_owned",
		"emitlane_ordering_partitions_handoff",
		"emitlane_ordering_partition_acquisitions_total",
		"emitlane_ordering_partition_rebalances_total",
		"emitlane_ordering_delivery_wait_seconds",
		"emitlane_ordering_gap_age_seconds",
	} {
		if !got[name] {
			t.Errorf("metric family %s is missing", name)
		}
	}
}

func TestNilMetricsIsNoOp(t *testing.T) {
	t.Parallel()
	var metrics *Metrics
	metrics.IncEnqueued()
	metrics.IncDelivered()
	metrics.RecordPublishFailure(false)
	metrics.IncRetried()
	metrics.IncDead()
	metrics.SetQueueDepth(1, 2, 3)
	metrics.ObserveDelivery(1)
	metrics.ObservePublish(1)
	metrics.SetOldestPending(1)
	metrics.SetRelayPaused(true)
	metrics.SetRelayInstances(1, 1)
	metrics.RecordReplay(1)
	metrics.IncAdminMutation("event.replay", "success")
	metrics.IncControlFailure()
	metrics.IncPresenceFailure("heartbeat")
	metrics.SetOrderingState(1, 1, 1, 1, 1, 1, 1)
	metrics.AddOrderingAcquisitions(1)
	metrics.IncOrderingRebalance()
	metrics.ObserveOrderingDeliveryWait(1)
}
