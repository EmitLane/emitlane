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
	metrics.IncOrderingFenced("begin_attempt")
	metrics.IncOrderingFenced("unbounded-value-must-be-ignored")
	metrics.ObserveIntegrityCheck("summary", "clean", 0.03)
	metrics.ObserveIntegrityCheck("unbounded-mode", "clean", 0.03)
	metrics.ObserveConsumerRecord("billing-v1", "processed", 0.2)
	metrics.ObserveConsumerRecord("billing-v1", "unbounded-value-must-be-ignored", 0.2)
	metrics.IncConsumerRetry("billing-v1")
	metrics.IncConsumerDuplicate("billing-v1")
	metrics.AddConsumerDead("billing-v1", 1)
	metrics.AddConsumerInflight("billing-v1", 1)
	metrics.IncConsumerRebalance("billing-v1")
	metrics.AddConsumerPaused("billing-v1", "retry", 1)
	metrics.AddConsumerPaused("billing-v1", "unbounded-value-must-be-ignored", 1)
	metrics.SetConsumerLag("billing-v1", "orders", 12)
	metrics.SetRelayCapacity(3, 4)
	metrics.ObserveRelayClaim(3, 0.01)
	metrics.RecordRelayBackpressure("workers_saturated")
	metrics.RecordRelayBackpressure("unbounded-value-must-be-ignored")
	metrics.IncRelayWakeup("notification")
	metrics.IncRelayWakeup("unbounded-value-must-be-ignored")

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
		"emitlane_ordering_fenced_attempts_total",
		"emitlane_integrity_checks_total",
		"emitlane_integrity_check_duration_seconds",
		"emitlane_consumer_records_total",
		"emitlane_consumer_processing_duration_seconds",
		"emitlane_consumer_retries_total",
		"emitlane_consumer_duplicates_total",
		"emitlane_consumer_dead_events",
		"emitlane_consumer_inflight",
		"emitlane_consumer_rebalances_total",
		"emitlane_consumer_paused_partitions",
		"emitlane_consumer_lag_records",
		"emitlane_relay_active_workers",
		"emitlane_relay_worker_capacity",
		"emitlane_relay_worker_saturation_ratio",
		"emitlane_relay_claim_size",
		"emitlane_relay_claim_duration_seconds",
		"emitlane_relay_backpressure_total",
		"emitlane_relay_wakeups_total",
	} {
		if !got[name] {
			t.Errorf("metric family %s is missing", name)
		}
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() == "unbounded-value-must-be-ignored" || label.GetValue() == "unbounded-mode" {
					t.Fatalf("unbounded metric label escaped validation: %s=%s", label.GetName(), label.GetValue())
				}
			}
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
	metrics.IncOrderingFenced("begin_attempt")
	metrics.ObserveIntegrityCheck("summary", "clean", 1)
	metrics.ObserveConsumerRecord("billing-v1", "processed", 1)
	metrics.IncConsumerRetry("billing-v1")
	metrics.IncConsumerDuplicate("billing-v1")
	metrics.AddConsumerDead("billing-v1", 1)
	metrics.AddConsumerInflight("billing-v1", 1)
	metrics.IncConsumerRebalance("billing-v1")
	metrics.AddConsumerPaused("billing-v1", "retry", 1)
	metrics.SetConsumerLag("billing-v1", "orders", 1)
	metrics.SetRelayCapacity(1, 1)
	metrics.ObserveRelayClaim(1, 1)
	metrics.RecordRelayBackpressure("workers_saturated")
	metrics.IncRelayWakeup("poll")
}
