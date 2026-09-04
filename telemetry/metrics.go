package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "emitlane"

// Metrics holds Prometheus instruments. A nil Metrics is safe to call.
type Metrics struct {
	enqueued             prometheus.Counter
	delivered            prometheus.Counter
	failed               *prometheus.CounterVec
	retried              prometheus.Counter
	dead                 prometheus.Counter
	pending              prometheus.Gauge
	inflight             prometheus.Gauge
	deadGauge            prometheus.Gauge
	deliveryDuration     prometheus.Histogram
	publishDuration      prometheus.Histogram
	oldestPending        prometheus.Gauge
	relayPaused          prometheus.Gauge
	relaysActive         prometheus.Gauge
	relaysStale          prometheus.Gauge
	replayBatches        prometheus.Counter
	replayedEvents       prometheus.Counter
	adminMutations       *prometheus.CounterVec
	controlFailures      prometheus.Counter
	presenceFailures     *prometheus.CounterVec
	orderingStreams      prometheus.Gauge
	orderingBlocked      prometheus.Gauge
	orderingGaps         prometheus.Gauge
	orderingDeadBlocked  prometheus.Gauge
	orderingOwned        prometheus.Gauge
	orderingHandoff      prometheus.Gauge
	orderingAcquisitions prometheus.Counter
	orderingRebalances   prometheus.Counter
	orderingDeliveryWait prometheus.Histogram
	orderingGapAge       prometheus.Gauge
}

// NewMetrics registers instruments with reg. The only label is the bounded
// publish result: retryable or permanent.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		enqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_enqueued_total",
			Help:      "Successful Writer INSERT calls; the caller-owned transaction may still roll back.",
		}),
		delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_delivered_total",
			Help:      "Outbox events marked delivered after broker acknowledgement.",
		}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_failed_total",
			Help:      "Broker publish attempts that failed.",
		}, []string{"result"}),
		retried: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_retried_total",
			Help:      "Failed events scheduled for retry.",
		}),
		dead: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_dead_total",
			Help:      "Events moved to dead state after policy exhaustion.",
		}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "pending_events",
			Help:      "Current number of pending outbox events.",
		}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "inflight_events",
			Help:      "Current number of inflight outbox events.",
		}),
		deadGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "dead_events",
			Help:      "Current number of dead outbox events.",
		}),
		deliveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "delivery_duration_seconds",
			Help:      "Time from event creation to delivered state.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}),
		publishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "publish_duration_seconds",
			Help:      "Broker publish latency after claim commit.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}),
		oldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "oldest_pending_seconds",
			Help:      "Age in seconds of the oldest pending outbox event.",
		}),
		relayPaused: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "relay_paused",
			Help:      "Whether durable cluster-wide relay pause is enabled (1) or disabled (0).",
		}),
		relaysActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "relay_instances_active",
			Help:      "Relay instances with a recent heartbeat and no stopped marker.",
		}),
		relaysStale: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "relay_instances_stale",
			Help:      "Relay instances whose heartbeat is older than the stale threshold.",
		}),
		replayBatches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "replay_batches_total",
			Help:      "Successfully committed single-event and batch replay operations.",
		}),
		replayedEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "replayed_events_total",
			Help:      "New outbox events created by successfully committed replay operations.",
		}),
		adminMutations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "admin_mutations_total",
			Help:      "Administrative mutation attempts by bounded action and result.",
		}, []string{"action", "result"}),
		controlFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "control_read_failures_total",
			Help:      "Failures reading durable relay control state.",
		}),
		presenceFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "relay_presence_failures_total",
			Help:      "Best-effort relay presence failures by operation.",
		}, []string{"operation"}),
		orderingStreams:      prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_streams", Help: "Durable ordered streams."}),
		orderingBlocked:      prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_streams_blocked", Help: "Ordered streams blocked by retry wait, gap, or dead event."}),
		orderingGaps:         prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_streams_gap", Help: "Ordered streams whose expected sequence is missing while a future sequence exists."}),
		orderingDeadBlocked:  prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_streams_dead_blocked", Help: "Ordered streams blocked by a dead expected event."}),
		orderingOwned:        prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_partitions_owned", Help: "Virtual ordering partitions with a valid owner outside handoff."}),
		orderingHandoff:      prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_partitions_handoff", Help: "Virtual ordering partitions waiting for the stale-publish handoff barrier."}),
		orderingAcquisitions: prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: "ordering_partition_acquisitions_total", Help: "Virtual ordering partitions newly observed as owned by this Relay."}),
		orderingRebalances:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: "ordering_partition_rebalances_total", Help: "Desired ownership maps changed after Relay membership changes."}),
		orderingDeliveryWait: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "ordering_delivery_wait_seconds", Help: "Wait from ordered event availability to broker publish start.", Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}}),
		orderingGapAge:       prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "ordering_gap_age_seconds", Help: "Age of the oldest currently observed ordered gap."}),
	}
	// CounterVec collectors are otherwise absent from exposition until their
	// first observation. Initialize the complete, bounded label set so operators
	// can alert on these series before the first failure occurs.
	m.failed.WithLabelValues("retryable")
	m.failed.WithLabelValues("permanent")
	for _, action := range []string{"relay.pause", "relay.resume", "event.retry", "event.replay", "replay.batch"} {
		m.adminMutations.WithLabelValues(action, "success")
		m.adminMutations.WithLabelValues(action, "failure")
	}
	for _, operation := range []string{"register", "heartbeat", "stop"} {
		m.presenceFailures.WithLabelValues(operation)
	}
	collectors := []prometheus.Collector{
		m.enqueued,
		m.delivered,
		m.failed,
		m.retried,
		m.dead,
		m.pending,
		m.inflight,
		m.deadGauge,
		m.deliveryDuration,
		m.publishDuration,
		m.oldestPending,
		m.relayPaused,
		m.relaysActive,
		m.relaysStale,
		m.replayBatches,
		m.replayedEvents,
		m.adminMutations,
		m.controlFailures,
		m.presenceFailures,
		m.orderingStreams,
		m.orderingBlocked,
		m.orderingGaps,
		m.orderingDeadBlocked,
		m.orderingOwned,
		m.orderingHandoff,
		m.orderingAcquisitions,
		m.orderingRebalances,
		m.orderingDeliveryWait,
		m.orderingGapAge,
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, previous := range registered {
				reg.Unregister(previous)
			}
			return nil, err
		}
		registered = append(registered, c)
	}
	return m, nil
}

// IncEnqueued records a successful Writer INSERT call. The caller-owned
// transaction may still be committed or rolled back afterward.
func (m *Metrics) IncEnqueued() {
	if m == nil {
		return
	}
	m.enqueued.Inc()
}

// IncDelivered records an event marked delivered after broker acknowledgement.
func (m *Metrics) IncDelivered() {
	if m == nil {
		return
	}
	m.delivered.Inc()
}

// RecordPublishFailure increments one of the two bounded failure series.
func (m *Metrics) RecordPublishFailure(permanent bool) {
	if m == nil {
		return
	}
	result := "retryable"
	if permanent {
		result = "permanent"
	}
	m.failed.WithLabelValues(result).Inc()
}

// IncRetried records an event scheduled for another publish attempt.
func (m *Metrics) IncRetried() {
	if m == nil {
		return
	}
	m.retried.Inc()
}

// IncDead records an event transitioned to dead.
func (m *Metrics) IncDead() {
	if m == nil {
		return
	}
	m.dead.Inc()
}

// ObservePublish records non-negative broker publish latency in seconds.
func (m *Metrics) ObservePublish(seconds float64) {
	if m == nil {
		return
	}
	if seconds < 0 {
		seconds = 0
	}
	m.publishDuration.Observe(seconds)
}

// ObserveDelivery records non-negative end-to-end delivery latency in seconds.
func (m *Metrics) ObserveDelivery(seconds float64) {
	if m == nil {
		return
	}
	if seconds < 0 {
		seconds = 0
	}
	m.deliveryDuration.Observe(seconds)
}

// SetQueueDepth updates the current durable queue-state gauges.
func (m *Metrics) SetQueueDepth(pending, inflight, dead float64) {
	if m == nil {
		return
	}
	pending = max(0, pending)
	inflight = max(0, inflight)
	dead = max(0, dead)
	m.pending.Set(pending)
	m.inflight.Set(inflight)
	m.deadGauge.Set(dead)
}

// SetOldestPending updates the non-negative age of the oldest pending event.
func (m *Metrics) SetOldestPending(seconds float64) {
	if m == nil {
		return
	}
	if seconds < 0 {
		seconds = 0
	}
	m.oldestPending.Set(seconds)
}

func (m *Metrics) SetRelayPaused(paused bool) {
	if m == nil {
		return
	}
	if paused {
		m.relayPaused.Set(1)
		return
	}
	m.relayPaused.Set(0)
}

func (m *Metrics) SetRelayInstances(active, stale float64) {
	if m == nil {
		return
	}
	m.relaysActive.Set(max(0, active))
	m.relaysStale.Set(max(0, stale))
}

func (m *Metrics) RecordReplay(events int) {
	if m == nil || events <= 0 {
		return
	}
	m.replayBatches.Inc()
	m.replayedEvents.Add(float64(events))
}

func (m *Metrics) IncAdminMutation(action, result string) {
	if m == nil {
		return
	}
	m.adminMutations.WithLabelValues(action, result).Inc()
}

func (m *Metrics) IncControlFailure() {
	if m == nil {
		return
	}
	m.controlFailures.Inc()
}

func (m *Metrics) IncPresenceFailure(operation string) {
	if m == nil {
		return
	}
	m.presenceFailures.WithLabelValues(operation).Inc()
}

func (m *Metrics) SetOrderingState(streams, blocked, gaps, deadBlocked, owned, handoff, gapAge float64) {
	if m == nil {
		return
	}
	m.orderingStreams.Set(max(0, streams))
	m.orderingBlocked.Set(max(0, blocked))
	m.orderingGaps.Set(max(0, gaps))
	m.orderingDeadBlocked.Set(max(0, deadBlocked))
	m.orderingOwned.Set(max(0, owned))
	m.orderingHandoff.Set(max(0, handoff))
	m.orderingGapAge.Set(max(0, gapAge))
}

func (m *Metrics) AddOrderingAcquisitions(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.orderingAcquisitions.Add(float64(count))
}

func (m *Metrics) IncOrderingRebalance() {
	if m == nil {
		return
	}
	m.orderingRebalances.Inc()
}

func (m *Metrics) ObserveOrderingDeliveryWait(seconds float64) {
	if m == nil {
		return
	}
	m.orderingDeliveryWait.Observe(max(0, seconds))
}
