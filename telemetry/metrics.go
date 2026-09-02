package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "emitlane"

// Metrics holds Prometheus instruments. A nil Metrics is safe to call.
type Metrics struct {
	enqueued         prometheus.Counter
	delivered        prometheus.Counter
	failed           *prometheus.CounterVec
	retried          prometheus.Counter
	dead             prometheus.Counter
	pending          prometheus.Gauge
	inflight         prometheus.Gauge
	deadGauge        prometheus.Gauge
	deliveryDuration prometheus.Histogram
	publishDuration  prometheus.Histogram
	oldestPending    prometheus.Gauge
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
	}
	// CounterVec collectors are otherwise absent from exposition until their
	// first observation. Initialize the complete, bounded label set so operators
	// can alert on these series before the first failure occurs.
	m.failed.WithLabelValues("retryable")
	m.failed.WithLabelValues("permanent")
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
