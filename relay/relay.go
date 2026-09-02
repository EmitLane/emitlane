package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/emitlane/emitlane/broker"
	"github.com/emitlane/emitlane/internal/clock"
	"github.com/emitlane/emitlane/telemetry"
)

// FailureHooks are test-only crash windows. Production must leave them nil.
//
// AfterClaimCommit is invoked after the claim transaction commits and before
// broker publish. A returned error skips publish; the row stays inflight.
//
// AfterPublishAck is invoked after broker acknowledgement and before the
// delivered update. A returned error skips that update; the row stays recoverable.
type FailureHooks struct {
	AfterClaimCommit func(ctx context.Context, event Event) error
	AfterPublishAck  func(ctx context.Context, event Event) error
}

// Relay claims outbox rows, publishes outside the claim transaction, and
// records the result. Delivery is at-least-once.
type Relay struct {
	cfg      Config
	store    Store
	pub      broker.Publisher
	log      *slog.Logger
	metrics  *telemetry.Metrics
	clock    clock.Clock
	rnd      randSource
	hooks    FailureHooks
	wake     chan struct{}
	listener WakeupListener
	presence RelayPresence

	orderingMu         sync.RWMutex
	orderingPartitions []OrderingPartition
}

// Option configures Relay.
type Option func(*Relay)

// WithLogger sets the structured logger. Payloads are never logged.
func WithLogger(log *slog.Logger) Option {
	return func(r *Relay) { r.log = log }
}

// WithMetrics sets Prometheus instruments.
func WithMetrics(m *telemetry.Metrics) Option {
	return func(r *Relay) { r.metrics = m }
}

// WithFailureHooks installs crash-window hooks for tests.
func WithFailureHooks(h FailureHooks) Option {
	return func(r *Relay) { r.hooks = h }
}

// WithWakeupListener enables a low-latency wake-up source. Polling is always
// active even when the listener cannot connect.
func WithWakeupListener(listener WakeupListener) Option {
	return func(r *Relay) { r.listener = listener }
}

// WithPresenceInfo supplies process metadata for relay discovery. Empty values
// are replaced with safe local defaults.
func WithPresenceInfo(hostname, version string) Option {
	return func(r *Relay) {
		r.presence.Hostname = hostname
		r.presence.Version = version
	}
}

// New constructs a Relay. Config is validated.
func New(cfg Config, store Store, pub broker.Publisher, opts ...Option) (*Relay, error) {
	if cfg.InstanceID == "" {
		cfg.InstanceID = NewInstanceID()
	}
	// These fields were added in v0.2. Treat their zero value as "use the v0.2
	// default" so callers that construct a complete v0.1 Config literal remain
	// source-compatible when passed to New. Negative values still fail Validate.
	defaults := DefaultConfig()
	if cfg.ControlInterval == 0 {
		cfg.ControlInterval = defaults.ControlInterval
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if cfg.PresenceStaleAfter == 0 {
		cfg.PresenceStaleAfter = defaults.PresenceStaleAfter
	}
	if cfg.OrderingRebalanceInterval == 0 {
		cfg.OrderingRebalanceInterval = defaults.OrderingRebalanceInterval
	}
	if cfg.OrderingLeaseDuration == 0 {
		cfg.OrderingLeaseDuration = defaults.OrderingLeaseDuration
	}
	if cfg.OrderingSafetyMargin == 0 {
		cfg.OrderingSafetyMargin = defaults.OrderingSafetyMargin
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("relay: store is required")
	}
	if pub == nil {
		return nil, errors.New("relay: publisher is required")
	}
	r := &Relay{
		cfg:      cfg,
		store:    store,
		pub:      pub,
		log:      slog.Default(),
		clock:    clock.System{},
		rnd:      newLockedRand(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()^0xdeadbeef)),
		wake:     make(chan struct{}, 1),
		presence: RelayPresence{InstanceID: cfg.InstanceID, OrderingCapable: true},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	if r.presence.Hostname == "" {
		r.presence.Hostname, _ = os.Hostname()
		if r.presence.Hostname == "" {
			r.presence.Hostname = "unknown"
		}
	}
	if r.presence.Version == "" {
		r.presence.Version = "dev"
	}
	r.presence.InstanceID = cfg.InstanceID
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.clock == nil {
		r.clock = clock.System{}
	}
	if r.rnd == nil {
		r.rnd = newLockedRand(1, 2)
	}
	return r, nil
}

// Run blocks until ctx is cancelled. Cancellation stops new claims immediately
// and lets already-started publishes drain until ShutdownTimeout.
func (r *Relay) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workCtx, forceWorkStop := context.WithCancel(context.WithoutCancel(ctx))
	defer forceWorkStop()

	runDone := make(chan struct{})
	defer close(runDone)
	go func() {
		select {
		case <-ctx.Done():
			timer := time.NewTimer(r.cfg.ShutdownTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				forceWorkStop()
			case <-runDone:
			}
		case <-runDone:
		}
	}()

	if r.listener != nil {
		go r.listener.Run(runCtx, r.wake)
	}
	if presence, ok := r.store.(PresenceStore); ok {
		r.presence.StartedAt = r.clock.Now()
		if err := presence.RegisterRelay(runCtx, r.presence); err != nil {
			r.log.Warn("relay presence registration failed; delivery continues", "error", err, "relay_instance", r.cfg.InstanceID)
			r.metrics.IncPresenceFailure("register")
		}
		go r.heartbeatLoop(runCtx, presence)
		defer r.markStopped(presence)
	}
	if orderingStore, ok := r.store.(OrderingPartitionStore); ok {
		orderingDone := make(chan struct{})
		go func() {
			defer close(orderingDone)
			r.orderingLoop(runCtx, orderingStore)
		}()
		defer func() {
			cancel()
			select {
			case <-orderingDone:
			case <-time.After(10 * time.Second):
				r.log.Warn("timed out stopping ordered partition reconciliation",
					"relay_instance", r.cfg.InstanceID)
			}
			r.releaseOrderingPartitions(orderingStore)
		}()
	}
	go r.statsLoop(runCtx)
	if r.cfg.Retention > 0 && r.cfg.CleanupInterval > 0 && r.cfg.CleanupBatch > 0 {
		go r.cleanupLoop(runCtx)
	}

	tickInterval := r.cfg.PollInterval
	if _, ok := r.store.(PauseState); ok {
		tickInterval = min(tickInterval, r.cfg.ControlInterval)
	}
	timer := time.NewTimer(tickInterval)
	defer timer.Stop()

	r.log.Info("relay started",
		"relay_instance", r.cfg.InstanceID,
		"batch_size", r.cfg.BatchSize,
		"concurrency", r.cfg.Concurrency,
		"poll_interval", r.cfg.PollInterval,
		"lease_duration", r.cfg.LeaseDuration,
	)

	for {
		if err := r.tick(runCtx, workCtx); err != nil && runCtx.Err() == nil {
			r.log.Error("relay tick failed", "error", err, "relay_instance", r.cfg.InstanceID)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(tickInterval)

		select {
		case <-runCtx.Done():
			r.log.Info("relay stopped", "relay_instance", r.cfg.InstanceID)
			return nil
		case <-r.wake:
			drain(r.wake)
		case <-timer.C:
		}
	}
}

func drain(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (r *Relay) tick(claimCtx, workCtx context.Context) error {
	claimLimit := min(r.cfg.BatchSize, r.cfg.Concurrency)
	for claimCtx.Err() == nil {
		if pauseState, ok := r.store.(PauseState); ok {
			paused, err := pauseState.RelayPaused(claimCtx)
			if err != nil {
				r.metrics.IncControlFailure()
				return fmt.Errorf("read relay pause state: %w", err)
			}
			r.metrics.SetRelayPaused(paused)
			if paused {
				return nil
			}
		}
		var events []Event
		if orderedStore, ok := r.store.(OrderedDeliveryStore); ok {
			ordered, err := orderedStore.ClaimOrdered(
				claimCtx,
				r.cfg.InstanceID,
				claimLimit,
				r.cfg.LeaseDuration,
				r.cfg.PublishTimeout+r.cfg.OrderingSafetyMargin,
			)
			if err != nil {
				return err
			}
			events = append(events, ordered...)
		}
		if len(events) < claimLimit {
			unordered, err := r.store.Claim(claimCtx, r.cfg.InstanceID, claimLimit-len(events), r.cfg.LeaseDuration)
			if err != nil {
				return err
			}
			events = append(events, unordered...)
		}
		if len(events) == 0 {
			return nil
		}

		var wg sync.WaitGroup
		wg.Add(len(events))
		for _, ev := range events {
			ev := ev
			go func() {
				defer wg.Done()
				r.handle(workCtx, ev)
			}()
		}
		wg.Wait()
	}
	return nil
}

func (r *Relay) handle(ctx context.Context, ev Event) {
	if r.hooks.AfterClaimCommit != nil {
		if err := r.hooks.AfterClaimCommit(ctx, ev); err != nil {
			r.log.Info("failure hook after claim; leaving inflight",
				"event_id", ev.ID.String(),
				"error", err,
				"relay_instance", r.cfg.InstanceID,
			)
			return
		}
	}

	if ev.Attempts >= r.cfg.MaxAttempts {
		r.markDead(ctx, ev, "publish attempt budget exhausted during lease recovery")
		return
	}
	attempt, err := r.beginAttempt(ctx, ev)
	if err != nil {
		r.log.Error("begin publish attempt failed; event remains recoverable",
			"event_id", ev.ID.String(),
			"relay_instance", r.cfg.InstanceID,
			"error", err,
		)
		return
	}
	ev.Attempts = attempt

	msg := toMessage(ev)
	pubCtx := telemetry.ExtractTrace(ctx, ev.Traceparent, ev.Tracestate)
	pubCtx, span := telemetry.Tracer().Start(pubCtx, "emitlane.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	span.SetAttributes(
		attribute.String("messaging.destination.name", ev.Destination),
		attribute.String("emitlane.event_id", ev.ID.String()),
		attribute.String("emitlane.event_type", ev.Type),
		attribute.Int("emitlane.attempt", ev.Attempts),
		attribute.String("emitlane.relay_instance", r.cfg.InstanceID),
	)
	traceparent, tracestate := telemetry.InjectTrace(pubCtx)
	setHeader(msg.Headers, broker.HeaderTraceparent, traceparent)
	setHeader(msg.Headers, broker.HeaderTracestate, tracestate)

	if r.cfg.PublishTimeout > 0 {
		var cancel context.CancelFunc
		pubCtx, cancel = context.WithTimeout(pubCtx, r.cfg.PublishTimeout)
		defer cancel()
	}
	if err := pubCtx.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		r.onPublishFailure(ctx, ev, err)
		return
	}

	start := r.clock.Now()
	err = r.pub.Publish(pubCtx, msg)
	r.metrics.ObservePublish(nonNegativeSeconds(r.clock.Now().Sub(start)))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		r.onPublishFailure(ctx, ev, err)
		return
	}
	span.End()

	if r.hooks.AfterPublishAck != nil {
		if hookErr := r.hooks.AfterPublishAck(ctx, ev); hookErr != nil {
			r.log.Info("failure hook after broker ack; leaving recoverable",
				"event_id", ev.ID.String(),
				"error", hookErr,
				"relay_instance", r.cfg.InstanceID,
				"status", ev.Status,
			)
			return
		}
	}

	// Broker ACK is durable. Mark delivered even if the parent context is
	// cancelled, otherwise the crash window is the normal at-least-once path.
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := r.markDelivered(markCtx, ev); err != nil {
		r.log.Error("mark delivered failed after broker ack; event remains recoverable",
			"event_id", ev.ID.String(),
			"destination", ev.Destination,
			"attempt", ev.Attempts,
			"relay_instance", r.cfg.InstanceID,
			"error", err,
		)
		return
	}
	r.metrics.IncDelivered()
	r.metrics.ObserveDelivery(nonNegativeSeconds(r.clock.Now().Sub(ev.CreatedAt)))
	r.log.Info("event delivered",
		"event_id", ev.ID.String(),
		"destination", ev.Destination,
		"event_type", ev.Type,
		"attempt", ev.Attempts,
		"relay_instance", r.cfg.InstanceID,
		"status", "delivered",
		"duration", r.clock.Now().Sub(start),
	)
}

func (r *Relay) beginAttempt(ctx context.Context, ev Event) (int, error) {
	if ev.OrderingKey == "" {
		return r.store.BeginAttempt(ctx, ev.ID, r.cfg.InstanceID, r.cfg.MaxAttempts)
	}
	store, ok := r.store.(OrderedDeliveryStore)
	if !ok {
		return 0, errors.New("relay: ordered event claimed without ordered store capability")
	}
	return store.BeginOrderedAttempt(ctx, ev.ID, r.cfg.InstanceID, ev.OrderingEpoch,
		r.cfg.MaxAttempts, r.cfg.PublishTimeout+r.cfg.OrderingSafetyMargin)
}

func (r *Relay) markDelivered(ctx context.Context, ev Event) error {
	if ev.OrderingKey == "" {
		return r.store.MarkDelivered(ctx, ev.ID, r.cfg.InstanceID)
	}
	store, ok := r.store.(OrderedDeliveryStore)
	if !ok {
		return errors.New("relay: ordered event missing ordered store capability")
	}
	return store.MarkOrderedDelivered(ctx, ev, r.cfg.InstanceID)
}

func nonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}

func (r *Relay) onPublishFailure(ctx context.Context, ev Event, pubErr error) {
	if ctx.Err() != nil && !errors.Is(pubErr, context.DeadlineExceeded) {
		r.log.Info("publish interrupted by shutdown; leaving inflight",
			"event_id", ev.ID.String(),
			"relay_instance", r.cfg.InstanceID,
			"error", pubErr,
		)
		return
	}

	permanent := broker.IsPermanent(pubErr)
	r.metrics.RecordPublishFailure(permanent)

	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	lastErr := pubErr.Error()
	if permanent || ev.Attempts >= r.cfg.MaxAttempts {
		r.markDead(markCtx, ev, lastErr)
		return
	}

	delay := delay(ev.Attempts, r.cfg.BaseDelay, r.cfg.MaxDelay, r.rnd)
	if err := r.markRetry(markCtx, ev, delay, lastErr); err != nil {
		r.log.Error("mark retry failed; event remains inflight",
			"event_id", ev.ID.String(),
			"relay_instance", r.cfg.InstanceID,
			"error", err,
		)
		return
	}
	r.metrics.IncRetried()
	r.log.Warn("event scheduled for retry",
		"event_id", ev.ID.String(),
		"destination", ev.Destination,
		"event_type", ev.Type,
		"attempt", ev.Attempts,
		"relay_instance", r.cfg.InstanceID,
		"status", "pending",
		"error", pubErr,
	)
}

func (r *Relay) markRetry(ctx context.Context, ev Event, delay time.Duration, lastError string) error {
	if ev.OrderingKey == "" {
		return r.store.MarkRetry(ctx, ev.ID, r.cfg.InstanceID, delay, lastError)
	}
	store, ok := r.store.(OrderedDeliveryStore)
	if !ok {
		return errors.New("relay: ordered event missing ordered store capability")
	}
	return store.MarkOrderedRetry(ctx, ev, r.cfg.InstanceID, delay, lastError)
}

func (r *Relay) markDead(ctx context.Context, ev Event, reason string) {
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var err error
	if ev.OrderingKey == "" {
		err = r.store.MarkDead(markCtx, ev.ID, r.cfg.InstanceID, reason)
	} else if store, ok := r.store.(OrderedDeliveryStore); ok {
		err = store.MarkOrderedDead(markCtx, ev, r.cfg.InstanceID, reason)
	} else {
		err = errors.New("relay: ordered event missing ordered store capability")
	}
	if err != nil {
		r.log.Error("mark dead failed; event remains inflight",
			"event_id", ev.ID.String(),
			"relay_instance", r.cfg.InstanceID,
			"error", err,
		)
		return
	}
	r.metrics.IncDead()
	r.log.Error("event dead",
		"event_id", ev.ID.String(),
		"destination", ev.Destination,
		"event_type", ev.Type,
		"attempt", ev.Attempts,
		"relay_instance", r.cfg.InstanceID,
		"status", "dead",
		"error", reason,
	)
}

func (r *Relay) statsLoop(ctx context.Context) {
	if r.cfg.StatsInterval <= 0 {
		return
	}
	t := time.NewTicker(r.cfg.StatsInterval)
	defer t.Stop()
	r.refreshStats(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refreshStats(ctx)
		}
	}
}

func (r *Relay) refreshStats(ctx context.Context) {
	var (
		st  Stats
		err error
	)
	if store, ok := r.store.(StatsWithPresence); ok {
		st, err = store.StatsSnapshotWithPresence(ctx, r.cfg.PresenceStaleAfter)
	} else {
		st, err = r.store.StatsSnapshot(ctx)
	}
	if err != nil {
		r.log.Warn("stats snapshot failed", "error", err)
		return
	}
	r.metrics.SetQueueDepth(float64(st.Pending), float64(st.Inflight), float64(st.Dead))
	r.metrics.SetOldestPending(st.OldestPendingSeconds)
	r.metrics.SetRelayPaused(st.Paused)
	r.metrics.SetRelayInstances(float64(st.RelaysActive), float64(st.RelaysStale))
}

func (r *Relay) heartbeatLoop(ctx context.Context, store PresenceStore) {
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := store.HeartbeatRelay(ctx, r.cfg.InstanceID); err != nil && ctx.Err() == nil {
				r.log.Warn("relay heartbeat failed; delivery continues", "error", err, "relay_instance", r.cfg.InstanceID)
				r.metrics.IncPresenceFailure("heartbeat")
			}
		}
	}
}

func (r *Relay) markStopped(store PresenceStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.MarkRelayStopped(ctx, r.cfg.InstanceID); err != nil {
		r.log.Warn("mark relay stopped failed", "error", err, "relay_instance", r.cfg.InstanceID)
		r.metrics.IncPresenceFailure("stop")
	}
}

func (r *Relay) cleanupLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.CleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := r.store.CleanupDelivered(ctx, r.cfg.Retention, r.cfg.CleanupBatch)
			if err != nil {
				r.log.Warn("delivered cleanup failed", "error", err)
				continue
			}
			if n > 0 {
				r.log.Info("cleaned delivered events", "count", n, "relay_instance", r.cfg.InstanceID)
			}
		}
	}
}

func toMessage(ev Event) broker.Message {
	headers := make(map[string]string, len(ev.Headers)+8)
	for k, v := range ev.Headers {
		headers[k] = v
	}
	delete(headers, broker.HeaderEventID)
	delete(headers, broker.HeaderEventType)
	delete(headers, broker.HeaderSchemaVersion)
	delete(headers, broker.HeaderAttempt)
	delete(headers, broker.HeaderOriginalEvent)
	delete(headers, broker.HeaderReplayBatch)
	delete(headers, broker.HeaderOrderingKey)
	delete(headers, broker.HeaderSequence)
	delete(headers, broker.HeaderPartition)
	delete(headers, broker.HeaderTraceparent)
	delete(headers, broker.HeaderTracestate)
	headers[broker.HeaderEventID] = ev.ID.String()
	headers[broker.HeaderEventType] = ev.Type
	headers[broker.HeaderSchemaVersion] = strconv.Itoa(ev.SchemaVersion)
	headers[broker.HeaderAttempt] = strconv.Itoa(ev.Attempts)
	if ev.ReplayedFromEventID != nil {
		headers[broker.HeaderOriginalEvent] = ev.ReplayedFromEventID.String()
	}
	if ev.ReplayBatchID != nil {
		headers[broker.HeaderReplayBatch] = ev.ReplayBatchID.String()
	}
	if ev.OrderingKey != "" && ev.OrderingPartition != nil {
		headers[broker.HeaderOrderingKey] = ev.OrderingKey
		headers[broker.HeaderSequence] = strconv.FormatInt(ev.OrderingSequence, 10)
		headers[broker.HeaderPartition] = strconv.Itoa(int(*ev.OrderingPartition))
	}
	if ev.Traceparent != "" {
		headers[broker.HeaderTraceparent] = ev.Traceparent
	}
	if ev.Tracestate != "" {
		headers[broker.HeaderTracestate] = ev.Tracestate
	}
	if ev.CorrelationID != "" {
		headers["emitlane-correlation-id"] = ev.CorrelationID
	}
	if ev.CausationID != "" {
		headers["emitlane-causation-id"] = ev.CausationID
	}
	return broker.Message{
		ID:          ev.ID.String(),
		Destination: ev.Destination,
		Key:         ev.Key,
		Payload:     ev.Payload,
		Headers:     headers,
	}
}

func setHeader(headers map[string]string, name, value string) {
	if value == "" {
		delete(headers, name)
		return
	}
	headers[name] = value
}
