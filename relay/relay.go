package relay

import (
	"context"
	"errors"
	"log/slog"
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

// New constructs a Relay. Config is validated.
func New(cfg Config, store Store, pub broker.Publisher, opts ...Option) (*Relay, error) {
	if cfg.InstanceID == "" {
		cfg.InstanceID = NewInstanceID()
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
		cfg:   cfg,
		store: store,
		pub:   pub,
		log:   slog.Default(),
		clock: clock.System{},
		rnd:   newLockedRand(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()^0xdeadbeef)),
		wake:  make(chan struct{}, 1),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
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
	go r.statsLoop(runCtx)
	if r.cfg.Retention > 0 && r.cfg.CleanupInterval > 0 && r.cfg.CleanupBatch > 0 {
		go r.cleanupLoop(runCtx)
	}

	timer := time.NewTimer(r.cfg.PollInterval)
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
		timer.Reset(r.cfg.PollInterval)

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
		events, err := r.store.Claim(claimCtx, r.cfg.InstanceID, claimLimit, r.cfg.LeaseDuration)
		if err != nil {
			return err
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
	attempt, err := r.store.BeginAttempt(ctx, ev.ID, r.cfg.InstanceID, r.cfg.MaxAttempts)
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
	if err := r.store.MarkDelivered(markCtx, ev.ID, r.cfg.InstanceID); err != nil {
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
	if err := r.store.MarkRetry(markCtx, ev.ID, r.cfg.InstanceID, delay, lastErr); err != nil {
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

func (r *Relay) markDead(ctx context.Context, ev Event, reason string) {
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := r.store.MarkDead(markCtx, ev.ID, r.cfg.InstanceID, reason); err != nil {
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
	st, err := r.store.StatsSnapshot(ctx)
	if err != nil {
		r.log.Warn("stats snapshot failed", "error", err)
		return
	}
	r.metrics.SetQueueDepth(float64(st.Pending), float64(st.Inflight), float64(st.Dead))
	r.metrics.SetOldestPending(st.OldestPendingSeconds)
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
	delete(headers, broker.HeaderTraceparent)
	delete(headers, broker.HeaderTracestate)
	headers[broker.HeaderEventID] = ev.ID.String()
	headers[broker.HeaderEventType] = ev.Type
	headers[broker.HeaderSchemaVersion] = strconv.Itoa(ev.SchemaVersion)
	headers[broker.HeaderAttempt] = strconv.Itoa(ev.Attempts)
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
