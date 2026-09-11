package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/emitlane/emitlane/inbox"
	"github.com/emitlane/emitlane/telemetry"
)

type Option func(*Runtime)

func WithIdentityResolver(resolver IdentityResolver) Option {
	return func(runtime *Runtime) {
		if resolver != nil {
			runtime.resolveIdentity = resolver
		}
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(runtime *Runtime) {
		if logger != nil {
			runtime.logger = logger
		}
	}
}

func WithMetrics(metrics *telemetry.Metrics) Option {
	return func(runtime *Runtime) { runtime.metrics = metrics }
}

// Runtime owns Kafka polling, PostgreSQL handler transactions, and offset
// commits. One group member per configured worker bounds concurrency.
type Runtime struct {
	config          Config
	pool            *pgxpool.Pool
	store           inbox.Store
	factory         SourceFactory
	handler         Handler
	resolveIdentity IdentityResolver
	formatError     func(error) string
	logger          *slog.Logger
	metrics         *telemetry.Metrics
}

func New(config Config, pool *pgxpool.Pool, store inbox.Store, factory SourceFactory, handler Handler, options ...Option) (*Runtime, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if pool == nil || store == nil || factory == nil || handler == nil {
		return nil, fmt.Errorf("consumer: PostgreSQL pool, Inbox store, source factory, and handler are required")
	}
	runtime := &Runtime{
		config: config, pool: pool, store: store, factory: factory, handler: handler,
		resolveIdentity: ResolveEventID,
		formatError:     safeErrorSummary,
		logger:          slog.Default(),
	}
	for _, option := range options {
		option(runtime)
	}
	return runtime, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.metrics.SetConsumerCapacity(r.config.Consumer, 0, r.config.Concurrency)
	defer r.metrics.SetConsumerCapacity(r.config.Consumer, 0, r.config.Concurrency)
	errCh := make(chan error, r.config.Concurrency)
	var wg sync.WaitGroup
	for index := range r.config.Concurrency {
		workerID := fmt.Sprintf("%s-%d", r.config.InstanceID, index+1)
		worker := newWorker(r, workerID)
		source, err := r.factory.NewSource(workerID, worker)
		if err != nil {
			cancel()
			wg.Wait()
			return fmt.Errorf("consumer: create source %s: %w", workerID, err)
		}
		worker.source = source
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer source.Close()
			r.metrics.AddConsumerWorker(r.config.Consumer, 1)
			defer r.metrics.AddConsumerWorker(r.config.Consumer, -1)
			if err := worker.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
	case err := <-errCh:
		cancel()
		<-done
		return err
	}
	shutdown := time.NewTimer(r.config.ShutdownTimeout)
	defer shutdown.Stop()
	select {
	case <-done:
		select {
		case err := <-errCh:
			return err
		default:
			return ctx.Err()
		}
	case <-shutdown.C:
		return fmt.Errorf("consumer: shutdown exceeded %s", r.config.ShutdownTimeout)
	}
}

type blockedPartition struct {
	record    SourceRecord
	eventID   uuid.UUID
	resumeAt  time.Time
	permanent bool
	reason    string
}

type worker struct {
	runtime *Runtime
	id      string
	source  Source

	mu           sync.Mutex
	activeCancel context.CancelFunc
	blocked      map[TopicPartition]blockedPartition
}

func newWorker(runtime *Runtime, id string) *worker {
	return &worker{runtime: runtime, id: id, blocked: make(map[TopicPartition]blockedPartition)}
}

func (w *worker) run(ctx context.Context) error {
	for {
		w.resumeDue(ctx)
		pollCtx, cancel := context.WithTimeout(ctx, w.runtime.config.MaintenancePoll)
		record, err := w.source.Poll(pollCtx)
		cancel()
		if err != nil {
			// franz-go represents a canceled poll as a synthetic fetch. With
			// BlockRebalanceOnPoll that result must also release the gate.
			w.source.AllowRebalance()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}
			// Broker and group-coordinator outages can surface through Poll as
			// control-plane errors. The current offset is unresolved, so keep
			// the member alive and let franz-go reconnect instead of turning a
			// recoverable Kafka outage into process termination.
			w.runtime.logger.WarnContext(ctx, "Kafka consumer poll failed; retrying",
				"consumer", w.runtime.config.Consumer, "worker", w.id, "error", err)
			if !waitRetry(ctx, w.runtime.config.MaintenancePoll) {
				return ctx.Err()
			}
			continue
		}
		w.runtime.metrics.ObserveConsumerPollBatch(w.runtime.config.Consumer, 1)

		resolved, retryAt, permanent, reason := w.process(ctx, record)
		if !resolved {
			w.source.Rewind(record)
			partition := TopicPartition{Topic: record.Topic, Partition: record.Partition}
			w.source.Pause(partition)
			w.block(partition, blockedPartition{record: record, eventID: retryAt.eventID, resumeAt: retryAt.at, permanent: permanent, reason: reason})
		}
		w.source.AllowRebalance()
	}
}

func waitRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type retryPoint struct {
	eventID uuid.UUID
	at      time.Time
}

func (w *worker) process(ctx context.Context, record SourceRecord) (bool, retryPoint, bool, string) {
	started := time.Now()
	message := Message{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Timestamp: record.Timestamp, Key: record.Key, Payload: record.Payload, Headers: record.Headers,
	}
	eventID, err := w.runtime.resolveIdentity(message)
	if err != nil {
		w.runtime.logger.WarnContext(ctx, "consumer protocol error blocks partition",
			"consumer", w.id, "topic", record.Topic, "partition", record.Partition,
			"offset", record.Offset, "error", err)
		w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "protocol_error", time.Since(started).Seconds())
		return false, retryPoint{}, true, "protocol"
	}
	message.EventID = eventID
	claim, err := w.runtime.store.Claim(ctx, inbox.ClaimRequest{
		Consumer: w.runtime.config.Consumer, EventID: eventID,
		Source:     inbox.Source{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Timestamp: record.Timestamp},
		LeaseOwner: w.id, LeaseDuration: w.runtime.config.LeaseDuration,
	})
	if err != nil {
		w.runtime.logger.WarnContext(ctx, "Inbox claim failed", "consumer", w.id,
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
		w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "error", time.Since(started).Seconds())
		return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.MaintenancePoll)}, false, "database"
	}
	switch claim.Disposition {
	case inbox.AlreadyProcessed:
		if err := w.source.Commit(ctx, record); err != nil {
			w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "error", time.Since(started).Seconds())
			return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.MaintenancePoll)}, false, "offset_commit"
		}
		w.runtime.metrics.IncConsumerDuplicate(w.runtime.config.Consumer)
		w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "duplicate", time.Since(started).Seconds())
		return true, retryPoint{}, false, ""
	case inbox.ClaimDead:
		return false, retryPoint{eventID: eventID}, true, "dead"
	case inbox.ClaimWaiting:
		resumeAt := claim.Event.AvailableAt
		if claim.Event.LeaseUntil != nil && claim.Event.LeaseUntil.After(resumeAt) {
			resumeAt = *claim.Event.LeaseUntil
		}
		return false, retryPoint{eventID: eventID, at: resumeAt}, false, "retry"
	case inbox.Claimed:
	default:
		return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.MaintenancePoll)}, false, "database"
	}

	message.Attempt = claim.Event.Attempts
	w.runtime.metrics.AddConsumerInflight(w.runtime.config.Consumer, 1)
	defer w.runtime.metrics.AddConsumerInflight(w.runtime.config.Consumer, -1)
	handlerCtx := traceContext(ctx, message.Headers)
	handlerCtx, cancel := context.WithTimeout(handlerCtx, w.runtime.config.HandlerTimeout)
	w.setActiveCancel(cancel)
	defer func() {
		w.setActiveCancel(nil)
		cancel()
	}()
	handlerCtx, span := telemetry.Tracer().Start(handlerCtx, "emitlane.consumer.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("emitlane.consumer", w.runtime.config.Consumer),
			attribute.String("messaging.destination.name", record.Topic),
			attribute.Int("messaging.kafka.partition", int(record.Partition)),
			attribute.Int64("messaging.kafka.offset", record.Offset),
			attribute.Int("emitlane.attempt", message.Attempt),
		),
	)
	defer span.End()
	tx, err := w.runtime.pool.Begin(handlerCtx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return w.transitionFailure(ctx, eventID, claim.Event, err, started)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(handlerCtx)) }()
	renewCtx, stopRenew := context.WithCancel(handlerCtx)
	renewed := make(chan error, 1)
	go func() {
		renewed <- w.renewLease(renewCtx, eventID, claim.Event.LeaseToken)
	}()
	handlerErr := w.runtime.handler(handlerCtx, tx, message)
	stopRenew()
	renewErr := <-renewed
	if renewErr != nil {
		span.RecordError(renewErr)
		span.SetStatus(codes.Error, renewErr.Error())
		_ = tx.Rollback(context.WithoutCancel(handlerCtx))
		if errors.Is(renewErr, inbox.ErrLeaseLost) {
			return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.LeaseDuration)}, false, "database"
		}
		return w.transitionFailure(ctx, eventID, claim.Event, renewErr, started)
	}
	if handlerErr == nil && handlerCtx.Err() != nil {
		handlerErr = handlerCtx.Err()
	}
	if handlerErr != nil {
		err := handlerErr
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = tx.Rollback(context.WithoutCancel(handlerCtx))
		return w.transitionFailure(ctx, eventID, claim.Event, err, started)
	}
	if err := w.runtime.store.MarkProcessed(handlerCtx, tx, w.runtime.config.Consumer, eventID, claim.Event.LeaseToken); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = tx.Rollback(context.WithoutCancel(handlerCtx))
		return false, retryPoint{eventID: eventID, at: *claim.Event.LeaseUntil}, false, "database"
	}
	if err := tx.Commit(handlerCtx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.MaintenancePoll)}, false, "database"
	}
	if err := w.source.Commit(ctx, record); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.MaintenancePoll)}, false, "offset_commit"
	}
	w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "processed", time.Since(started).Seconds())
	return true, retryPoint{}, false, ""
}

func (w *worker) transitionFailure(ctx context.Context, eventID uuid.UUID, event inbox.Event, failure error, started time.Time) (bool, retryPoint, bool, string) {
	transitionTimeout := w.runtime.config.ShutdownTimeout
	if transitionTimeout > 5*time.Second {
		transitionTimeout = 5 * time.Second
	}
	transitionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), transitionTimeout)
	defer cancel()
	lastError := w.runtime.formatError(failure)
	if inbox.IsPermanent(failure) || event.Attempts >= w.runtime.config.MaxAttempts {
		if err := w.runtime.store.MarkDead(transitionCtx, w.runtime.config.Consumer, eventID, event.LeaseToken, lastError); err != nil {
			w.runtime.logger.WarnContext(ctx, "failed to record dead Inbox state", "consumer", w.runtime.config.Consumer,
				"topic", event.Source.Topic, "partition", event.Source.Partition, "error", err)
			w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "error", time.Since(started).Seconds())
			return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.LeaseDuration)}, false, "database"
		}
		w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "dead", time.Since(started).Seconds())
		return false, retryPoint{eventID: eventID}, true, "dead"
	}
	delay := w.runtime.nextRetryDelay(event.Attempts)
	if err := w.runtime.store.MarkRetry(transitionCtx, w.runtime.config.Consumer, eventID, event.LeaseToken, delay, lastError); err != nil {
		w.runtime.logger.WarnContext(ctx, "failed to record Inbox retry", "consumer", w.runtime.config.Consumer,
			"topic", event.Source.Topic, "partition", event.Source.Partition, "error", err)
		w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "error", time.Since(started).Seconds())
		return false, retryPoint{eventID: eventID, at: time.Now().Add(w.runtime.config.LeaseDuration)}, false, "database"
	}
	w.runtime.metrics.IncConsumerRetry(w.runtime.config.Consumer)
	w.runtime.metrics.ObserveConsumerRecord(w.runtime.config.Consumer, "retry", time.Since(started).Seconds())
	return false, retryPoint{eventID: eventID, at: time.Now().Add(delay)}, false, "retry"
}

func safeErrorSummary(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "handler timeout"
	case inbox.IsPermanent(err):
		return "permanent handler failure"
	default:
		return "retryable handler failure"
	}
}

func (w *worker) renewLease(ctx context.Context, eventID, token uuid.UUID) error {
	ticker := time.NewTicker(w.runtime.config.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.runtime.store.Renew(ctx, w.runtime.config.Consumer, eventID, token, w.runtime.config.LeaseDuration); err != nil {
				w.mu.Lock()
				cancel := w.activeCancel
				w.mu.Unlock()
				if cancel != nil {
					cancel()
				}
				return fmt.Errorf("consumer: renew Inbox lease: %w", err)
			}
		}
	}
}

func traceContext(ctx context.Context, headers []Header) context.Context {
	var traceparent, tracestate string
	for _, header := range headers {
		switch header.Key {
		case "traceparent":
			traceparent = string(header.Value)
		case "tracestate":
			tracestate = string(header.Value)
		}
	}
	return telemetry.ExtractTrace(ctx, traceparent, tracestate)
}

func (w *worker) resumeDue(ctx context.Context) {
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for partition, blocked := range w.blocked {
		if blocked.permanent {
			if blocked.eventID == uuid.Nil {
				continue
			}
			event, err := w.runtime.store.Get(ctx, w.runtime.config.Consumer, blocked.eventID)
			if err != nil || event.Status == inbox.StatusDead {
				continue
			}
		} else if blocked.resumeAt.After(now) {
			continue
		}
		w.source.Resume(partition)
		w.runtime.metrics.AddConsumerPaused(w.runtime.config.Consumer, blocked.reason, -1)
		if blocked.reason == "dead" {
			w.runtime.metrics.AddConsumerDead(w.runtime.config.Consumer, -1)
		}
		delete(w.blocked, partition)
	}
}

func (w *worker) setActiveCancel(cancel context.CancelFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.activeCancel = cancel
}

func (w *worker) Assigned(partitions map[string][]int32) {
	w.runtime.metrics.IncConsumerRebalance(w.runtime.config.Consumer)
	w.runtime.logger.Info("consumer partitions assigned", "consumer", w.runtime.config.Consumer,
		"worker", w.id, "partitions", partitions)
}

func (w *worker) block(partition TopicPartition, blocked blockedPartition) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if existing, ok := w.blocked[partition]; ok {
		w.runtime.metrics.AddConsumerPaused(w.runtime.config.Consumer, existing.reason, -1)
		if existing.reason == "dead" {
			w.runtime.metrics.AddConsumerDead(w.runtime.config.Consumer, -1)
		}
	}
	w.blocked[partition] = blocked
	w.runtime.metrics.AddConsumerPaused(w.runtime.config.Consumer, blocked.reason, 1)
	w.runtime.metrics.RecordConsumerBackpressure(w.runtime.config.Consumer, consumerBackpressureReason(blocked.reason))
	if blocked.reason == "dead" {
		w.runtime.metrics.AddConsumerDead(w.runtime.config.Consumer, 1)
	}
}

func consumerBackpressureReason(reason string) string {
	switch reason {
	case "retry":
		return "retry_wait"
	case "dead", "protocol":
		return "dead"
	case "database", "offset_commit":
		return "database_slow"
	default:
		return "workers_saturated"
	}
}

func (w *worker) Revoked(partitions map[string][]int32) { w.releasePartitions(partitions) }

func (w *worker) Lost(partitions map[string][]int32) { w.releasePartitions(partitions) }

func (w *worker) RebalanceBlocked() {
	w.mu.Lock()
	cancel := w.activeCancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *worker) releasePartitions(partitions map[string][]int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for topic, values := range partitions {
		for _, partition := range values {
			key := TopicPartition{Topic: topic, Partition: partition}
			w.source.Resume(key)
			if blocked, ok := w.blocked[key]; ok {
				w.runtime.metrics.AddConsumerPaused(w.runtime.config.Consumer, blocked.reason, -1)
				if blocked.reason == "dead" {
					w.runtime.metrics.AddConsumerDead(w.runtime.config.Consumer, -1)
				}
			}
			delete(w.blocked, key)
		}
	}
}
