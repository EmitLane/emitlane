package admin

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/telemetry"
)

type Service struct {
	store      Store
	staleAfter time.Duration
	metrics    *telemetry.Metrics
}

func NewService(store Store, staleAfter time.Duration, metrics *telemetry.Metrics) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("admin: store is required")
	}
	if staleAfter <= 0 {
		return nil, fmt.Errorf("admin: stale threshold must be > 0")
	}
	return &Service{store: store, staleAfter: staleAfter, metrics: metrics}, nil
}

func (s *Service) Stats(ctx context.Context) (Stats, error) {
	return s.store.OperationalStats(ctx, s.staleAfter)
}

func (s *Service) ListEvents(ctx context.Context, filter EventFilter) (EventPage, error) {
	if err := normalizeEventFilter(&filter, false); err != nil {
		return EventPage{}, err
	}
	return s.store.ListEvents(ctx, filter)
}

func (s *Service) InspectEvent(ctx context.Context, id uuid.UUID, includeSensitive bool) (Event, error) {
	if id == uuid.Nil {
		return Event{}, fmt.Errorf("%w: event id is required", ErrInvalid)
	}
	return s.store.InspectEvent(ctx, id, includeSensitive)
}

func (s *Service) ListRelays(ctx context.Context) ([]RelayInstance, error) {
	return s.store.ListRelays(ctx, s.staleAfter)
}

func (s *Service) GetRelay(ctx context.Context, id string) (RelayInstance, error) {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 256 {
		return RelayInstance{}, fmt.Errorf("%w: relay id is required", ErrInvalid)
	}
	return s.store.GetRelay(ctx, id, s.staleAfter)
}

func (s *Service) ControlState(ctx context.Context) (ControlState, error) {
	return s.store.ControlState(ctx)
}

func (s *Service) SetPaused(ctx context.Context, paused bool, mutation Mutation) (ControlState, error) {
	mutation, err := normalizeMutation(ctx, mutation, false)
	if err != nil {
		return ControlState{}, err
	}
	action := "relay.resume"
	if paused {
		action = "relay.pause"
	}
	state, err := s.store.SetPaused(ctx, paused, mutation)
	s.metrics.IncAdminMutation(action, resultLabel(err))
	return state, err
}

func (s *Service) RetryDead(ctx context.Context, id uuid.UUID, mutation Mutation) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: event id is required", ErrInvalid)
	}
	mutation, err := normalizeMutation(ctx, mutation, false)
	if err != nil {
		return err
	}
	err = s.store.RetryDeadAudited(ctx, id, mutation)
	s.metrics.IncAdminMutation("event.retry", resultLabel(err))
	return err
}

func (s *Service) ReplayEvent(ctx context.Context, id uuid.UUID, mutation Mutation) (ReplayResult, error) {
	if id == uuid.Nil {
		return ReplayResult{}, fmt.Errorf("%w: event id is required", ErrInvalid)
	}
	mutation, err := normalizeMutation(ctx, mutation, true)
	if err != nil {
		return ReplayResult{}, err
	}
	result, err := s.store.ReplayEvent(ctx, id, mutation)
	s.metrics.IncAdminMutation("event.replay", resultLabel(err))
	if err == nil {
		s.metrics.RecordReplay(result.Created)
	}
	return result, err
}

func (s *Service) PreviewReplay(ctx context.Context, filter EventFilter) (ReplayPreview, error) {
	if err := normalizeEventFilter(&filter, true); err != nil {
		return ReplayPreview{}, err
	}
	return s.store.PreviewReplay(ctx, filter, 10, filter.Limit)
}

func (s *Service) ReplayBatch(ctx context.Context, filter EventFilter, mutation Mutation) (ReplayResult, error) {
	if err := normalizeEventFilter(&filter, true); err != nil {
		return ReplayResult{}, err
	}
	mutation, err := normalizeMutation(ctx, mutation, true)
	if err != nil {
		return ReplayResult{}, err
	}
	result, err := s.store.ReplayBatch(ctx, filter, mutation, filter.Limit)
	s.metrics.IncAdminMutation("replay.batch", resultLabel(err))
	if err == nil {
		s.metrics.RecordReplay(result.Created)
	}
	return result, err
}

func (s *Service) ListAudit(ctx context.Context, filter AuditFilter) (AuditPage, error) {
	filter.Action = strings.TrimSpace(filter.Action)
	if filter.Limit == 0 {
		filter.Limit = DefaultPageSize
	}
	if filter.Limit < 1 || filter.Limit > MaxPageSize {
		return AuditPage{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalid, MaxPageSize)
	}
	return s.store.ListAudit(ctx, filter)
}

func (s *Service) ListOrderingStreams(ctx context.Context, filter OrderingStreamFilter) (OrderingStreamPage, error) {
	filter.State = strings.ToLower(strings.TrimSpace(filter.State))
	filter.Destination = strings.TrimSpace(filter.Destination)
	if filter.State != "" && filter.State != "ready" && filter.State != "inflight" &&
		filter.State != "retry_wait" && filter.State != "gap" && filter.State != "dead_blocked" {
		return OrderingStreamPage{}, fmt.Errorf("%w: unsupported ordering state %q", ErrInvalid, filter.State)
	}
	if filter.Partition != nil && (*filter.Partition < 0 || *filter.Partition >= 64) {
		return OrderingStreamPage{}, fmt.Errorf("%w: partition must be between 0 and 63", ErrInvalid)
	}
	if filter.Limit == 0 {
		filter.Limit = DefaultPageSize
	}
	if filter.Limit < 1 || filter.Limit > MaxPageSize {
		return OrderingStreamPage{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalid, MaxPageSize)
	}
	return s.store.ListOrderingStreams(ctx, filter)
}

func (s *Service) InspectOrderingStream(ctx context.Context, destination, orderingKey string) (OrderingStream, error) {
	destination = strings.TrimSpace(destination)
	orderingKey = strings.TrimSpace(orderingKey)
	if destination == "" || orderingKey == "" {
		return OrderingStream{}, fmt.Errorf("%w: destination and ordering_key are required", ErrInvalid)
	}
	return s.store.InspectOrderingStream(ctx, destination, orderingKey)
}

func (s *Service) ListOrderingPartitions(ctx context.Context) ([]OrderingPartition, error) {
	return s.store.ListOrderingPartitions(ctx, s.staleAfter)
}

func normalizeEventFilter(filter *EventFilter, replay bool) error {
	filter.Destination = strings.TrimSpace(filter.Destination)
	filter.EventType = strings.TrimSpace(filter.EventType)
	if filter.CreatedFrom != nil && filter.CreatedTo != nil && filter.CreatedFrom.After(*filter.CreatedTo) {
		return fmt.Errorf("%w: created_from must not be after created_to", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(filter.Statuses))
	statuses := make([]string, 0, len(filter.Statuses))
	for _, status := range filter.Statuses {
		status = strings.ToLower(strings.TrimSpace(status))
		if status == "" {
			continue
		}
		allowed := status == "pending" || status == "inflight" || status == "delivered" || status == "dead"
		if !allowed || replay && status != "delivered" && status != "dead" {
			return fmt.Errorf("%w: unsupported status %q", ErrInvalid, status)
		}
		if _, ok := seen[status]; !ok {
			seen[status] = struct{}{}
			statuses = append(statuses, status)
		}
	}
	filter.Statuses = statuses
	if replay {
		if !filter.HasSelector() {
			return fmt.Errorf("%w: replay requires a non-empty selector", ErrInvalid)
		}
		if len(filter.Statuses) == 0 {
			filter.Statuses = []string{"delivered"}
		}
		if filter.Limit == 0 {
			filter.Limit = MaxReplayBatch
		}
		if filter.Limit < 1 || filter.Limit > MaxReplayBatch {
			return fmt.Errorf("%w: replay limit must be between 1 and %d", ErrInvalid, MaxReplayBatch)
		}
		return nil
	}
	if filter.Limit == 0 {
		filter.Limit = DefaultPageSize
	}
	if filter.Limit < 1 || filter.Limit > MaxPageSize {
		return fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalid, MaxPageSize)
	}
	return nil
}

func normalizeMutation(ctx context.Context, mutation Mutation, reasonRequired bool) (Mutation, error) {
	mutation.Actor = strings.TrimSpace(mutation.Actor)
	mutation.Reason = strings.TrimSpace(mutation.Reason)
	mutation.RequestID = strings.TrimSpace(mutation.RequestID)
	mutation.OrderingMode = strings.ToLower(strings.TrimSpace(mutation.OrderingMode))
	if mutation.Actor == "" || len(mutation.Actor) > 256 {
		return Mutation{}, fmt.Errorf("%w: actor is required", ErrInvalid)
	}
	if !safeText(mutation.Actor) {
		return Mutation{}, fmt.Errorf("%w: actor contains invalid characters", ErrInvalid)
	}
	if reasonRequired && mutation.Reason == "" {
		return Mutation{}, fmt.Errorf("%w: reason is required", ErrInvalid)
	}
	if len(mutation.Reason) > 4096 {
		return Mutation{}, fmt.Errorf("%w: reason is too long", ErrInvalid)
	}
	if !safeText(mutation.Reason) {
		return Mutation{}, fmt.Errorf("%w: reason contains invalid characters", ErrInvalid)
	}
	if len(mutation.RequestID) > 128 {
		return Mutation{}, fmt.Errorf("%w: request id is too long", ErrInvalid)
	}
	if !safeText(mutation.RequestID) {
		return Mutation{}, fmt.Errorf("%w: request id contains invalid characters", ErrInvalid)
	}
	if mutation.Traceparent == "" {
		mutation.Traceparent, mutation.Tracestate = telemetry.InjectTrace(ctx)
	}
	if mutation.OrderingMode != "" && mutation.OrderingMode != "unordered" {
		return Mutation{}, fmt.Errorf("%w: ordering_mode must be unordered", ErrInvalid)
	}
	return mutation, nil
}

func safeText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func resultLabel(err error) string {
	if err == nil {
		return "success"
	}
	return "failure"
}
