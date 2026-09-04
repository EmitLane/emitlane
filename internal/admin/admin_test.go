package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type stubStore struct {
	stats            Stats
	event            Event
	lastReplayFilter EventFilter
}

func (s *stubStore) OperationalStats(context.Context, time.Duration) (Stats, error) {
	return s.stats, nil
}
func (s *stubStore) ListEvents(context.Context, EventFilter) (EventPage, error) {
	return EventPage{Events: []Event{s.event}}, nil
}
func (s *stubStore) InspectEvent(_ context.Context, _ uuid.UUID, sensitive bool) (Event, error) {
	event := s.event
	if sensitive {
		payload := "c2VjcmV0"
		event.PayloadBase64 = &payload
		event.Headers = map[string]string{"secret": "value"}
	}
	return event, nil
}
func (s *stubStore) ListRelays(context.Context, time.Duration) ([]RelayInstance, error) {
	return nil, nil
}
func (s *stubStore) GetRelay(context.Context, string, time.Duration) (RelayInstance, error) {
	return RelayInstance{}, ErrNotFound
}
func (s *stubStore) ControlState(context.Context) (ControlState, error) {
	return ControlState{}, nil
}
func (s *stubStore) SetPaused(_ context.Context, paused bool, _ Mutation) (ControlState, error) {
	return ControlState{Paused: paused}, nil
}
func (s *stubStore) RetryDeadAudited(context.Context, uuid.UUID, Mutation) error { return nil }
func (s *stubStore) ReplayEvent(context.Context, uuid.UUID, Mutation) (ReplayResult, error) {
	return ReplayResult{Created: 1}, nil
}
func (s *stubStore) PreviewReplay(_ context.Context, filter EventFilter, _, executionLimit int) (ReplayPreview, error) {
	s.lastReplayFilter = filter
	return ReplayPreview{Count: 1, Limit: executionLimit}, nil
}
func (s *stubStore) ReplayBatch(context.Context, EventFilter, Mutation, int) (ReplayResult, error) {
	return ReplayResult{Created: 1}, nil
}
func (s *stubStore) ListAudit(context.Context, AuditFilter) (AuditPage, error) {
	return AuditPage{}, nil
}
func (s *stubStore) ListOrderingStreams(context.Context, OrderingStreamFilter) (OrderingStreamPage, error) {
	return OrderingStreamPage{Streams: []OrderingStream{{Destination: "orders", OrderingKey: "order:1", State: "gap"}}}, nil
}
func (s *stubStore) InspectOrderingStream(context.Context, string, string) (OrderingStream, error) {
	return OrderingStream{Destination: "orders", OrderingKey: "order:1", State: "gap"}, nil
}
func (s *stubStore) ListOrderingPartitions(context.Context, time.Duration) ([]OrderingPartition, error) {
	return []OrderingPartition{{PartitionID: 1, State: "owned"}}, nil
}

func newTestService(t *testing.T, store Store) *Service {
	t.Helper()
	service, err := NewService(store, 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestCursorRoundTripAndValidation(t *testing.T) {
	t.Parallel()
	want := Cursor{CreatedAt: time.Now().UTC().Truncate(time.Microsecond), ID: uuid.New()}
	raw, err := EncodeCursor(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCursor(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) || got.ID != want.ID {
		t.Fatalf("cursor round trip: got=%+v want=%+v", got, want)
	}
	if _, err := DecodeCursor("not-base64***"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed cursor error = %v", err)
	}
}

func TestReplayRequiresSelectorAndDefaultsToDelivered(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	service := newTestService(t, store)
	if _, err := service.PreviewReplay(context.Background(), EventFilter{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty selector error = %v", err)
	}
	if _, err := service.PreviewReplay(context.Background(), EventFilter{Destination: "orders"}); err != nil {
		t.Fatal(err)
	}
	if len(store.lastReplayFilter.Statuses) != 1 || store.lastReplayFilter.Statuses[0] != "delivered" {
		t.Fatalf("default replay statuses = %v", store.lastReplayFilter.Statuses)
	}
	if _, err := service.PreviewReplay(context.Background(), EventFilter{Destination: "orders", Statuses: []string{"pending"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("pending replay error = %v", err)
	}
}

func TestAdminBearerAuthAndRequestID(t *testing.T) {
	t.Parallel()
	store := &stubStore{stats: Stats{Pending: 2}}
	handler := newHandler(newTestService(t, store), "correct-token", false, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	req.Header.Set("X-Request-ID", "incident-42")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusUnauthorized || response.Header().Get("X-Request-ID") != "incident-42" {
		t.Fatalf("unauthorized response: code=%d request-id=%q", response.Code, response.Header().Get("X-Request-ID"))
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-token code=%d", response.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	req.Header.Set("Authorization", "Bearer correct-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("authorized code=%d body=%s", response.Code, response.Body.String())
	}
	var stats Stats
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil || stats.Pending != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestPayloadExposureRequiresServerOptIn(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	store := &stubStore{event: Event{ID: id, Status: "delivered"}}
	service := newTestService(t, store)

	handler := newHandler(service, "", false, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/events/"+id.String(), nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "c2VjcmV0") || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("default inspection exposed sensitive data: code=%d body=%s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/events/"+id.String()+"?payload=true", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("payload-disabled code=%d body=%s", response.Code, response.Body.String())
	}

	handler = newHandler(service, "", true, nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/events/"+id.String()+"?payload=true", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "c2VjcmV0") {
		t.Fatalf("payload-enabled code=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminRejectsUnknownMutationFields(t *testing.T) {
	t.Parallel()
	handler := newHandler(newTestService(t, &stubStore{}), "", false, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/relay/pause", strings.NewReader(`{"reason":"ok","unexpected":true}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown-field code=%d body=%s", response.Code, response.Body.String())
	}
}

func TestOrderingRoutesAndCursorValidation(t *testing.T) {
	t.Parallel()
	handler := newHandler(newTestService(t, &stubStore{}), "", false, nil)
	for _, path := range []string{
		"/v1/ordering/streams?blocked_only=true",
		"/v1/ordering/stream?destination=orders&ordering_key=order%3A1",
		"/v1/ordering/partitions",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s code=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	cursor, err := EncodeOrderingStreamCursor(OrderingStreamCursor{Destination: "orders", OrderingKey: "order:1"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeOrderingStreamCursor(cursor)
	if err != nil || decoded.Destination != "orders" || decoded.OrderingKey != "order:1" {
		t.Fatalf("ordering cursor=%+v err=%v", decoded, err)
	}
}
