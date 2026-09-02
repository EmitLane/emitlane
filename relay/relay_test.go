package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emitlane/emitlane/broker"
)

type memoryStore struct {
	mu             sync.Mutex
	events         []Event
	claimLimits    []int
	attempts       map[uuid.UUID]int
	beginCalls     int
	beginErr       error
	delivered      []uuid.UUID
	retried        []uuid.UUID
	dead           []uuid.UUID
	cleanupInvoked int
}

type failingPresenceStore struct {
	*memoryStore
}

func (*failingPresenceStore) RegisterRelay(context.Context, RelayPresence) error {
	return errors.New("presence unavailable")
}

func (*failingPresenceStore) HeartbeatRelay(context.Context, string) error {
	return errors.New("presence unavailable")
}

func (*failingPresenceStore) MarkRelayStopped(context.Context, string) error {
	return errors.New("presence unavailable")
}

func (s *memoryStore) Claim(_ context.Context, owner string, limit int, _ time.Duration) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimLimits = append(s.claimLimits, limit)
	if len(s.events) == 0 {
		return nil, nil
	}
	n := min(limit, len(s.events))
	claimed := append([]Event(nil), s.events[:n]...)
	s.events = s.events[n:]
	for i := range claimed {
		claimed[i].LeaseOwner = owner
		claimed[i].Status = "inflight"
	}
	return claimed, nil
}

func (s *memoryStore) BeginAttempt(_ context.Context, id uuid.UUID, _ string, _ int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginCalls++
	if s.beginErr != nil {
		return 0, s.beginErr
	}
	if s.attempts == nil {
		s.attempts = make(map[uuid.UUID]int)
	}
	s.attempts[id]++
	return s.attempts[id], nil
}

func (s *memoryStore) MarkDelivered(_ context.Context, id uuid.UUID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered = append(s.delivered, id)
	return nil
}

func (s *memoryStore) MarkRetry(_ context.Context, id uuid.UUID, _ string, _ time.Duration, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retried = append(s.retried, id)
	return nil
}

func (s *memoryStore) MarkDead(_ context.Context, id uuid.UUID, _ string, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = append(s.dead, id)
	return nil
}

func (s *memoryStore) StatsSnapshot(context.Context) (Stats, error) { return Stats{}, nil }

func (s *memoryStore) CleanupDelivered(context.Context, time.Duration, int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupInvoked++
	return 0, nil
}

type trackingPublisher struct {
	mu        sync.Mutex
	active    int
	maxActive int
	messages  []broker.Message
	started   chan struct{}
	release   <-chan struct{}
	err       error
}

func (p *trackingPublisher) Publish(ctx context.Context, msg broker.Message) error {
	p.mu.Lock()
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.messages = append(p.messages, msg)
	started := p.started
	release := p.release
	p.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			p.finish()
			return ctx.Err()
		}
	}
	p.finish()
	return p.err
}

func (p *trackingPublisher) finish() {
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
}

func (p *trackingPublisher) Close() error { return nil }

func testRelay(t *testing.T, store Store, pub broker.Publisher, mutate func(*Config), opts ...Option) *Relay {
	t.Helper()
	cfg := DefaultConfig()
	cfg.InstanceID = "relay-test"
	cfg.PollInterval = 10 * time.Millisecond
	cfg.StatsInterval = 0
	cfg.Retention = 0
	if mutate != nil {
		mutate(&cfg)
	}
	opts = append(opts, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	rly, err := New(cfg, store, pub, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return rly
}

func TestPresenceDefaultsAreAppliedAfterOptions(t *testing.T) {
	rly := testRelay(t, &memoryStore{}, &trackingPublisher{}, nil, WithPresenceInfo("", ""))
	if rly.presence.Hostname == "" {
		t.Fatal("empty hostname option must fall back to a usable hostname")
	}
	if rly.presence.Version != "dev" {
		t.Fatalf("presence version %q, want dev", rly.presence.Version)
	}
	if rly.presence.InstanceID != rly.cfg.InstanceID {
		t.Fatalf("presence instance id %q, want %q", rly.presence.InstanceID, rly.cfg.InstanceID)
	}
}

func TestNewBackfillsV02ConfigFieldsForV01Callers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InstanceID = "v01-caller"
	cfg.ControlInterval = 0
	cfg.HeartbeatInterval = 0
	cfg.PresenceStaleAfter = 0

	rly, err := New(cfg, &memoryStore{}, &trackingPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	defaults := DefaultConfig()
	if rly.cfg.ControlInterval != defaults.ControlInterval ||
		rly.cfg.HeartbeatInterval != defaults.HeartbeatInterval ||
		rly.cfg.PresenceStaleAfter != defaults.PresenceStaleAfter {
		t.Fatalf("operational defaults not applied: %#v", rly.cfg)
	}
}

func TestTickClaimsOnlyWorkerCapacityAndDrainsBacklog(t *testing.T) {
	store := &memoryStore{}
	for range 5 {
		store.events = append(store.events, Event{
			ID:            uuid.Must(uuid.NewV7()),
			Destination:   "orders.events",
			Type:          "order.created",
			SchemaVersion: 1,
			CreatedAt:     time.Now(),
		})
	}
	pub := &trackingPublisher{}
	rly := testRelay(t, store, pub, func(cfg *Config) {
		cfg.BatchSize = 100
		cfg.Concurrency = 2
	})
	if err := rly.tick(context.Background(), context.Background()); err != nil {
		t.Fatal(err)
	}

	store.mu.Lock()
	limits := append([]int(nil), store.claimLimits...)
	delivered := len(store.delivered)
	store.mu.Unlock()
	if delivered != 5 {
		t.Fatalf("delivered %d events, want 5", delivered)
	}
	for _, limit := range limits {
		if limit != 2 {
			t.Fatalf("claim limit %d, want worker capacity 2", limit)
		}
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if pub.maxActive > 2 {
		t.Fatalf("publisher concurrency %d exceeds configured 2", pub.maxActive)
	}
	if len(pub.messages) != 5 {
		t.Fatalf("published %d messages, want 5", len(pub.messages))
	}
	for _, msg := range pub.messages {
		if msg.Headers[broker.HeaderAttempt] != "1" {
			t.Fatalf("attempt header %q, want 1", msg.Headers[broker.HeaderAttempt])
		}
	}
}

func TestPresenceFailureDoesNotStopDelivery(t *testing.T) {
	eventID := uuid.Must(uuid.NewV7())
	base := &memoryStore{events: []Event{{
		ID: eventID, Destination: "orders.events", Type: "order.created",
		SchemaVersion: 1, CreatedAt: time.Now(),
	}}}
	store := &failingPresenceStore{memoryStore: base}
	rly := testRelay(t, store, &trackingPublisher{}, func(cfg *Config) {
		cfg.HeartbeatInterval = 10 * time.Millisecond
		cfg.PresenceStaleAfter = 50 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := rly.Run(ctx); err != nil {
		t.Fatal(err)
	}
	base.mu.Lock()
	defer base.mu.Unlock()
	if len(base.delivered) != 1 || base.delivered[0] != eventID {
		t.Fatalf("presence failure blocked delivery: %v", base.delivered)
	}
}

func TestFailureAfterClaimDoesNotStartAttempt(t *testing.T) {
	store := &memoryStore{}
	pub := &trackingPublisher{}
	rly := testRelay(t, store, pub, nil, WithFailureHooks(FailureHooks{
		AfterClaimCommit: func(context.Context, Event) error { return errors.New("crash") },
	}))
	rly.handle(context.Background(), Event{ID: uuid.Must(uuid.NewV7()), Destination: "orders.events"})

	store.mu.Lock()
	beginCalls := store.beginCalls
	store.mu.Unlock()
	pub.mu.Lock()
	published := len(pub.messages)
	pub.mu.Unlock()
	if beginCalls != 0 || published != 0 {
		t.Fatalf("begin calls=%d published=%d, want zero", beginCalls, published)
	}
}

func TestRunDrainsStartedPublishOnCancellation(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	eventID := uuid.Must(uuid.NewV7())
	store := &memoryStore{events: []Event{{
		ID:            eventID,
		Destination:   "orders.events",
		Type:          "order.created",
		SchemaVersion: 1,
		CreatedAt:     time.Now(),
	}}}
	pub := &trackingPublisher{started: started, release: release}
	rly := testRelay(t, store, pub, func(cfg *Config) {
		cfg.ShutdownTimeout = time.Second
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rly.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("publish did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("relay stopped before publish drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not stop after publish drained")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.delivered) != 1 || store.delivered[0] != eventID {
		t.Fatalf("delivered events %v", store.delivered)
	}
}

func TestRunForcesPublishCancellationAfterShutdownTimeout(t *testing.T) {
	started := make(chan struct{}, 1)
	neverRelease := make(chan struct{})
	store := &memoryStore{events: []Event{{
		ID:            uuid.Must(uuid.NewV7()),
		Destination:   "orders.events",
		Type:          "order.created",
		SchemaVersion: 1,
		CreatedAt:     time.Now(),
	}}}
	pub := &trackingPublisher{started: started, release: neverRelease}
	rly := testRelay(t, store, pub, func(cfg *Config) {
		cfg.ShutdownTimeout = 50 * time.Millisecond
		cfg.PublishTimeout = 5 * time.Second
		cfg.LeaseDuration = 10 * time.Second
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rly.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("publish did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not enforce shutdown timeout")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.delivered) != 0 || len(store.retried) != 0 || len(store.dead) != 0 {
		t.Fatal("interrupted publish must remain inflight for lease recovery")
	}
}
