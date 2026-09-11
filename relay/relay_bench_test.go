package relay

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// BenchmarkContinuousRefill measures the scheduler working set with an
// in-memory durable-port double. It is an allocation regression signal; real
// PostgreSQL/Kafka capacity is measured by emitlane-bench separately.
func BenchmarkContinuousRefill(b *testing.B) {
	b.StopTimer()
	for range b.N {
		store := &memoryStore{events: make([]Event, 0, 128)}
		for range 128 {
			store.events = append(store.events, Event{
				ID: uuid.New(), Destination: "benchmark.events", Type: "benchmark.event",
				SchemaVersion: 1, CreatedAt: time.Now(),
			})
		}
		rly := testRelay(b, store, &trackingPublisher{}, func(cfg *Config) {
			cfg.BatchSize = 64
			cfg.Concurrency = 16
			cfg.PollInterval = time.Hour
			cfg.IdleBackoffMax = time.Hour
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		b.StartTimer()
		go func() { done <- rly.Run(ctx) }()
		deadline := time.After(time.Second)
		for {
			store.mu.Lock()
			delivered := len(store.delivered)
			store.mu.Unlock()
			if delivered == 128 {
				break
			}
			select {
			case <-deadline:
				b.Fatal("relay did not drain benchmark backlog")
			case <-time.After(time.Microsecond):
			}
		}
		cancel()
		if err := <-done; err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}
