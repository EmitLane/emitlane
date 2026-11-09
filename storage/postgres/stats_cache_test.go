package postgres

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emitlane/emitlane/relay"
)

func TestStatsCacheExpiryAndFailedRefresh(t *testing.T) {
	var cache statsSnapshotCache
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	calls := 0
	var failure error
	load := func(context.Context) (relay.Stats, error) {
		calls++
		return relay.Stats{Pending: int64(calls)}, failure
	}
	first, captured, err := cache.snapshot(context.Background(), clock, load)
	if err != nil || first.Pending != 1 || !captured.Equal(now) {
		t.Fatalf("first snapshot=%+v at=%s err=%v", first, captured, err)
	}
	now = now.Add(statsSnapshotReuse - time.Nanosecond)
	value, at, err := cache.snapshot(context.Background(), clock, load)
	if err != nil || value != first || at != captured || calls != 1 {
		t.Fatalf("cache hit changed its source timestamp or loaded again: %+v %s %v calls=%d", value, at, err, calls)
	}
	now = now.Add(time.Nanosecond)
	failure = errors.New("database unavailable")
	value, at, err = cache.snapshot(context.Background(), clock, load)
	if !errors.Is(err, failure) || !at.IsZero() || value != (relay.Stats{}) {
		t.Fatal("failed refresh was returned as successful data")
	}
	if cache.value != first || cache.capturedAt != captured || cache.expiresAt.After(now) {
		t.Fatal("failed refresh replaced or extended the previous observation")
	}
	failure = nil
	value, at, err = cache.snapshot(context.Background(), clock, load)
	if err != nil || value.Pending != 3 || at != now || calls != 3 {
		t.Fatalf("refresh did not recover: %+v %s %v calls=%d", value, at, err, calls)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := cache.snapshot(canceled, clock, load); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached data ignored caller cancellation: %v", err)
	}
}

func TestStatsCacheCoalescesConcurrentLoadsAndCancelsWaiters(t *testing.T) {
	var cache statsSnapshotCache
	var calls atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	load := func(context.Context) (relay.Stats, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return relay.Stats{Pending: 7}, nil
	}
	clock := func() time.Time { return time.Unix(1000, 0) }
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			value, at, err := cache.snapshot(context.Background(), clock, load)
			if err != nil || value.Pending != 7 || at != clock() {
				t.Errorf("concurrent observation=%+v at=%s err=%v", value, at, err)
			}
		}()
	}
	<-entered
	wait, cancel := context.WithCancel(context.Background())
	observedWait := &statsWaitContext{Context: wait, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, _, err := cache.snapshot(observedWait, clock, load); done <- err }()
	<-observedWait.waiting
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("canceled waiter=%v", err)
	}
	close(release)
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("duplicate full loads: %d", calls.Load())
	}
}

// Signal only once snapshot has entered the channel wait, not merely before
// its initial ctx.Err check. This makes cancellation exercise the waiter path.
type statsWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *statsWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
