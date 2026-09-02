package relay

import (
	"sync"
	"testing"
	"time"
)

type seqRand struct {
	values []float64
	i      int
}

func (s *seqRand) Float64() float64 {
	if s.i >= len(s.values) {
		return 0
	}
	v := s.values[s.i]
	s.i++
	return v
}

func TestDelayBounds(t *testing.T) {
	t.Parallel()
	base := time.Second
	maxDelay := 30 * time.Minute
	for attempt := 1; attempt < 80; attempt++ {
		d := delay(attempt, base, maxDelay, &seqRand{values: []float64{0}})
		if d != 0 {
			t.Fatalf("jitter 0 must yield 0, attempt %d got %s", attempt, d)
		}
		d = delay(attempt, base, maxDelay, &seqRand{values: []float64{1}})
		if d > maxDelay {
			t.Fatalf("delay %s exceeds max at attempt %d", d, attempt)
		}
		if d < 0 {
			t.Fatalf("negative delay %s", d)
		}
	}
}

func TestDelayCapFormula(t *testing.T) {
	t.Parallel()
	base := time.Second
	maxDelay := 30 * time.Minute
	d := delay(1, base, maxDelay, &seqRand{values: []float64{1}})
	if d != time.Second {
		t.Fatalf("attempt 1 cap should be 1s, got %s", d)
	}
	d = delay(2, base, maxDelay, &seqRand{values: []float64{1}})
	if d != 2*time.Second {
		t.Fatalf("attempt 2 cap should be 2s, got %s", d)
	}
}

func TestDelayDeterministic(t *testing.T) {
	t.Parallel()
	a := delay(3, time.Second, time.Minute, &seqRand{values: []float64{0.5}})
	b := delay(3, time.Second, time.Minute, &seqRand{values: []float64{0.5}})
	if a != b {
		t.Fatalf("%s != %s", a, b)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.InstanceID = "test"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.BatchSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error")
	}
	cfg = DefaultConfig()
	cfg.InstanceID = "test"
	cfg.PublishTimeout = cfg.LeaseDuration
	if err := cfg.Validate(); err == nil {
		t.Fatal("publish timeout must be < lease")
	}
}

func TestLockedRandConcurrent(t *testing.T) {
	r := newLockedRand(1, 2)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				f := r.Float64()
				if f < 0 || f >= 1 {
					t.Errorf("out of range %f", f)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkDelay(b *testing.B) {
	r := newLockedRand(1, 2)
	b.ReportAllocs()
	for b.Loop() {
		_ = delay(4, time.Second, 30*time.Minute, r)
	}
}
