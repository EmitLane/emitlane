package relay

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// delay returns a full-jitter exponential backoff:
//
//	cap = min(maxDelay, baseDelay * 2^(attempt-1))
//	delay = random(0, cap)
//
// attempt is the one-based number of broker publish attempts already started.
// Overflow of the exponent is avoided.
func delay(attempt int, base, maxDelay time.Duration, rnd randSource) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	capDelay := cappedExp(attempt-1, base, maxDelay)
	if capDelay <= 0 {
		return 0
	}
	f := 0.0
	if rnd != nil {
		f = rnd.Float64()
	}
	if f < 0 {
		f = 0
	}
	if f > 1 {
		f = 1
	}
	return time.Duration(f * float64(capDelay))
}

func cappedExp(attempt int, base, maxDelay time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if maxDelay < base {
		return maxDelay
	}
	if attempt <= 0 {
		if base > maxDelay {
			return maxDelay
		}
		return base
	}
	if attempt > 62 {
		return maxDelay
	}
	// cap = min(maxDelay, base * 2^attempt) without overflowing int64.
	maxFactor := float64(maxDelay) / float64(base)
	pow := math.Ldexp(1, attempt) // 2^attempt
	if pow >= maxFactor {
		return maxDelay
	}
	return time.Duration(float64(base) * pow)
}

// randSource is a source of jitter in [0, 1).
type randSource interface {
	Float64() float64
}

// lockedRand wraps rand/v2.Rand for concurrent workers.
type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newLockedRand(seed1, seed2 uint64) *lockedRand {
	return &lockedRand{r: rand.New(rand.NewPCG(seed1, seed2))}
}

func (l *lockedRand) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}
