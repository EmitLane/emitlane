package consumer

import (
	"math/rand"
	"time"
)

func retryDelay(attempt int, base, maximum time.Duration, jitter, sample float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	capDelay := base
	for current := 1; current < attempt && capDelay < maximum; current++ {
		if capDelay > maximum/2 {
			capDelay = maximum
			break
		}
		capDelay *= 2
	}
	if capDelay > maximum {
		capDelay = maximum
	}
	if sample < 0 {
		sample = 0
	}
	if sample > 1 {
		sample = 1
	}
	factor := 1 - jitter + jitter*sample
	return time.Duration(float64(capDelay) * factor)
}

func (r *Runtime) nextRetryDelay(attempt int) time.Duration {
	return retryDelay(attempt, r.config.BaseDelay, r.config.MaxDelay, r.config.Jitter, rand.Float64())
}
