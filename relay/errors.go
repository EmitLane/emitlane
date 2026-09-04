package relay

import "errors"

// ErrFenced means an ordered transition lost its event or partition authority.
// It is an expected concurrency outcome: stale work must be discarded without
// publishing or mutating durable stream state.
var ErrFenced = errors.New("relay: ordered transition fenced")
