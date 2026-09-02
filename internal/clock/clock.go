package clock

import "time"

// Clock is a testable source of the current time.
// It is internal and must not be exposed in the public API.
type Clock interface {
	Now() time.Time
}

// System returns the wall clock.
type System struct{}

func (System) Now() time.Time { return time.Now() }
