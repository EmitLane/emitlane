package outbox

import "errors"

// ErrInvalidEvent is returned when an event fails validation before enqueue.
var ErrInvalidEvent = errors.New("outbox: invalid event")
