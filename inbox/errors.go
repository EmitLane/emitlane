package inbox

import (
	"errors"
)

// ErrAlreadyProcessed is returned by ProcessStrict when the (consumer, event)
// pair was already recorded. Process treats this as success.
var ErrAlreadyProcessed = errors.New("inbox: event already processed")

// ErrInvalidRequest is returned when consumer or event ID is missing/invalid.
var ErrInvalidRequest = errors.New("inbox: invalid request")

// ErrLifecycleConflict is returned when legacy processing collides with an
// active managed lifecycle row or Kafka source coordinates identify a
// different event.
var ErrLifecycleConflict = errors.New("inbox: lifecycle conflict")

// ErrLeaseLost means an attempt no longer owns the durable lease token. Any
// business transaction associated with the stale attempt must be rolled back.
var ErrLeaseLost = errors.New("inbox: lease lost")

// ErrNotFound is returned when an Inbox identity does not exist.
var ErrNotFound = errors.New("inbox: event not found")
