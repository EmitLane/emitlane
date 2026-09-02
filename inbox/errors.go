package inbox

import (
	"errors"
)

// ErrAlreadyProcessed is returned by ProcessStrict when the (consumer, event)
// pair was already recorded. Process treats this as success.
var ErrAlreadyProcessed = errors.New("inbox: event already processed")

// ErrInvalidRequest is returned when consumer or event ID is missing/invalid.
var ErrInvalidRequest = errors.New("inbox: invalid request")
