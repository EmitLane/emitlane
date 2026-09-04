package outbox

import "errors"

// ErrInvalidEvent is returned when an event fails validation before enqueue.
var ErrInvalidEvent = errors.New("outbox: invalid event")

// ErrDuplicateSequence reports that an event already occupies the sequence in
// the same destination-scoped ordered stream.
var ErrDuplicateSequence = errors.New("outbox: duplicate ordered sequence")

// ErrOrderingConflict reports incompatible durable stream metadata.
var ErrOrderingConflict = errors.New("outbox: ordering stream conflict")

// ErrSequenceAlreadyPassed reports an attempted insert below the stream's
// durable next sequence.
var ErrSequenceAlreadyPassed = errors.New("outbox: ordered sequence already passed")
