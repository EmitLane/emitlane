package outbox

import (
	"encoding/json"
	"fmt"
)

// JSON serializes v with the standard library encoder.
// It never panics; serialization failures are returned as errors.
func JSON(v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("outbox json: %w", err)
	}
	return payload, nil
}
