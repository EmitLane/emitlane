package postgres

import (
	"context"
	"time"

	"github.com/emitlane/emitlane/integrity"
)

const (
	adminIntegrityMaxFindings = 50
	adminIntegrityTimeout     = 5 * time.Second
)

// IntegritySummary exposes the fixed, bounded Admin API verification profile.
// Full scans remain a CLI-only operator action.
func (s *Store) IntegritySummary(ctx context.Context, staleAfter time.Duration) (integrity.Report, error) {
	config := integrity.DefaultConfig()
	config.MaxFindings = adminIntegrityMaxFindings
	config.StatementTimeout = adminIntegrityTimeout
	config.PresenceStaleAfter = staleAfter
	verifier, err := integrity.NewVerifier(s.pool, config)
	if err != nil {
		return integrity.Report{}, err
	}
	return verifier.Check(ctx, integrity.ModeSummary)
}
