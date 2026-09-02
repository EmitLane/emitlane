package relay

import (
	"fmt"
	"strings"
	"time"
)

// Config controls claim, concurrency, polling, leases and retry.
type Config struct {
	BatchSize     int
	Concurrency   int
	PollInterval  time.Duration
	LeaseDuration time.Duration

	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration

	PublishTimeout     time.Duration
	ShutdownTimeout    time.Duration
	StatsInterval      time.Duration
	ControlInterval    time.Duration
	HeartbeatInterval  time.Duration
	PresenceStaleAfter time.Duration
	CleanupInterval    time.Duration
	Retention          time.Duration
	CleanupBatch       int

	OrderingRebalanceInterval time.Duration
	OrderingLeaseDuration     time.Duration
	OrderingSafetyMargin      time.Duration

	InstanceID string
}

// DefaultConfig returns production-oriented defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:                 100,
		Concurrency:               4,
		PollInterval:              5 * time.Second,
		LeaseDuration:             30 * time.Second,
		MaxAttempts:               10,
		BaseDelay:                 time.Second,
		MaxDelay:                  30 * time.Minute,
		PublishTimeout:            10 * time.Second,
		ShutdownTimeout:           15 * time.Second,
		StatsInterval:             5 * time.Second,
		ControlInterval:           2 * time.Second,
		HeartbeatInterval:         10 * time.Second,
		PresenceStaleAfter:        30 * time.Second,
		CleanupInterval:           time.Minute,
		Retention:                 7 * 24 * time.Hour,
		CleanupBatch:              1000,
		OrderingRebalanceInterval: 2 * time.Second,
		OrderingLeaseDuration:     30 * time.Second,
		OrderingSafetyMargin:      time.Second,
	}
}

// Validate rejects nonsensical values.
func (c Config) Validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("relay: batch size must be > 0")
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("relay: concurrency must be > 0")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("relay: poll interval must be > 0")
	}
	if c.LeaseDuration <= 0 {
		return fmt.Errorf("relay: lease duration must be > 0")
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("relay: max attempts must be >= 1")
	}
	if c.BaseDelay <= 0 {
		return fmt.Errorf("relay: base delay must be > 0")
	}
	if c.MaxDelay < c.BaseDelay {
		return fmt.Errorf("relay: max delay must be >= base delay")
	}
	if c.PublishTimeout <= 0 {
		return fmt.Errorf("relay: publish timeout must be > 0")
	}
	if c.PublishTimeout >= c.LeaseDuration {
		return fmt.Errorf("relay: publish timeout must be < lease duration")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("relay: shutdown timeout must be > 0")
	}
	if c.StatsInterval < 0 {
		return fmt.Errorf("relay: stats interval must be >= 0")
	}
	if c.ControlInterval <= 0 {
		return fmt.Errorf("relay: control interval must be > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("relay: heartbeat interval must be > 0")
	}
	if c.PresenceStaleAfter <= c.HeartbeatInterval {
		return fmt.Errorf("relay: presence stale threshold must be greater than heartbeat interval")
	}
	if c.CleanupInterval < 0 {
		return fmt.Errorf("relay: cleanup interval must be >= 0")
	}
	if c.Retention < 0 {
		return fmt.Errorf("relay: delivered retention must be >= 0")
	}
	if strings.TrimSpace(c.InstanceID) == "" {
		return fmt.Errorf("relay: instance id is required")
	}
	if c.InstanceID != strings.TrimSpace(c.InstanceID) {
		return fmt.Errorf("relay: instance id must not have surrounding whitespace")
	}
	if c.CleanupBatch < 0 {
		return fmt.Errorf("relay: cleanup batch must be >= 0")
	}
	if c.Retention > 0 && c.CleanupInterval == 0 {
		return fmt.Errorf("relay: cleanup interval must be > 0 when retention is enabled")
	}
	if c.Retention > 0 && c.CleanupBatch == 0 {
		return fmt.Errorf("relay: cleanup batch must be > 0 when retention is enabled")
	}
	if c.OrderingRebalanceInterval <= 0 {
		return fmt.Errorf("relay: ordering rebalance interval must be > 0")
	}
	if c.OrderingLeaseDuration <= 0 {
		return fmt.Errorf("relay: ordering lease duration must be > 0")
	}
	if c.OrderingSafetyMargin <= 0 {
		return fmt.Errorf("relay: ordering safety margin must be > 0")
	}
	if c.OrderingLeaseDuration <= c.PublishTimeout+c.OrderingSafetyMargin {
		return fmt.Errorf("relay: ordering lease duration must exceed publish timeout plus safety margin")
	}
	if c.OrderingRebalanceInterval >= c.OrderingLeaseDuration {
		return fmt.Errorf("relay: ordering rebalance interval must be less than ordering lease duration")
	}
	return nil
}
