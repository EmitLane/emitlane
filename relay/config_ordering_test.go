package relay

import "testing"

func TestConfigValidateOrderingTiming(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.OrderingLeaseDuration = cfg.PublishTimeout + cfg.OrderingSafetyMargin
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected ordering lease timing error")
	}
	cfg = DefaultConfig()
	cfg.OrderingRebalanceInterval = cfg.OrderingLeaseDuration
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected ordering rebalance timing error")
	}
}

func TestConfigValidateIdleBackoff(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.IdleBackoffMax = cfg.PollInterval - 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected idle backoff validation error")
	}
}
