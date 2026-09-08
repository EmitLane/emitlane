package consumer

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Consumer           string
	InstanceID         string
	Concurrency        int
	MaxAttempts        int
	BaseDelay          time.Duration
	MaxDelay           time.Duration
	Jitter             float64
	HandlerTimeout     time.Duration
	LeaseDuration      time.Duration
	LeaseRenewInterval time.Duration
	MaintenancePoll    time.Duration
	ShutdownTimeout    time.Duration
}

func DefaultConfig() Config {
	return Config{
		Concurrency:        1,
		MaxAttempts:        10,
		BaseDelay:          time.Second,
		MaxDelay:           30 * time.Minute,
		Jitter:             1,
		HandlerTimeout:     2 * time.Minute,
		LeaseDuration:      30 * time.Second,
		LeaseRenewInterval: 10 * time.Second,
		MaintenancePoll:    250 * time.Millisecond,
		ShutdownTimeout:    10 * time.Second,
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Consumer) == "" {
		return fmt.Errorf("consumer: durable consumer name is required")
	}
	if strings.TrimSpace(c.InstanceID) == "" {
		return fmt.Errorf("consumer: instance ID is required")
	}
	if c.Concurrency < 1 || c.Concurrency > 128 {
		return fmt.Errorf("consumer: concurrency must be between 1 and 128")
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("consumer: max attempts must be >= 1")
	}
	if c.BaseDelay <= 0 || c.MaxDelay < c.BaseDelay {
		return fmt.Errorf("consumer: retry delays must be positive and max must be >= base")
	}
	if c.Jitter < 0 || c.Jitter > 1 {
		return fmt.Errorf("consumer: jitter must be between 0 and 1")
	}
	if c.HandlerTimeout <= 0 {
		return fmt.Errorf("consumer: handler timeout must be > 0")
	}
	if c.LeaseDuration <= 0 {
		return fmt.Errorf("consumer: lease duration must be > 0")
	}
	if c.LeaseRenewInterval <= 0 || c.LeaseRenewInterval*2 >= c.LeaseDuration {
		return fmt.Errorf("consumer: lease renew interval must be positive and less than half the lease duration")
	}
	if c.MaintenancePoll <= 0 || c.MaintenancePoll >= c.LeaseDuration {
		return fmt.Errorf("consumer: maintenance poll must be positive and shorter than lease duration")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("consumer: shutdown timeout must be > 0")
	}
	return nil
}
