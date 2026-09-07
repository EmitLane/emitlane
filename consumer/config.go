package consumer

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Consumer        string
	InstanceID      string
	Concurrency     int
	HandlerTimeout  time.Duration
	LeaseDuration   time.Duration
	MaintenancePoll time.Duration
	ShutdownTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		Concurrency:     1,
		HandlerTimeout:  30 * time.Second,
		LeaseDuration:   45 * time.Second,
		MaintenancePoll: 250 * time.Millisecond,
		ShutdownTimeout: 10 * time.Second,
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
	if c.HandlerTimeout <= 0 {
		return fmt.Errorf("consumer: handler timeout must be > 0")
	}
	if c.LeaseDuration <= c.HandlerTimeout {
		return fmt.Errorf("consumer: lease duration must exceed handler timeout")
	}
	if c.MaintenancePoll <= 0 || c.MaintenancePoll >= c.LeaseDuration {
		return fmt.Errorf("consumer: maintenance poll must be positive and shorter than lease duration")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("consumer: shutdown timeout must be > 0")
	}
	return nil
}
