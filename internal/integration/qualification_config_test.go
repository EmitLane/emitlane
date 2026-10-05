//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

type qualificationConfig struct {
	duration, interval time.Duration
	rate               int
	seed               int64
	output             string
}

func TestQualificationConfigRejectsUnsafeOrIncompleteRuns(t *testing.T) {
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "3m")
	t.Setenv("EMITLANE_QUALIFICATION_OUTPUT", "/tmp/qualification-test")
	t.Setenv("EMITLANE_QUALIFICATION_INTERVAL", "")
	t.Setenv("EMITLANE_QUALIFICATION_RATE", "")
	t.Setenv("EMITLANE_QUALIFICATION_SEED", "")
	if c, err := readQualificationConfig(); err != nil || c.interval != time.Minute || c.rate != 10 {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"EMITLANE_QUALIFICATION_DURATION", "73h"}, {"EMITLANE_QUALIFICATION_DURATION", "1m"},
		{"EMITLANE_QUALIFICATION_OUTPUT", ""}, {"EMITLANE_QUALIFICATION_INTERVAL", "1s"},
		{"EMITLANE_QUALIFICATION_RATE", "100"}, {"EMITLANE_QUALIFICATION_SEED", "invalid"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := readQualificationConfig(); err == nil {
				t.Fatal("invalid qualification accepted")
			}
		})
	}
}

func readQualificationConfig() (qualificationConfig, error) {
	c := qualificationConfig{rate: 10, seed: 20261005, output: os.Getenv("EMITLANE_QUALIFICATION_OUTPUT")}
	var err error
	c.duration, err = time.ParseDuration(os.Getenv("EMITLANE_QUALIFICATION_DURATION"))
	if err != nil || c.duration < 2*time.Minute || c.duration > 72*time.Hour {
		return c, fmt.Errorf("qualification duration must be between 2m and 72h")
	}
	c.interval = min(5*time.Minute, c.duration/3)
	if raw := os.Getenv("EMITLANE_QUALIFICATION_INTERVAL"); raw != "" {
		c.interval, err = time.ParseDuration(raw)
		if err != nil || c.interval < 30*time.Second || c.interval > c.duration/2 {
			return c, fmt.Errorf("qualification interval must be between 30s and half the duration")
		}
	}
	if raw := os.Getenv("EMITLANE_QUALIFICATION_RATE"); raw != "" {
		c.rate, err = strconv.Atoi(raw)
		if err != nil || c.rate < 1 || c.rate > 50 {
			return c, fmt.Errorf("qualification rate must be between 1 and 50 events/s")
		}
	}
	if raw := os.Getenv("EMITLANE_QUALIFICATION_SEED"); raw != "" {
		c.seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return c, fmt.Errorf("qualification seed: %w", err)
		}
	}
	if c.output == "" {
		return c, fmt.Errorf("EMITLANE_QUALIFICATION_OUTPUT is required for durable evidence")
	}
	return c, nil
}
