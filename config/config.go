package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/emitlane/emitlane/relay"
)

// Config is the standalone process configuration, loaded from the environment.
type Config struct {
	DatabaseURL       string
	KafkaBrokers      []string
	KafkaClientID     string
	AutoCreateTopics  bool
	HTTPAddr          string
	LogLevel          string
	Relay             relay.Config
	DBMaxConns        int32
	DBMinConns        int32
	DBMaxConnLifetime time.Duration
}

// Load reads EMITLANE_* environment variables and applies defaults.
func Load() (Config, error) {
	var err error
	cfg := Config{
		DatabaseURL:       strings.TrimSpace(os.Getenv("EMITLANE_DATABASE_URL")),
		KafkaClientID:     envOr("EMITLANE_KAFKA_CLIENT_ID", "emitlane"),
		AutoCreateTopics:  false,
		HTTPAddr:          envOr("EMITLANE_HTTP_ADDR", ":8080"),
		LogLevel:          strings.ToLower(envOr("EMITLANE_LOG_LEVEL", "info")),
		Relay:             relay.DefaultConfig(),
		DBMaxConns:        10,
		DBMinConns:        2,
		DBMaxConnLifetime: time.Hour,
	}
	cfg.KafkaBrokers = splitCSV(os.Getenv("EMITLANE_KAFKA_BROKERS"))
	if cfg.AutoCreateTopics, err = envBool("EMITLANE_KAFKA_AUTO_CREATE_TOPICS", cfg.AutoCreateTopics); err != nil {
		return Config{}, err
	}
	if id := strings.TrimSpace(os.Getenv("EMITLANE_INSTANCE_ID")); id != "" {
		cfg.Relay.InstanceID = id
	} else {
		cfg.Relay.InstanceID = relay.NewInstanceID()
	}

	if cfg.Relay.BatchSize, err = envInt("EMITLANE_RELAY_BATCH_SIZE", cfg.Relay.BatchSize); err != nil {
		return Config{}, err
	}
	if cfg.Relay.Concurrency, err = envInt("EMITLANE_RELAY_CONCURRENCY", cfg.Relay.Concurrency); err != nil {
		return Config{}, err
	}
	if cfg.Relay.PollInterval, err = envDuration("EMITLANE_RELAY_POLL_INTERVAL", cfg.Relay.PollInterval); err != nil {
		return Config{}, err
	}
	if cfg.Relay.LeaseDuration, err = envDuration("EMITLANE_RELAY_LEASE_DURATION", cfg.Relay.LeaseDuration); err != nil {
		return Config{}, err
	}
	if cfg.Relay.MaxAttempts, err = envInt("EMITLANE_RETRY_MAX_ATTEMPTS", cfg.Relay.MaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.Relay.BaseDelay, err = envDuration("EMITLANE_RETRY_BASE_DELAY", cfg.Relay.BaseDelay); err != nil {
		return Config{}, err
	}
	if cfg.Relay.MaxDelay, err = envDuration("EMITLANE_RETRY_MAX_DELAY", cfg.Relay.MaxDelay); err != nil {
		return Config{}, err
	}
	if cfg.Relay.PublishTimeout, err = envDuration("EMITLANE_PUBLISH_TIMEOUT", cfg.Relay.PublishTimeout); err != nil {
		return Config{}, err
	}
	if cfg.Relay.ShutdownTimeout, err = envDuration("EMITLANE_SHUTDOWN_TIMEOUT", cfg.Relay.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.Relay.StatsInterval, err = envDuration("EMITLANE_STATS_INTERVAL", cfg.Relay.StatsInterval); err != nil {
		return Config{}, err
	}
	if cfg.Relay.Retention, err = envDuration("EMITLANE_RETENTION_DELIVERED", cfg.Relay.Retention); err != nil {
		return Config{}, err
	}
	if cfg.Relay.CleanupInterval, err = envDuration("EMITLANE_RETENTION_INTERVAL", cfg.Relay.CleanupInterval); err != nil {
		return Config{}, err
	}
	if cfg.Relay.CleanupBatch, err = envInt("EMITLANE_RETENTION_BATCH", cfg.Relay.CleanupBatch); err != nil {
		return Config{}, err
	}
	if n, err := envInt32("EMITLANE_DB_MAX_CONNS", cfg.DBMaxConns); err != nil {
		return Config{}, err
	} else {
		cfg.DBMaxConns = n
	}
	if n, err := envInt32("EMITLANE_DB_MIN_CONNS", cfg.DBMinConns); err != nil {
		return Config{}, err
	} else {
		cfg.DBMinConns = n
	}
	if cfg.DBMaxConnLifetime, err = envDuration("EMITLANE_DB_MAX_CONN_LIFETIME", cfg.DBMaxConnLifetime); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks required values and relay settings.
func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("EMITLANE_DATABASE_URL is required")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("EMITLANE_KAFKA_BROKERS is required")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("EMITLANE_HTTP_ADDR must not be empty")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("EMITLANE_LOG_LEVEL must be one of debug, info, warn, error")
	}
	if c.DBMaxConns < 1 {
		return fmt.Errorf("EMITLANE_DB_MAX_CONNS must be >= 1")
	}
	if c.DBMinConns < 0 || c.DBMinConns > c.DBMaxConns {
		return fmt.Errorf("EMITLANE_DB_MIN_CONNS must be between 0 and EMITLANE_DB_MAX_CONNS")
	}
	return c.Relay.Validate()
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s is not a valid integer: %q", key, raw)
	}
	return n, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	if raw == "0" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s is not a valid duration: %q", key, raw)
	}
	return d, nil
}

func envInt32(key string, fallback int32) (int32, error) {
	n, err := envInt(key, int(fallback))
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("%s is outside the supported 32-bit range: %d", key, n)
	}
	return int32(n), nil
}

func envBool(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s is not a valid boolean: %q", key, raw)
	}
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
