package config

import (
	"testing"
	"time"
)

func TestLoadRequiresDatabaseAndBrokers(t *testing.T) {
	t.Setenv("EMITLANE_DATABASE_URL", "")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("EMITLANE_RELAY_BATCH_SIZE", "50")
	t.Setenv("EMITLANE_RETRY_MAX_ATTEMPTS", "3")
	t.Setenv("EMITLANE_RELAY_POLL_INTERVAL", "2s")
	t.Setenv("EMITLANE_INSTANCE_ID", "relay-test")
	t.Setenv("EMITLANE_RETENTION_DELIVERED", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Relay.BatchSize != 50 {
		t.Fatalf("batch %d", cfg.Relay.BatchSize)
	}
	if cfg.Relay.MaxAttempts != 3 {
		t.Fatalf("attempts %d", cfg.Relay.MaxAttempts)
	}
	if cfg.Relay.PollInterval != 2*time.Second {
		t.Fatalf("poll %s", cfg.Relay.PollInterval)
	}
	if len(cfg.KafkaBrokers) != 2 {
		t.Fatalf("brokers %v", cfg.KafkaBrokers)
	}
	if cfg.Relay.InstanceID != "relay-test" {
		t.Fatalf("instance %s", cfg.Relay.InstanceID)
	}
	if cfg.Relay.Retention != 0 {
		t.Fatalf("retention %s", cfg.Relay.Retention)
	}
	if cfg.AutoCreateTopics {
		t.Fatal("topic auto-creation must be opt-in")
	}
}

func TestLoadRejectsBadDuration(t *testing.T) {
	t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("EMITLANE_RELAY_POLL_INTERVAL", "not-a-duration")
	t.Setenv("EMITLANE_INSTANCE_ID", "x")
	if _, err := Load(); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadRejectsBadBooleanAndLogLevel(t *testing.T) {
	t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("EMITLANE_KAFKA_AUTO_CREATE_TOPICS", "sometimes")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid boolean error")
	}

	t.Setenv("EMITLANE_KAFKA_AUTO_CREATE_TOPICS", "false")
	t.Setenv("EMITLANE_LOG_LEVEL", "verbose")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid log level error")
	}
}

func TestLoadRejectsConnectionCountOverflow(t *testing.T) {
	t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("EMITLANE_DB_MAX_CONNS", "4294967297")
	if _, err := Load(); err == nil {
		t.Fatal("expected connection count overflow error")
	}
}
