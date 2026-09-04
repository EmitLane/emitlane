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
	t.Setenv("EMITLANE_ORDERING_REBALANCE_INTERVAL", "3s")
	t.Setenv("EMITLANE_ORDERING_LEASE_DURATION", "40s")
	t.Setenv("EMITLANE_ORDERING_SAFETY_MARGIN", "2s")
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
	if cfg.Relay.OrderingRebalanceInterval != 3*time.Second ||
		cfg.Relay.OrderingLeaseDuration != 40*time.Second || cfg.Relay.OrderingSafetyMargin != 2*time.Second {
		t.Fatalf("ordering timing: rebalance=%s lease=%s safety=%s",
			cfg.Relay.OrderingRebalanceInterval, cfg.Relay.OrderingLeaseDuration, cfg.Relay.OrderingSafetyMargin)
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

func TestLoadRejectsNonPositiveConnectionLifetime(t *testing.T) {
	for _, value := range []string{"0", "-1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
			t.Setenv("EMITLANE_KAFKA_BROKERS", "localhost:9092")
			t.Setenv("EMITLANE_DB_MAX_CONN_LIFETIME", value)
			if _, err := Load(); err == nil {
				t.Fatalf("expected %q to be rejected", value)
			}
		})
	}
}

func TestValidateAdminBindingSafety(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{"127.0.0.1:8081", "localhost:8081", "[::1]:8081"} {
		if err := ValidateAdmin(AdminConfig{Enabled: true, Addr: addr}); err != nil {
			t.Errorf("loopback %s: %v", addr, err)
		}
	}
	for _, addr := range []string{":8081", "0.0.0.0:8081", "[::]:8081", "admin.internal:8081"} {
		if err := ValidateAdmin(AdminConfig{Enabled: true, Addr: addr}); err == nil {
			t.Errorf("unsafe unauthenticated binding %s was accepted", addr)
		}
		if err := ValidateAdmin(AdminConfig{Enabled: true, Addr: addr, Token: "secret"}); err != nil {
			t.Errorf("authenticated binding %s: %v", addr, err)
		}
	}
}

func TestAdminIsDisabledByDefault(t *testing.T) {
	t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "localhost:9092")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Admin.Enabled {
		t.Fatal("admin listener must be disabled by default")
	}
}
