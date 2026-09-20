package config

import (
	"strings"
	"testing"

	"github.com/emitlane/emitlane/broker/kafka"
)

func clearKafkaSecurityEnv(t *testing.T) {
	t.Helper()
	for _, suffix := range []string{"TLS_ENABLED", "TLS_CA_FILE", "TLS_CERT_FILE", "TLS_KEY_FILE", "TLS_SERVER_NAME", "SASL_MECHANISM", "SASL_USERNAME", "SASL_PASSWORD", "SASL_PASSWORD_FILE"} {
		t.Setenv("EMITLANE_KAFKA_"+suffix, "")
	}
}

func TestLoadKafkaSecurityDefaults(t *testing.T) {
	clearKafkaSecurityEnv(t)
	t.Setenv("EMITLANE_DATABASE_URL", "")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "")
	cfg, err := LoadKafkaSecurity()
	if err != nil || cfg != (kafka.SecurityConfig{}) {
		t.Fatalf("expected standalone zero security configuration: %v", err)
	}
}

func TestLoadKafkaSecurityPreservesCredentials(t *testing.T) {
	clearKafkaSecurityEnv(t)
	t.Setenv("EMITLANE_KAFKA_TLS_ENABLED", "true")
	t.Setenv("EMITLANE_KAFKA_TLS_CA_FILE", "/mounted/ca.pem")
	t.Setenv("EMITLANE_KAFKA_TLS_CERT_FILE", "/mounted/client.pem")
	t.Setenv("EMITLANE_KAFKA_TLS_KEY_FILE", "/mounted/client.key")
	t.Setenv("EMITLANE_KAFKA_TLS_SERVER_NAME", " broker.example ")
	t.Setenv("EMITLANE_KAFKA_SASL_MECHANISM", " scram-sha-512 ")
	t.Setenv("EMITLANE_KAFKA_SASL_USERNAME", " service ")
	t.Setenv("EMITLANE_KAFKA_SASL_PASSWORD", " pass word ")
	cfg, err := LoadKafkaSecurity()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.TLS.Enabled || cfg.TLS.CAFile != "/mounted/ca.pem" || cfg.TLS.CertFile != "/mounted/client.pem" || cfg.TLS.KeyFile != "/mounted/client.key" || cfg.TLS.ServerName != "broker.example" {
		t.Fatal("TLS settings were not loaded")
	}
	if cfg.SASL.Mechanism != kafka.SASLSCRAMSHA512 || cfg.SASL.Username != " service " || cfg.SASL.Password != " pass word " {
		t.Fatal("SASL settings changed credential bytes")
	}
	// File existence is checked once by client construction, not by the loader.
	t.Setenv("EMITLANE_KAFKA_SASL_PASSWORD", "")
	t.Setenv("EMITLANE_KAFKA_SASL_PASSWORD_FILE", "/mounted/password")
	cfg, err = LoadKafkaSecurity()
	if err != nil || cfg.SASL.PasswordFile != "/mounted/password" {
		t.Fatalf("password file configuration: %v", err)
	}
}

func TestLoadKafkaSecurityRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"invalid boolean", map[string]string{"TLS_ENABLED": "private-value"}},
		{"disabled TLS", map[string]string{"TLS_CA_FILE": "/private-value/ca"}},
		{"unpaired key", map[string]string{"TLS_ENABLED": "true", "TLS_KEY_FILE": "/private-value/key"}},
		{"unknown mechanism", map[string]string{"TLS_ENABLED": "true", "SASL_MECHANISM": "private-value"}},
		{"unused credentials", map[string]string{"SASL_PASSWORD": "private-value"}},
		{"SASL without TLS", map[string]string{"SASL_MECHANISM": "PLAIN", "SASL_USERNAME": "user", "SASL_PASSWORD": "private-value"}},
		{"missing password", map[string]string{"TLS_ENABLED": "true", "SASL_MECHANISM": "PLAIN", "SASL_USERNAME": "private-value"}},
		{"two sources", map[string]string{"TLS_ENABLED": "true", "SASL_MECHANISM": "PLAIN", "SASL_USERNAME": "user", "SASL_PASSWORD": "private-value", "SASL_PASSWORD_FILE": "/private-value/password"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearKafkaSecurityEnv(t)
			for suffix, value := range tc.env {
				t.Setenv("EMITLANE_KAFKA_"+suffix, value)
			}
			_, err := LoadKafkaSecurity()
			if err == nil || strings.Contains(err.Error(), "private-value") {
				t.Fatal("expected a configuration error without secret values")
			}
		})
	}
}

func TestLoadIncludesKafkaSecurity(t *testing.T) {
	clearKafkaSecurityEnv(t)
	t.Setenv("EMITLANE_DATABASE_URL", "postgres://localhost/emitlane")
	t.Setenv("EMITLANE_KAFKA_BROKERS", "localhost:9093")
	t.Setenv("EMITLANE_KAFKA_TLS_ENABLED", "true")
	cfg, err := Load()
	if err != nil || !cfg.KafkaSecurity.TLS.Enabled {
		t.Fatalf("standalone security configuration: %v", err)
	}
	cfg.KafkaSecurity.SASL.Password = "unused"
	if cfg.Validate() == nil {
		t.Fatal("programmatic standalone configuration bypassed validation")
	}
}
