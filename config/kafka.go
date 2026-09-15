package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/emitlane/emitlane/broker/kafka"
)

// LoadKafkaSecurity reads the shared publisher/consumer EMITLANE_KAFKA_*
// security settings without requiring standalone relay or database settings.
// Files are read by the Kafka constructors, not by this function.
func LoadKafkaSecurity() (kafka.SecurityConfig, error) {
	enabled, err := envBool("EMITLANE_KAFKA_TLS_ENABLED", false)
	if err != nil {
		return kafka.SecurityConfig{}, fmt.Errorf("EMITLANE_KAFKA_TLS_ENABLED must be a boolean")
	}
	cfg := kafka.SecurityConfig{
		TLS: kafka.TLSConfig{
			Enabled:    enabled,
			CAFile:     os.Getenv("EMITLANE_KAFKA_TLS_CA_FILE"),
			CertFile:   os.Getenv("EMITLANE_KAFKA_TLS_CERT_FILE"),
			KeyFile:    os.Getenv("EMITLANE_KAFKA_TLS_KEY_FILE"),
			ServerName: strings.TrimSpace(os.Getenv("EMITLANE_KAFKA_TLS_SERVER_NAME")),
		},
		SASL: kafka.SASLConfig{
			Mechanism:    kafka.SASLMechanism(strings.ToUpper(strings.TrimSpace(os.Getenv("EMITLANE_KAFKA_SASL_MECHANISM")))),
			Username:     os.Getenv("EMITLANE_KAFKA_SASL_USERNAME"),
			Password:     os.Getenv("EMITLANE_KAFKA_SASL_PASSWORD"),
			PasswordFile: os.Getenv("EMITLANE_KAFKA_SASL_PASSWORD_FILE"),
		},
	}
	if err := cfg.Validate(); err != nil {
		return kafka.SecurityConfig{}, err
	}
	return cfg, nil
}
