package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/emitlane/emitlane/broker"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	err := Config{}.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	cfg := Config{Brokers: []string{"localhost:9092"}, PublishTimeout: time.Second}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestClassifyPermanent(t *testing.T) {
	t.Parallel()
	err := classify(kerr.InvalidTopicException)
	if !broker.IsPermanent(err) {
		t.Fatalf("expected permanent, got %v", err)
	}
	err = classify(errors.New("connection reset"))
	if broker.IsPermanent(err) {
		t.Fatalf("network-like error should be retryable: %v", err)
	}
	err = classify(kerr.LeaderNotAvailable)
	if broker.IsPermanent(err) {
		t.Fatalf("leader election should be retryable: %v", err)
	}
	err = classify(kerr.MessageTooLarge)
	if !broker.IsPermanent(err) {
		t.Fatalf("oversized record should be permanent: %v", err)
	}
}
