package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
)

// Config configures the Kafka publisher.
type Config struct {
	Brokers          []string
	ClientID         string
	PublishTimeout   time.Duration
	AutoCreateTopics bool
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: at least one broker is required")
	}
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return errors.New("kafka: broker address must not be empty")
		}
	}
	if c.PublishTimeout <= 0 {
		return errors.New("kafka: publish timeout must be > 0")
	}
	return nil
}

// Publisher publishes records with franz-go and waits for produce results.
//
// EmitLane owns retry/backoff. The client is configured with required acks=all,
// idempotent producing, and no additional record retries so a failed ProduceSync
// becomes one EmitLane attempt.
type Publisher struct {
	client *kgo.Client
}

// NewPublisher constructs a Kafka publisher.
func NewPublisher(cfg Config) (*Publisher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	brokers := make([]string, len(cfg.Brokers))
	for i, brokerAddress := range cfg.Brokers {
		brokers[i] = strings.TrimSpace(brokerAddress)
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "emitlane"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(0),
		kgo.RecordDeliveryTimeout(cfg.PublishTimeout),
		kgo.ProduceRequestTimeout(cfg.PublishTimeout),
		kgo.AllowIdempotentProduceCancellation(),
		kgo.ProducerLinger(0),
		kgo.UnknownTopicRetries(0),
	}
	if cfg.AutoCreateTopics {
		opts = append(opts,
			kgo.AllowAutoTopicCreation(),
			// A short metadata retry so auto-created topics exist before
			// ProduceSync returns. EmitLane still owns longer backoff.
			kgo.UnknownTopicRetries(4),
		)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client: %w", err)
	}
	return &Publisher{client: client}, nil
}

// Publish waits for the broker to acknowledge the record before returning.
func (p *Publisher) Publish(ctx context.Context, message broker.Message) error {
	if p == nil || p.client == nil {
		return errors.New("kafka: publisher is closed")
	}
	headers := make([]kgo.RecordHeader, 0, len(message.Headers))
	for k, v := range message.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	record := &kgo.Record{
		Topic:   message.Destination,
		Key:     message.Key,
		Value:   message.Payload,
		Headers: headers,
	}
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return classify(err)
	}
	return nil
}

// Ping checks that at least one broker is reachable.
func (p *Publisher) Ping(ctx context.Context) error {
	if p == nil || p.client == nil {
		return errors.New("kafka: publisher is closed")
	}
	return p.client.Ping(ctx)
}

// Close flushes and closes the client.
func (p *Publisher) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	p.client.Close()
	return nil
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if isPermanent(err) {
		return fmt.Errorf("%w: %w", broker.ErrPermanent, err)
	}
	return err
}

func isPermanent(err error) bool {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		switch ke.Code {
		case kerr.InvalidTopicException.Code,
			kerr.MessageTooLarge.Code,
			kerr.RecordListTooLarge.Code,
			kerr.InvalidRecord.Code,
			kerr.PolicyViolation.Code,
			kerr.TopicAuthorizationFailed.Code,
			kerr.ClusterAuthorizationFailed.Code,
			kerr.TransactionalIDAuthorizationFailed.Code,
			kerr.UnsupportedVersion.Code,
			kerr.UnsupportedForMessageFormat.Code:
			return true
		}
	}
	return false
}
