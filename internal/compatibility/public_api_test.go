// Package compatibility_test exercises the SDK as an application would: it
// imports only public EmitLane packages and never opens a network connection.
package compatibility_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/emitlane/emitlane/broker"
	"github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/inbox"
	"github.com/emitlane/emitlane/integrity"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
	"github.com/emitlane/emitlane/storage/postgres"
	"github.com/emitlane/emitlane/telemetry"
)

var (
	_ broker.Publisher             = (*kafka.Publisher)(nil)
	_ consumer.SourceFactory       = (*kafka.ConsumerFactory)(nil)
	_ relay.Store                  = (*postgres.Store)(nil)
	_ relay.OrderedDeliveryStore   = (*postgres.Store)(nil)
	_ relay.OrderingPartitionStore = (*postgres.Store)(nil)
	_ relay.WakeupListener         = (*postgres.Listener)(nil)
	_ inbox.Store                  = (*postgres.InboxStore)(nil)
	_ inbox.OperatorStore          = (*postgres.InboxStore)(nil)
	_ error                        = postgres.ErrSchemaIncompatible
)

// ComposeApplication is deliberately not called: it verifies that a downstream
// application can wire the SDK without depending on internal types or issuing
// database/broker requests during this test suite.
func ComposeApplication(pool *pgxpool.Pool, publisher broker.Publisher, source consumer.SourceFactory) (*relay.Relay, *consumer.Runtime, error) {
	metrics, err := telemetry.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		return nil, nil, err
	}
	store, err := postgres.NewStore(pool)
	if err != nil {
		return nil, nil, err
	}
	relayConfig := relay.DefaultConfig()
	relayConfig.InstanceID = "compatibility-relay"
	runner, err := relay.New(relayConfig, store, publisher, relay.WithMetrics(metrics))
	if err != nil {
		return nil, nil, err
	}
	inboxStore, err := postgres.NewInboxStore(pool)
	if err != nil {
		return nil, nil, err
	}
	writer := outbox.NewWriter(outbox.WithMetrics(metrics))
	handler := consumer.Handler(func(ctx context.Context, tx pgx.Tx, message consumer.Message) error {
		payload, err := outbox.JSON(struct {
			SourceID string `json:"source_id"`
		}{SourceID: message.EventID.String()})
		if err != nil {
			return inbox.Permanent(err)
		}
		_, err = writer.Enqueue(ctx, tx, outbox.Event{
			Destination: "processed.events", Type: "event.processed",
			Key: message.Key, Payload: payload, CausationID: message.EventID.String(),
		})
		return err
	})
	consumerConfig := consumer.DefaultConfig()
	consumerConfig.Consumer = "compatibility-handler"
	consumerConfig.InstanceID = "compatibility-consumer"
	runtime, err := consumer.New(consumerConfig, pool, inboxStore, source, handler,
		consumer.WithMetrics(metrics), consumer.WithIdentityResolver(consumer.ResolveEventID))
	return runner, runtime, err
}

// Function values keep lifecycle and migration entry points in the compile
// contract without running their I/O or creating client background goroutines.
var (
	_ func(context.Context, pgx.Tx, string, string, func(context.Context, pgx.Tx) error) error = inbox.Process
	_ func(context.Context, pgx.Tx, string, string, func(context.Context, pgx.Tx) error) error = inbox.ProcessStrict
	_ func(*relay.Relay, context.Context) error                                                = (*relay.Relay).Run
	_ func(*consumer.Runtime, context.Context) error                                           = (*consumer.Runtime).Run
	_ func(context.Context, *pgxpool.Pool) error                                               = postgres.MigrateUp
	_ func(kafka.Config) (*kafka.Publisher, error)                                             = kafka.NewPublisher
	_ func(kafka.ConsumerConfig) (*kafka.ConsumerFactory, error)                               = kafka.NewConsumerFactory
	_ func(*pgxpool.Pool, integrity.Config) (*integrity.Verifier, error)                       = integrity.NewVerifier
)

func TestApplicationIdentityAndErrorHandling(t *testing.T) {
	id := uuid.MustParse("019535d9-e589-7000-8000-000000000001")
	message := consumer.Message{Headers: []consumer.Header{
		{Key: broker.HeaderEventID, Value: []byte(id.String())},
	}}
	resolved, err := consumer.ResolveEventID(message)
	if err != nil || resolved != id {
		t.Fatalf("resolve published identity: got %v, %v", resolved, err)
	}
	_, err = outbox.NewWriter().Enqueue(context.Background(), nil, outbox.Event{})
	if !errors.Is(err, outbox.ErrInvalidEvent) {
		t.Fatalf("application cannot classify invalid enqueue: %v", err)
	}
	cause := errors.New("business validation")
	failure := fmt.Errorf("handler: %w", inbox.Permanent(cause))
	if !inbox.IsPermanent(failure) || !errors.Is(failure, cause) {
		t.Fatalf("application cannot classify wrapped permanent failure: %v", failure)
	}
	if !broker.IsPermanent(fmt.Errorf("publish: %w", broker.ErrPermanent)) {
		t.Fatal("application cannot classify wrapped permanent publish failure")
	}
}

func TestApplicationSecurityConfiguration(t *testing.T) {
	security := kafka.SecurityConfig{
		TLS: kafka.TLSConfig{Enabled: true},
		SASL: kafka.SASLConfig{
			Mechanism: kafka.SASLSCRAMSHA256, Username: "application",
			PasswordFile: "/run/secrets/kafka-password",
		},
	}
	// Validate checks structure; no file must exist and no broker is contacted.
	if err := (kafka.Config{
		Brokers: []string{"broker.example:9093"}, PublishTimeout: 5 * time.Second,
		Security: security,
	}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (kafka.ConsumerConfig{
		Brokers: []string{"broker.example:9093"}, Topics: []string{"orders.events"},
		Group: "application", SessionTimeout: 10 * time.Second,
		RebalanceTimeout: 30 * time.Second, FetchMaxWait: 250 * time.Millisecond,
		Security: security,
	}).Validate(); err != nil {
		t.Fatal(err)
	}
}
