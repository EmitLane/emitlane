package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/consumer"
)

type ConsumerConfig struct {
	Brokers          []string
	ClientID         string
	Group            string
	Topics           []string
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	FetchMaxWait     time.Duration
}

func (c ConsumerConfig) Validate() error {
	if len(c.Brokers) == 0 || len(c.Topics) == 0 {
		return errors.New("kafka consumer: brokers and topics are required")
	}
	for _, value := range append(append([]string{}, c.Brokers...), c.Topics...) {
		if strings.TrimSpace(value) == "" {
			return errors.New("kafka consumer: broker and topic values must not be blank")
		}
	}
	if strings.TrimSpace(c.Group) == "" {
		return errors.New("kafka consumer: group is required")
	}
	if c.SessionTimeout <= 0 || c.RebalanceTimeout <= 0 || c.FetchMaxWait <= 0 {
		return errors.New("kafka consumer: session, rebalance, and fetch timeouts must be positive")
	}
	return nil
}

// ConsumerFactory creates franz-go group members without exposing franz-go
// records to business handlers.
type ConsumerFactory struct {
	config ConsumerConfig
}

func NewConsumerFactory(config ConsumerConfig) (*ConsumerFactory, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &ConsumerFactory{config: config}, nil
}

func (f *ConsumerFactory) NewSource(workerID string, listener consumer.RebalanceListener) (consumer.Source, error) {
	clientID := strings.TrimSpace(f.config.ClientID)
	if clientID == "" {
		clientID = "emitlane-consumer"
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(f.config.Brokers...),
		kgo.ClientID(clientID+"-"+workerID),
		kgo.ConsumerGroup(f.config.Group),
		kgo.ConsumeTopics(f.config.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.SessionTimeout(f.config.SessionTimeout),
		kgo.RebalanceTimeout(f.config.RebalanceTimeout),
		kgo.FetchMaxWait(f.config.FetchMaxWait),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			listener.Assigned(partitions)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			listener.Revoked(partitions)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			listener.Lost(partitions)
		}),
		kgo.OnPartitionsCallbackBlocked(func(_ context.Context, _ *kgo.Client) {
			listener.RebalanceBlocked()
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: create client: %w", err)
	}
	return &consumerSource{client: client}, nil
}

type consumerSource struct {
	client *kgo.Client
}

func (s *consumerSource) Poll(ctx context.Context) (consumer.SourceRecord, error) {
	for {
		fetches := s.client.PollRecords(ctx, 1)
		if errs := fetches.Errors(); len(errs) > 0 {
			return consumer.SourceRecord{}, errs[0].Err
		}
		records := fetches.Records()
		if len(records) == 0 {
			if err := ctx.Err(); err != nil {
				return consumer.SourceRecord{}, err
			}
			continue
		}
		record := records[0]
		headers := make([]consumer.Header, len(record.Headers))
		for index, header := range record.Headers {
			headers[index] = consumer.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
		}
		return consumer.SourceRecord{
			Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
			Timestamp: record.Timestamp, Key: append([]byte(nil), record.Key...),
			Payload: append([]byte(nil), record.Value...), Headers: headers, AdapterToken: record,
		}, nil
	}
}

func (s *consumerSource) Commit(ctx context.Context, record consumer.SourceRecord) error {
	native, ok := record.AdapterToken.(*kgo.Record)
	if !ok || native == nil {
		return errors.New("kafka consumer: invalid commit token")
	}
	return s.client.CommitRecords(ctx, native)
}

func (s *consumerSource) Rewind(record consumer.SourceRecord) {
	native, ok := record.AdapterToken.(*kgo.Record)
	if !ok || native == nil {
		return
	}
	s.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		native.Topic: {native.Partition: {Epoch: native.LeaderEpoch, Offset: native.Offset}},
	})
}

func (s *consumerSource) Pause(partition consumer.TopicPartition) {
	s.client.PauseFetchPartitions(map[string][]int32{partition.Topic: {partition.Partition}})
}

func (s *consumerSource) Resume(partition consumer.TopicPartition) {
	s.client.ResumeFetchPartitions(map[string][]int32{partition.Topic: {partition.Partition}})
}

func (s *consumerSource) AllowRebalance() { s.client.AllowRebalance() }

func (s *consumerSource) Close() { s.client.CloseAllowingRebalance() }

var _ consumer.SourceFactory = (*ConsumerFactory)(nil)
