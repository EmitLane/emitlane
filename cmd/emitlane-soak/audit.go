package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const finalAuditTimeout = 2 * time.Minute

// auditKafka rebuilds correctness observations from immutable topic ranges.
// The caller must stop producers before invoking it.
func auditKafka(ctx context.Context, brokers, topics []string, live *verifier) (*verifier, error) {
	metadataClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("create Kafka audit metadata client: %w", err)
	}
	endOffsets, err := kadm.NewClient(metadataClient).ListEndOffsets(ctx, topics...)
	metadataClient.Close()
	if err != nil {
		return nil, fmt.Errorf("list Kafka audit end offsets: %w", err)
	}
	if err := endOffsets.Error(); err != nil {
		return nil, fmt.Errorf("validate Kafka audit end offsets: %w", err)
	}

	starts := make(map[string]map[int32]kgo.Offset, len(topics))
	targets := make(map[string]map[int32]int64, len(topics))
	remaining := 0
	for _, topic := range topics {
		partitions, ok := endOffsets[topic]
		if !ok || len(partitions) == 0 {
			return nil, fmt.Errorf("kafka audit topic %q has no partitions", topic)
		}
		starts[topic] = make(map[int32]kgo.Offset, len(partitions))
		targets[topic] = make(map[int32]int64, len(partitions))
		for partition, end := range partitions {
			if end.Err != nil {
				return nil, fmt.Errorf("kafka audit topic %q partition %d: %w", topic, partition, end.Err)
			}
			if end.Offset < 0 {
				return nil, fmt.Errorf("kafka audit topic %q partition %d has invalid end offset %d", topic, partition, end.Offset)
			}
			starts[topic][partition] = kgo.NewOffset().At(0)
			targets[topic][partition] = end.Offset
			if end.Offset > 0 {
				remaining++
			}
		}
	}

	audit := live.auditClone()
	if remaining == 0 {
		return audit, nil
	}
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(starts),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka audit consumer: %w", err)
	}
	defer consumer.Close()

	completed := make(map[string]map[int32]bool, len(topics))
	for remaining > 0 {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			first := errs[0]
			return nil, fmt.Errorf("kafka audit fetch %s[%d]: %w", first.Topic, first.Partition, first.Err)
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			topicTargets, ok := targets[partition.Topic]
			if !ok {
				return
			}
			target, ok := topicTargets[partition.Partition]
			if !ok || target == 0 {
				return
			}
			if completed[partition.Topic] == nil {
				completed[partition.Topic] = make(map[int32]bool)
			}
			if completed[partition.Topic][partition.Partition] {
				return
			}
			for _, record := range partition.Records {
				if record.Offset >= target {
					continue
				}
				audit.observeForAudit(record)
				if record.Offset+1 >= target {
					completed[partition.Topic][partition.Partition] = true
					remaining--
					break
				}
			}
		})
	}
	return audit, nil
}
