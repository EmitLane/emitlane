//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	managed "github.com/emitlane/emitlane/consumer"
	"os"
	"strconv"
	"testing"
)

// These are opt-in fixture settings, never production defaults or retry policy.
type qualificationCapacity struct {
	Consumers      int   `json:"consumers"`
	PoolConns      int32 `json:"pool_connections"` // Zero retains pgxpool's default.
	KafkaMemoryMiB int64 `json:"kafka_memory_mib"`
	KafkaCPUs      int64 `json:"kafka_cpus"` // Zero removes only the broker's CPU ceiling.
}

func readQualificationCapacity() (qualificationCapacity, error) {
	c := qualificationCapacity{Consumers: 2, KafkaMemoryMiB: 1536, KafkaCPUs: 2}
	if os.Getenv("EMITLANE_QUALIFICATION_DURATION") == "" {
		return c, nil
	}
	for _, o := range []struct {
		key              string
		minimum, maximum int64
		set              func(int64)
	}{
		{"EMITLANE_QUALIFICATION_CONSUMERS", 1, 8, func(n int64) { c.Consumers = int(n) }},
		{"EMITLANE_QUALIFICATION_POOL", 1, 32, func(n int64) { c.PoolConns = int32(n) }},
		{"EMITLANE_QUALIFICATION_KAFKA_MEMORY_MIB", 1536, 3072, func(n int64) { c.KafkaMemoryMiB = n }},
		{"EMITLANE_QUALIFICATION_KAFKA_CPUS", 0, 4, func(n int64) { c.KafkaCPUs = n }},
	} {
		if raw := os.Getenv(o.key); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < o.minimum || n > o.maximum {
				return c, fmt.Errorf("%s must be between %d and %d", o.key, o.minimum, o.maximum)
			}
			o.set(n)
		}
	}
	return c, nil
}

func TestQualificationCapacityIsOptInAndBounded(t *testing.T) {
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "")
	for _, key := range []string{"CONSUMERS", "POOL", "KAFKA_MEMORY_MIB", "KAFKA_CPUS"} {
		t.Setenv("EMITLANE_QUALIFICATION_"+key, "invalid")
	}
	c, err := readQualificationCapacity()
	if err != nil || c.Consumers != 2 || c.PoolConns != 0 || c.KafkaMemoryMiB != 1536 || c.KafkaCPUs != 2 {
		t.Fatalf("ordinary fixture changed: %+v %v", c, err)
	}
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "15m")
	for key, value := range map[string]string{"CONSUMERS": "8", "POOL": "12", "KAFKA_MEMORY_MIB": "3072", "KAFKA_CPUS": "0"} {
		t.Setenv("EMITLANE_QUALIFICATION_"+key, value)
	}
	c, err = readQualificationCapacity()
	if err != nil || c.Consumers != 8 || c.PoolConns != 12 || c.KafkaMemoryMiB != 3072 || c.KafkaCPUs != 0 {
		t.Fatalf("expanded capacity: %+v %v", c, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"CONSUMERS", "0"}, {"CONSUMERS", "9"}, {"POOL", "0"}, {"POOL", "33"},
		{"KAFKA_MEMORY_MIB", "1535"}, {"KAFKA_MEMORY_MIB", "3073"}, {"KAFKA_CPUS", "-1"}, {"KAFKA_CPUS", "5"}, {"POOL", "invalid"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv("EMITLANE_QUALIFICATION_"+tc.key, tc.value)
			if _, err := readQualificationCapacity(); err == nil {
				t.Fatal("unsafe capacity accepted")
			}
		})
	}
}

// Verify the diagnostic wrapper delegates the failure path without committing
// or changing offsets, and keeps timings bounded to the fixture partitions.
type qualificationTimingSource struct {
	managed.Source
	calls []string
}

func (s *qualificationTimingSource) Poll(context.Context) (managed.SourceRecord, error) {
	s.calls = append(s.calls, "poll")
	return managed.SourceRecord{Partition: 7, Offset: 123}, nil
}
func (s *qualificationTimingSource) Rewind(managed.SourceRecord)  { s.calls = append(s.calls, "rewind") }
func (s *qualificationTimingSource) Pause(managed.TopicPartition) { s.calls = append(s.calls, "pause") }
func (s *qualificationTimingSource) Resume(managed.TopicPartition) {
	s.calls = append(s.calls, "resume")
}
func (s *qualificationTimingSource) AllowRebalance() { s.calls = append(s.calls, "allow") }
func (s *qualificationTimingSource) Commit(context.Context, managed.SourceRecord) error {
	s.calls = append(s.calls, "commit")
	return errors.New("injected offset failure")
}
func (s *qualificationTimingSource) Close() { s.calls = append(s.calls, "close") }
func TestQualificationPartitionTimingDelegatesUnresolvedAndBoundsPauses(t *testing.T) {
	inner := &qualificationTimingSource{}
	q := &qualificationQueryTracer{}
	s := &qualificationDiagnosticSource{Source: inner, tracer: q}
	r, err := s.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.Rewind(r)
	s.Pause(managed.TopicPartition{Partition: 7})
	s.AllowRebalance()
	s.Resume(managed.TopicPartition{Partition: 7})
	if err := s.Commit(context.Background(), r); err == nil {
		t.Fatal("offset failure swallowed")
	}
	for n := int32(0); n < 1000; n++ {
		s.Pause(managed.TopicPartition{Partition: n})
	}
	if len(s.paused) != qualificationMaxPartitions {
		t.Fatalf("unbounded pauses: %d", len(s.paused))
	}
	s.censorPauses(map[string][]int32{"fixture": {7}})
	s.Resume(managed.TopicPartition{Partition: 7})
	s.Close()
	v := q.drain()
	if v["consumer.partition.7.unresolved"].Count != 1 || v["consumer.partition.7.resolved"].Count != 0 || v["consumer.partition.7.pause_residence"].Count != 1 || v["consumer.partition.7.offset_commit"].Errors != 1 || v["consumer.partition.7.pause_censored"].Count != 1 {
		t.Fatalf("wrong timing evidence: %v", v)
	}
	if fmt.Sprint(inner.calls[:6]) != "[poll rewind pause allow resume commit]" || inner.calls[len(inner.calls)-1] != "close" {
		t.Fatal("source call order changed")
	}
	if qualificationPartitionKey(999, "handler") != "consumer.partition.other.handler" {
		t.Fatal("unbounded timing key")
	}
}
