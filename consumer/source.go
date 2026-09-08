package consumer

import (
	"context"
	"time"
)

// SourceRecord is the broker-neutral delivery envelope used between the
// runtime and an adapter. Token is adapter-private and never reaches handlers.
type SourceRecord struct {
	Topic        string
	Partition    int32
	Offset       int64
	Timestamp    time.Time
	Key          []byte
	Payload      []byte
	Headers      []Header
	AdapterToken any
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

// RebalanceListener receives bounded group lifecycle notifications.
type RebalanceListener interface {
	Assigned(map[string][]int32)
	Revoked(map[string][]int32)
	Lost(map[string][]int32)
	RebalanceBlocked()
}

// Source is implemented by the franz-go adapter. Rewind is called before
// allowing a rebalance whenever the current offset is unresolved.
type Source interface {
	Poll(context.Context) (SourceRecord, error)
	Commit(context.Context, SourceRecord) error
	Rewind(SourceRecord)
	Pause(TopicPartition)
	Resume(TopicPartition)
	AllowRebalance()
	Close()
}

// SourceFactory creates one bounded group member per runtime worker.
type SourceFactory interface {
	NewSource(workerID string, listener RebalanceListener) (Source, error)
}
