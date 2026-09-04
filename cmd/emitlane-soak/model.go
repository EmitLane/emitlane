package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"runtime"
	"sort"
	"time"
)

const (
	soakRoot       = ".emitlane/soak"
	postgresImage  = "postgres:16-alpine"
	kafkaImage     = "apache/kafka-native:4.3.1"
	progressPeriod = 5 * time.Second
)

type Config struct {
	RunID           string        `json:"run_id"`
	Profile         string        `json:"profile"`
	Seed            uint64        `json:"seed"`
	Duration        time.Duration `json:"duration"`
	RecoveryTimeout time.Duration `json:"recovery_timeout"`
	Warmup          time.Duration `json:"warmup"`
	Relays          int           `json:"relays"`
	OrderedStreams  int           `json:"ordered_streams"`
	OrderedPercent  int           `json:"ordered_percent"`
	EventsPerSecond int           `json:"events_per_second"`
	FaultInterval   time.Duration `json:"fault_interval"`
	Faults          bool          `json:"faults"`
	PayloadBytes    int           `json:"payload_bytes"`
}

type State struct {
	RunID     string    `json:"run_id"`
	State     string    `json:"state"`
	Phase     string    `json:"phase"`
	UpdatedAt time.Time `json:"updated_at"`
	Reason    string    `json:"reason,omitempty"`
}

type Progress struct {
	RunID               string        `json:"run_id"`
	State               string        `json:"state"`
	Phase               string        `json:"phase"`
	Profile             string        `json:"profile"`
	Seed                uint64        `json:"seed"`
	StartedAt           time.Time     `json:"started_at"`
	UpdatedAt           time.Time     `json:"updated_at"`
	Elapsed             time.Duration `json:"elapsed"`
	CommittedEvents     int64         `json:"committed_events"`
	ObservedUnique      int64         `json:"observed_unique_events"`
	BrokerRecords       int64         `json:"broker_records"`
	DuplicateRecords    int64         `json:"duplicate_records"`
	NotObservedYet      int64         `json:"not_observed_yet"`
	OrderedStreams      int           `json:"ordered_streams"`
	Relays              int           `json:"relays"`
	RelayRestarts       int64         `json:"relay_restarts"`
	RelayCrashTakeovers int64         `json:"relay_crash_takeovers"`
	KafkaOutages        int64         `json:"kafka_outages"`
	PauseCycles         int64         `json:"pause_cycles"`
	OrderingRegressions int64         `json:"ordering_regressions"`
	OrderingSkips       int64         `json:"ordering_skips"`
}

type Diagnostic struct {
	Kind             string `json:"kind"`
	EventID          string `json:"event_id,omitempty"`
	Stream           string `json:"stream,omitempty"`
	PreviousSequence int64  `json:"previous_sequence,omitempty"`
	ObservedSequence int64  `json:"observed_sequence,omitempty"`
	Detail           string `json:"detail,omitempty"`
}

type Result struct {
	RunID                 string             `json:"run_id"`
	Profile               string             `json:"profile"`
	Seed                  uint64             `json:"seed"`
	StartedAt             time.Time          `json:"start_timestamp"`
	EndedAt               time.Time          `json:"end_timestamp"`
	FinalState            string             `json:"final_state"`
	FailureReason         string             `json:"failure_reason,omitempty"`
	GitBranch             string             `json:"git_branch"`
	GitCommit             string             `json:"git_commit"`
	GoVersion             string             `json:"go_version"`
	OS                    string             `json:"os"`
	Arch                  string             `json:"arch"`
	CPU                   string             `json:"cpu"`
	DockerVersion         string             `json:"docker_version"`
	PostgreSQLVersion     string             `json:"postgresql_version"`
	KafkaVersion          string             `json:"kafka_version"`
	DurationSeconds       float64            `json:"duration_seconds"`
	Configuration         Config             `json:"actual_configuration"`
	CommittedEvents       int64              `json:"committed_events"`
	OrderedCommitted      int64              `json:"ordered_committed"`
	UnorderedCommitted    int64              `json:"unordered_committed"`
	ObservedUniqueEvents  int64              `json:"observed_unique_events"`
	BrokerRecords         int64              `json:"broker_records"`
	DuplicateRecords      int64              `json:"duplicate_records"`
	LostEvents            int64              `json:"lost_events"`
	OrderedStreams        int                `json:"ordered_streams"`
	OrderingRegressions   int64              `json:"ordering_regressions"`
	OrderingSkips         int64              `json:"ordering_skips"`
	RelayRestarts         int64              `json:"relay_restarts"`
	RelayCrashTakeovers   int64              `json:"relay_crash_takeovers"`
	KafkaOutages          int64              `json:"kafka_outages"`
	KafkaOutageSeconds    float64            `json:"kafka_outage_duration_seconds"`
	PauseCycles           int64              `json:"pause_cycles"`
	PartitionAcquisitions int64              `json:"partition_acquisitions"`
	PartitionHandoffs     int64              `json:"partition_handoffs"`
	PendingFinal          int64              `json:"pending_final"`
	InflightFinal         int64              `json:"inflight_final"`
	DeadFinal             int64              `json:"dead_final"`
	BlockedStreamsFinal   int64              `json:"blocked_streams_final"`
	GapStreamsFinal       int64              `json:"gap_streams_final"`
	InfrastructureErrors  int64              `json:"infrastructure_errors"`
	ThroughputEventsSec   float64            `json:"throughput_events_sec"`
	LatencyP50Millis      float64            `json:"latency_p50_ms"`
	LatencyP95Millis      float64            `json:"latency_p95_ms"`
	LatencyP99Millis      float64            `json:"latency_p99_ms"`
	BacklogRecoverySecs   float64            `json:"backlog_recovery_seconds"`
	FaultDurations        map[string]float64 `json:"fault_durations_seconds"`
	Diagnostics           []Diagnostic       `json:"diagnostics,omitempty"`
	ExitCode              int                `json:"exit_code"`
}

func profile(name string) (Config, error) {
	switch name {
	case "quick":
		return Config{Profile: name, Duration: 90 * time.Second, RecoveryTimeout: 3 * time.Minute, Warmup: 3 * time.Second, Relays: 2, OrderedStreams: 100, OrderedPercent: 80, EventsPerSecond: 40, FaultInterval: 12 * time.Second, Faults: true, PayloadBytes: 256}, nil
	case "standard":
		return Config{Profile: name, Duration: 20 * time.Minute, RecoveryTimeout: 5 * time.Minute, Warmup: 10 * time.Second, Relays: 4, OrderedStreams: 1000, OrderedPercent: 80, EventsPerSecond: 120, FaultInterval: 45 * time.Second, Faults: true, PayloadBytes: 512}, nil
	case "release":
		return Config{Profile: name, Duration: time.Hour, RecoveryTimeout: 10 * time.Minute, Warmup: 15 * time.Second, Relays: 4, OrderedStreams: 3500, OrderedPercent: 80, EventsPerSecond: 250, FaultInterval: time.Minute, Faults: true, PayloadBytes: 512}, nil
	default:
		return Config{}, fmt.Errorf("unknown profile %q (want quick, standard, or release)", name)
	}
}

func (c Config) validate() error {
	if c.Duration <= 0 || c.RecoveryTimeout <= 0 || c.Warmup < 0 {
		return errors.New("duration and recovery-timeout must be positive; warmup must not be negative")
	}
	if c.Relays < 1 || c.OrderedStreams < 1 || c.EventsPerSecond < 1 || c.PayloadBytes < 1 {
		return errors.New("relays, streams, rate and payload bytes must be positive")
	}
	if c.OrderedPercent < 1 || c.OrderedPercent > 99 {
		return errors.New("ordered percent must be between 1 and 99")
	}
	return nil
}

func newSeed() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	var seed uint64
	for _, v := range b {
		seed = seed<<8 | uint64(v)
	}
	return seed, nil
}

func seededRand(seed uint64) *mathrand.Rand {
	return mathrand.New(mathrand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

func newRunID(now time.Time, seed uint64) string {
	var b [3]byte
	b[0], b[1], b[2] = byte(seed>>16), byte(seed>>8), byte(seed)
	return now.UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b[:])
}

func verdict(r Result) error {
	checks := []struct {
		name  string
		value int64
	}{
		{"lost committed events", r.LostEvents}, {"ordering regressions", r.OrderingRegressions},
		{"unexpected sequence skips", r.OrderingSkips}, {"pending final", r.PendingFinal},
		{"inflight final", r.InflightFinal}, {"dead final", r.DeadFinal},
		{"blocked ordered streams final", r.BlockedStreamsFinal}, {"gap streams final", r.GapStreamsFinal},
		{"infrastructure/runner errors", r.InfrastructureErrors},
	}
	for _, check := range checks {
		if check.value != 0 {
			return fmt.Errorf("%s: %d", check.name, check.value)
		}
	}
	return nil
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]float64(nil), values...)
	slicesSort(copyValues)
	idx := int(math.Ceil(float64(len(copyValues))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(copyValues) {
		idx = len(copyValues) - 1
	}
	return copyValues[idx]
}

func slicesSort(values []float64) {
	sort.Float64s(values)
}

func platformCPU() string { return fmt.Sprintf("%d logical CPUs", runtime.NumCPU()) }
