// Package integrity verifies EmitLane's durable PostgreSQL protocol state.
// Verification is diagnostic and never repairs or mutates application data.
package integrity

import (
	"fmt"
	"sort"
	"time"
)

type Mode string

const (
	ModeSummary Mode = "summary"
	ModeFull    Mode = "full"
	ModeStream  Mode = "stream"
)

type Severity string

const (
	SeverityInfo      Severity = "info"
	SeverityWarning   Severity = "warning"
	SeverityViolation Severity = "violation"
)

type Code string

const (
	CodeSchemaVersionMismatch          Code = "SCHEMA_VERSION_MISMATCH"
	CodePartitionCountInvalid          Code = "PARTITION_COUNT_INVALID"
	CodePartitionIDInvalid             Code = "PARTITION_ID_INVALID"
	CodePartitionEpochInvalid          Code = "PARTITION_EPOCH_INVALID"
	CodePartitionLeaseShapeInvalid     Code = "PARTITION_LEASE_SHAPE_INVALID"
	CodePartitionOwnerStale            Code = "PARTITION_OWNER_STALE"
	CodePartitionHandoffOverdue        Code = "PARTITION_HANDOFF_OVERDUE"
	CodeOrderingFieldsPartial          Code = "ORDERING_FIELDS_PARTIAL"
	CodeOrderingPartitionMismatch      Code = "ORDERING_PARTITION_MISMATCH"
	CodeStreamPartitionMismatch        Code = "STREAM_PARTITION_MISMATCH"
	CodeStreamCursorInvalid            Code = "STREAM_CURSOR_INVALID"
	CodeActiveSequenceBehindCursor     Code = "ACTIVE_SEQUENCE_BEHIND_CURSOR"
	CodeExpectedSequenceDuplicate      Code = "EXPECTED_SEQUENCE_DUPLICATE"
	CodeStreamGap                      Code = "STREAM_GAP"
	CodeStreamDeadBlocked              Code = "STREAM_DEAD_BLOCKED"
	CodeStreamRetryWait                Code = "STREAM_RETRY_WAIT"
	CodeStaleEventLease                Code = "STALE_EVENT_LEASE"
	CodeRuntimeControlInvalid          Code = "RUNTIME_CONTROL_INVALID"
	CodeRelayPresenceStale             Code = "RELAY_PRESENCE_STALE"
	CodeInboxStateInvalid              Code = "INBOX_STATE_INVALID"
	CodeInboxLeaseShapeInvalid         Code = "INBOX_LEASE_SHAPE_INVALID"
	CodeInboxSourceMetadataInvalid     Code = "INBOX_SOURCE_METADATA_INVALID"
	CodeInboxProcessedTimestampInvalid Code = "INBOX_PROCESSED_TIMESTAMP_INVALID"
	CodeInboxAttemptsInvalid           Code = "INBOX_ATTEMPTS_INVALID"
	CodeInboxStaleLease                Code = "INBOX_STALE_LEASE"
	CodeInboxDeadBlocked               Code = "INBOX_DEAD_BLOCKED"
	CodeInboxRetryWait                 Code = "INBOX_RETRY_WAIT"
)

var stableCodes = []Code{
	CodeSchemaVersionMismatch,
	CodePartitionCountInvalid,
	CodePartitionIDInvalid,
	CodePartitionEpochInvalid,
	CodePartitionLeaseShapeInvalid,
	CodePartitionOwnerStale,
	CodePartitionHandoffOverdue,
	CodeOrderingFieldsPartial,
	CodeOrderingPartitionMismatch,
	CodeStreamPartitionMismatch,
	CodeStreamCursorInvalid,
	CodeActiveSequenceBehindCursor,
	CodeExpectedSequenceDuplicate,
	CodeStreamGap,
	CodeStreamDeadBlocked,
	CodeStreamRetryWait,
	CodeStaleEventLease,
	CodeRuntimeControlInvalid,
	CodeRelayPresenceStale,
	CodeInboxStateInvalid,
	CodeInboxLeaseShapeInvalid,
	CodeInboxSourceMetadataInvalid,
	CodeInboxProcessedTimestampInvalid,
	CodeInboxAttemptsInvalid,
	CodeInboxStaleLease,
	CodeInboxDeadBlocked,
	CodeInboxRetryWait,
}

// StableCodes returns a copy of the fixed finding-code contract.
func StableCodes() []Code {
	return append([]Code(nil), stableCodes...)
}

func (c Code) Valid() bool {
	for _, candidate := range stableCodes {
		if c == candidate {
			return true
		}
	}
	return false
}

type Finding struct {
	Code            Code     `json:"code"`
	Severity        Severity `json:"severity"`
	Message         string   `json:"message"`
	Destination     string   `json:"destination,omitempty"`
	OrderingKey     string   `json:"ordering_key,omitempty"`
	EventID         string   `json:"event_id,omitempty"`
	PartitionID     *int16   `json:"partition_id,omitempty"`
	Consumer        string   `json:"consumer,omitempty"`
	SourcePartition *int32   `json:"source_partition,omitempty"`
	Expected        any      `json:"expected,omitempty"`
	Observed        any      `json:"observed,omitempty"`
}

type Summary struct {
	Violations          int64 `json:"violations"`
	Warnings            int64 `json:"warnings"`
	Infos               int64 `json:"infos"`
	PendingEvents       int64 `json:"pending_events"`
	InflightEvents      int64 `json:"inflight_events"`
	DeadEvents          int64 `json:"dead_events"`
	ActiveOrderedEvents int64 `json:"active_ordered_events"`
	OrderedStreams      int64 `json:"ordered_streams"`
	ReadyStreams        int64 `json:"ready_streams"`
	InflightStreams     int64 `json:"inflight_streams"`
	BlockedStreams      int64 `json:"blocked_streams"`
	GapStreams          int64 `json:"gap_streams"`
	DeadBlockedStreams  int64 `json:"dead_blocked_streams"`
	RetryWaitStreams    int64 `json:"retry_wait_streams"`
	StaleEventLeases    int64 `json:"stale_event_leases"`
	OrderingPartitions  int64 `json:"ordering_partitions"`
	OwnedPartitions     int64 `json:"owned_partitions"`
	HandoffPartitions   int64 `json:"handoff_partitions"`
	StaleRelays         int64 `json:"stale_relays"`
	InboxPending        int64 `json:"inbox_pending"`
	InboxInflight       int64 `json:"inbox_inflight"`
	InboxRetryWait      int64 `json:"inbox_retry_wait"`
	InboxProcessed      int64 `json:"inbox_processed"`
	InboxDead           int64 `json:"inbox_dead"`
	InboxStaleLeases    int64 `json:"inbox_stale_leases"`
}

type Report struct {
	Result            string    `json:"result"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	Mode              Mode      `json:"mode"`
	Clean             bool      `json:"clean"`
	Summary           Summary   `json:"summary"`
	Findings          []Finding `json:"findings"`
	FindingsTruncated bool      `json:"findings_truncated"`
}

// ExitCode maps a completed report to the CLI contract. Operational failures
// are represented by returned errors and use exit status 1 in the CLI.
func ExitCode(report Report, strict bool) int {
	if report.Summary.Violations > 0 || strict && report.Summary.Warnings > 0 {
		return 2
	}
	return 0
}

type accumulator struct {
	report      Report
	maxFindings int
}

func newAccumulator(mode Mode, maxFindings int, startedAt time.Time) *accumulator {
	return &accumulator{
		report:      Report{Mode: mode, StartedAt: startedAt, Findings: []Finding{}},
		maxFindings: maxFindings,
	}
}

func (a *accumulator) add(f Finding) {
	switch f.Severity {
	case SeverityInfo:
		a.report.Summary.Infos++
	case SeverityWarning:
		a.report.Summary.Warnings++
	case SeverityViolation:
		a.report.Summary.Violations++
	default:
		panic(fmt.Sprintf("integrity: unsupported severity %q", f.Severity))
	}
	if len(a.report.Findings) < a.maxFindings {
		a.report.Findings = append(a.report.Findings, f)
	} else {
		a.report.FindingsTruncated = true
	}
}

func (a *accumulator) finish(finishedAt time.Time) Report {
	a.report.FinishedAt = finishedAt
	a.report.Clean = a.report.Summary.Violations == 0
	switch {
	case a.report.Summary.Violations > 0:
		a.report.Result = "violations"
	case a.report.Summary.Warnings > 0:
		a.report.Result = "warnings"
	default:
		a.report.Result = "clean"
	}
	sort.SliceStable(a.report.Findings, func(i, j int) bool {
		left, right := a.report.Findings[i], a.report.Findings[j]
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		if left.Destination != right.Destination {
			return left.Destination < right.Destination
		}
		if left.Consumer != right.Consumer {
			return left.Consumer < right.Consumer
		}
		if left.EventID != right.EventID {
			return left.EventID < right.EventID
		}
		leftPartition, rightPartition := int16(-1), int16(-1)
		if left.PartitionID != nil {
			leftPartition = *left.PartitionID
		}
		if right.PartitionID != nil {
			rightPartition = *right.PartitionID
		}
		if leftPartition != rightPartition {
			return leftPartition < rightPartition
		}
		leftSourcePartition, rightSourcePartition := int32(-1), int32(-1)
		if left.SourcePartition != nil {
			leftSourcePartition = *left.SourcePartition
		}
		if right.SourcePartition != nil {
			rightSourcePartition = *right.SourcePartition
		}
		return leftSourcePartition < rightSourcePartition
	})
	return a.report
}

type StreamState string

const (
	StreamStateIdle        StreamState = "idle"
	StreamStateReady       StreamState = "ready"
	StreamStateInflight    StreamState = "inflight"
	StreamStateRetryWait   StreamState = "retry_wait"
	StreamStateGap         StreamState = "gap"
	StreamStateDeadBlocked StreamState = "dead_blocked"
)

type CursorObservation struct {
	StartSequence        int64
	NextSequence         int64
	ExpectedCount        int64
	ExpectedStatus       string
	ExpectedAvailableAt  *time.Time
	ExpectedLeaseUntil   *time.Time
	LowestFutureSequence *int64
}

type StreamCursor struct {
	Destination   string    `json:"destination"`
	OrderingKey   string    `json:"ordering_key"`
	PartitionID   int16     `json:"partition_id"`
	StartSequence int64     `json:"start_sequence"`
	NextSequence  int64     `json:"next_sequence"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type StreamEvent struct {
	ID          string     `json:"id"`
	Sequence    int64      `json:"sequence"`
	PartitionID int16      `json:"partition_id"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	AvailableAt time.Time  `json:"available_at"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

type PartitionState struct {
	PartitionID      int16      `json:"partition_id"`
	LeaseOwner       string     `json:"lease_owner,omitempty"`
	LeaseUntil       *time.Time `json:"lease_until,omitempty"`
	Epoch            int64      `json:"epoch"`
	HandoffNotBefore *time.Time `json:"handoff_not_before,omitempty"`
	PublishTimeoutMS *int       `json:"publish_timeout_ms,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

type StreamInspection struct {
	Report
	Destination       string          `json:"destination"`
	OrderingKey       string          `json:"ordering_key"`
	ExpectedPartition int16           `json:"expected_partition"`
	BlockingCondition StreamState     `json:"blocking_condition"`
	Stream            *StreamCursor   `json:"stream,omitempty"`
	Partition         *PartitionState `json:"partition,omitempty"`
	Events            []StreamEvent   `json:"events"`
	EventsTruncated   bool            `json:"events_truncated"`
}

// ClassifyCursor classifies one stream from active retained rows only.
func ClassifyCursor(now time.Time, observation CursorObservation) StreamState {
	if observation.ExpectedCount == 0 {
		if observation.LowestFutureSequence != nil {
			return StreamStateGap
		}
		return StreamStateIdle
	}
	switch observation.ExpectedStatus {
	case "dead":
		return StreamStateDeadBlocked
	case "inflight":
		return StreamStateInflight
	case "pending":
		if observation.ExpectedAvailableAt != nil && observation.ExpectedAvailableAt.After(now) {
			return StreamStateRetryWait
		}
		return StreamStateReady
	default:
		return StreamStateIdle
	}
}
