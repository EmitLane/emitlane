package integrity

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStableCodesAreUniqueAndValid(t *testing.T) {
	t.Parallel()
	seen := make(map[Code]bool)
	for _, code := range StableCodes() {
		if seen[code] {
			t.Fatalf("duplicate stable code %q", code)
		}
		seen[code] = true
		if !code.Valid() {
			t.Fatalf("stable code %q is not valid", code)
		}
	}
	if Code("DYNAMIC_CODE").Valid() {
		t.Fatal("dynamic code unexpectedly accepted")
	}
}

func TestAccumulatorSeverityResultAndTruncation(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	acc := newAccumulator(ModeFull, 2, started)
	acc.add(Finding{Code: CodeStreamGap, Severity: SeverityWarning, Message: "gap"})
	acc.add(Finding{Code: CodePartitionOwnerStale, Severity: SeverityWarning, Message: "owner"})
	acc.add(Finding{Code: CodeStreamCursorInvalid, Severity: SeverityViolation, Message: "cursor"})
	report := acc.finish(started.Add(time.Second))
	if report.Result != "violations" || report.Clean {
		t.Fatalf("result=%q clean=%t", report.Result, report.Clean)
	}
	if report.Summary.Warnings != 2 || report.Summary.Violations != 1 {
		t.Fatalf("summary=%+v", report.Summary)
	}
	if len(report.Findings) != 2 || !report.FindingsTruncated {
		t.Fatalf("findings=%d truncated=%t", len(report.Findings), report.FindingsTruncated)
	}
}

func TestExitCodeMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		report Report
		strict bool
		want   int
	}{
		{name: "clean", report: Report{}, want: 0},
		{name: "warning default", report: Report{Summary: Summary{Warnings: 1}}, want: 0},
		{name: "warning strict", report: Report{Summary: Summary{Warnings: 1}}, strict: true, want: 2},
		{name: "violation", report: Report{Summary: Summary{Violations: 1}}, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ExitCode(tt.report, tt.strict); got != tt.want {
				t.Fatalf("ExitCode()=%d want %d", got, tt.want)
			}
		})
	}
}

func TestReportJSONContract(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	acc := newAccumulator(ModeSummary, 10, now)
	report := acc.finish(now.Add(time.Millisecond))
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"result", "mode", "started_at", "finished_at", "clean", "summary", "findings", "findings_truncated"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("JSON field %q is missing: %s", field, raw)
		}
	}
}

func TestStreamInspectionJSONContract(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	inspection := StreamInspection{
		Report: Report{Result: "warnings", Mode: ModeStream, StartedAt: now, FinishedAt: now,
			Clean: true, Findings: []Finding{}, Summary: Summary{Warnings: 1}},
		Destination: "orders.events", OrderingKey: "order:123", ExpectedPartition: 61,
		BlockingCondition: StreamStateGap, Events: []StreamEvent{},
	}
	raw, err := json.Marshal(inspection)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"result", "mode", "summary", "destination", "ordering_key", "expected_partition", "blocking_condition", "events"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("stream JSON field %q is missing: %s", field, raw)
		}
	}
	if _, nested := decoded["Report"]; nested {
		t.Fatalf("embedded report unexpectedly nested: %s", raw)
	}
}

func TestClassifyCursorIsRetentionAware(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   CursorObservation
		want StreamState
	}{
		{name: "retained history absent", in: CursorObservation{StartSequence: 1, NextSequence: 100}, want: StreamStateIdle},
		{name: "gap", in: CursorObservation{StartSequence: 1, NextSequence: 100, LowestFutureSequence: int64Pointer(102)}, want: StreamStateGap},
		{name: "retry wait", in: CursorObservation{ExpectedCount: 1, ExpectedStatus: "pending", ExpectedAvailableAt: timePointer(now.Add(time.Second))}, want: StreamStateRetryWait},
		{name: "ready", in: CursorObservation{ExpectedCount: 1, ExpectedStatus: "pending", ExpectedAvailableAt: timePointer(now)}, want: StreamStateReady},
		{name: "inflight", in: CursorObservation{ExpectedCount: 1, ExpectedStatus: "inflight"}, want: StreamStateInflight},
		{name: "dead", in: CursorObservation{ExpectedCount: 1, ExpectedStatus: "dead"}, want: StreamStateDeadBlocked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyCursor(now, tt.in); got != tt.want {
				t.Fatalf("ClassifyCursor()=%q want %q", got, tt.want)
			}
		})
	}
}

func int64Pointer(value int64) *int64        { return &value }
func timePointer(value time.Time) *time.Time { return &value }
