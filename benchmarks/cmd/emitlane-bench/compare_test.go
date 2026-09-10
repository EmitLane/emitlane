package main

import "testing"

func TestCompareRunsMatchesScenarioAndRelayCount(t *testing.T) {
	baseline := []comparisonRun{{Scenario: "backlog-drain", RelayInstances: 1, EventsPerSecond: 100, LatencyP95Millis: 20, LatencyP99Millis: 30}}
	candidate := []comparisonRun{{Scenario: "backlog-drain", RelayInstances: 1, EventsPerSecond: 125, LatencyP95Millis: 16, LatencyP99Millis: 24}}
	rows := compareRuns(baseline, candidate)
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(rows))
	}
	if rows[0].ThroughputDeltaPercent != 25 {
		t.Fatalf("throughput delta=%f, want 25", rows[0].ThroughputDeltaPercent)
	}
	if rows[0].P95DeltaPercent != -20 {
		t.Fatalf("p95 delta=%f, want -20", rows[0].P95DeltaPercent)
	}
}

func TestCompareRunsMarksIncompatibleEnvironment(t *testing.T) {
	clean := false
	seed := uint64(7)
	baseline := []comparisonRun{{
		Scenario: "backlog-drain", RelayInstances: 1, EventsPerSecond: 100,
		EmitLaneCommit: "baseline", GitDirty: &clean, Seed: &seed, ValidReleaseEvidence: boolPointer(true),
		Machine:       comparisonMachine{OS: "darwin", Arch: "arm64", GoVersion: "go1.27.0"},
		PostgreSQL:    map[string]any{"version": "16.15", "fsync": "on", "full_page_writes": "on", "synchronous_commit": "on"},
		Kafka:         map[string]any{"version": "4.3.1", "broker_count": float64(1), "required_acks": "all"},
		PayloadSize:   1024,
		Configuration: map[string]any{"batch_size": float64(200), "concurrency": float64(16), "poll_interval": "100ms", "lease_duration": "3s", "publish_timeout": "2s"},
	}}
	candidate := []comparisonRun{{
		Scenario: "backlog-drain", RelayInstances: 1, EventsPerSecond: 120,
		EmitLaneCommit: "candidate", GitDirty: &clean, Seed: &seed, ValidReleaseEvidence: boolPointer(true),
		Machine:       comparisonMachine{OS: "linux", Arch: "arm64", GoVersion: "go1.27.0"},
		PostgreSQL:    map[string]any{"version": "16.15", "fsync": "on", "full_page_writes": "on", "synchronous_commit": "on"},
		Kafka:         map[string]any{"version": "4.3.1", "broker_count": float64(1), "required_acks": "all"},
		PayloadSize:   1024,
		Configuration: map[string]any{"batch_size": float64(200), "concurrency": float64(16), "poll_interval": "100ms", "lease_duration": "3s", "publish_timeout": "2s"},
	}}
	rows := compareRuns(baseline, candidate)
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(rows))
	}
	if rows[0].EnvironmentCompatible {
		t.Fatal("comparison unexpectedly marked compatible")
	}
	if len(rows[0].EnvironmentDifferences) == 0 {
		t.Fatal("comparison did not report metadata mismatch")
	}
}

func TestComparisonEvidenceMarksDirtyRun(t *testing.T) {
	dirty := true
	issues := evidenceIssues("candidate", comparisonProvenance{
		EmitLaneCommit: "candidate", GitDirty: &dirty, Seed: uint64Pointer(1), ValidReleaseEvidence: boolPointer(false),
	})
	if len(issues) != 2 {
		t.Fatalf("issues=%v, want dirty and invalid evidence", issues)
	}
}

func boolPointer(value bool) *bool       { return &value }
func uint64Pointer(value uint64) *uint64 { return &value }
