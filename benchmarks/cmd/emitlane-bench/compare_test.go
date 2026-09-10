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
