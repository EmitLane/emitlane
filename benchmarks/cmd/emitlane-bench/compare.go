package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
)

type comparisonRun struct {
	Scenario         string  `json:"scenario"`
	RelayInstances   int     `json:"relay_instances"`
	EventsPerSecond  float64 `json:"events_per_second"`
	LatencyP50Millis float64 `json:"latency_p50_ms"`
	LatencyP95Millis float64 `json:"latency_p95_ms"`
	LatencyP99Millis float64 `json:"latency_p99_ms"`
	MemoryBytes      uint64  `json:"memory_bytes"`
}

type comparisonDocument struct {
	Runs []comparisonRun `json:"runs"`
}

type comparisonSummary struct {
	Scenario       string  `json:"scenario"`
	RelayInstances int     `json:"relay_instances"`
	RunCount       int     `json:"run_count"`
	Throughput     float64 `json:"throughput_events_per_second_mean"`
	LatencyP50     float64 `json:"latency_p50_ms_mean"`
	LatencyP95     float64 `json:"latency_p95_ms_mean"`
	LatencyP99     float64 `json:"latency_p99_ms_mean"`
	MemoryBytes    float64 `json:"memory_bytes_mean"`
}

type comparisonRow struct {
	Baseline               comparisonSummary `json:"baseline"`
	Candidate              comparisonSummary `json:"candidate"`
	ThroughputDeltaPercent float64           `json:"throughput_delta_percent"`
	P95DeltaPercent        float64           `json:"p95_delta_percent"`
	P99DeltaPercent        float64           `json:"p99_delta_percent"`
}

func runCompare(args []string) error {
	flags := flag.NewFlagSet("emitlane-bench compare", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	baselinePath := flags.String("baseline", "", "baseline JSON file")
	candidatePath := flags.String("candidate", "", "candidate JSON file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *baselinePath == "" || *candidatePath == "" {
		return errors.New("compare requires --baseline and --candidate")
	}
	baseline, err := loadComparisonRuns(*baselinePath)
	if err != nil {
		return fmt.Errorf("load baseline: %w", err)
	}
	candidate, err := loadComparisonRuns(*candidatePath)
	if err != nil {
		return fmt.Errorf("load candidate: %w", err)
	}
	rows := compareRuns(baseline, candidate)
	return json.NewEncoder(os.Stdout).Encode(rows)
}

func loadComparisonRuns(path string) ([]comparisonRun, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document comparisonDocument
	if err := json.Unmarshal(raw, &document); err == nil && len(document.Runs) > 0 {
		return document.Runs, nil
	}
	var single comparisonRun
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, err
	}
	if single.Scenario == "" {
		return nil, errors.New("benchmark JSON has no scenario or runs")
	}
	return []comparisonRun{single}, nil
}

func compareRuns(baseline, candidate []comparisonRun) []comparisonRow {
	baselineSummaries := summarizeRuns(baseline)
	candidateSummaries := summarizeRuns(candidate)
	rows := make([]comparisonRow, 0)
	for key, base := range baselineSummaries {
		current, ok := candidateSummaries[key]
		if !ok {
			continue
		}
		rows = append(rows, comparisonRow{
			Baseline: base, Candidate: current,
			ThroughputDeltaPercent: percentDelta(base.Throughput, current.Throughput),
			P95DeltaPercent:        percentDelta(base.LatencyP95, current.LatencyP95),
			P99DeltaPercent:        percentDelta(base.LatencyP99, current.LatencyP99),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Baseline.Scenario == rows[j].Baseline.Scenario {
			return rows[i].Baseline.RelayInstances < rows[j].Baseline.RelayInstances
		}
		return rows[i].Baseline.Scenario < rows[j].Baseline.Scenario
	})
	return rows
}

func summarizeRuns(runs []comparisonRun) map[string]comparisonSummary {
	type total struct{ comparisonSummary }
	totals := make(map[string]total)
	for _, run := range runs {
		key := fmt.Sprintf("%s/%d", run.Scenario, run.RelayInstances)
		item := totals[key]
		item.Scenario = run.Scenario
		item.RelayInstances = run.RelayInstances
		item.RunCount++
		item.Throughput += run.EventsPerSecond
		item.LatencyP50 += run.LatencyP50Millis
		item.LatencyP95 += run.LatencyP95Millis
		item.LatencyP99 += run.LatencyP99Millis
		item.MemoryBytes += float64(run.MemoryBytes)
		totals[key] = item
	}
	result := make(map[string]comparisonSummary, len(totals))
	for key, item := range totals {
		count := float64(item.RunCount)
		item.Throughput /= count
		item.LatencyP50 /= count
		item.LatencyP95 /= count
		item.LatencyP99 /= count
		item.MemoryBytes /= count
		result[key] = item.comparisonSummary
	}
	return result
}

func percentDelta(baseline, candidate float64) float64 {
	if baseline == 0 {
		return 0
	}
	return (candidate - baseline) / baseline * 100
}
