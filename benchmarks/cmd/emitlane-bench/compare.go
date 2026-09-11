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
	Scenario             string            `json:"scenario"`
	RelayInstances       int               `json:"relay_instances"`
	EventsPerSecond      float64           `json:"events_per_second"`
	LatencyP50Millis     float64           `json:"latency_p50_ms"`
	LatencyP95Millis     float64           `json:"latency_p95_ms"`
	LatencyP99Millis     float64           `json:"latency_p99_ms"`
	MemoryBytes          uint64            `json:"memory_bytes"`
	GoVersion            string            `json:"go_version"`
	OS                   string            `json:"os"`
	Arch                 string            `json:"arch"`
	EmitLaneCommit       string            `json:"emitlane_commit"`
	GitDirty             *bool             `json:"git_dirty,omitempty"`
	Seed                 *uint64           `json:"seed,omitempty"`
	ValidReleaseEvidence *bool             `json:"valid_release_evidence,omitempty"`
	Machine              comparisonMachine `json:"machine,omitempty"`
	PostgreSQL           map[string]any    `json:"postgresql,omitempty"`
	Kafka                map[string]any    `json:"kafka,omitempty"`
	PayloadSize          int               `json:"payload_size,omitempty"`
	Configuration        map[string]any    `json:"configuration,omitempty"`
}

type comparisonDocument struct {
	Runs                 []comparisonRun   `json:"runs"`
	EmitLaneCommit       string            `json:"emitlane_commit"`
	BaselineCommit       string            `json:"baseline_commit"`
	GitDirty             *bool             `json:"git_dirty,omitempty"`
	Seed                 *uint64           `json:"seed,omitempty"`
	ValidReleaseEvidence *bool             `json:"valid_release_evidence,omitempty"`
	Machine              comparisonMachine `json:"machine,omitempty"`
	PostgreSQL           map[string]any    `json:"postgresql,omitempty"`
	Kafka                map[string]any    `json:"kafka,omitempty"`
	PayloadSize          int               `json:"payload_size,omitempty"`
	Configuration        map[string]any    `json:"configuration,omitempty"`
}

type comparisonMachine struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	GoVersion string `json:"go_version"`
}

type comparisonSummary struct {
	Scenario            string               `json:"scenario"`
	RelayInstances      int                  `json:"relay_instances"`
	RunCount            int                  `json:"run_count"`
	Throughput          float64              `json:"throughput_events_per_second_mean"`
	LatencyP50          float64              `json:"latency_p50_ms_mean"`
	LatencyP95          float64              `json:"latency_p95_ms_mean"`
	LatencyP99          float64              `json:"latency_p99_ms_mean"`
	MemoryBytes         float64              `json:"memory_bytes_mean"`
	Provenance          comparisonProvenance `json:"provenance"`
	MetadataDifferences []string             `json:"metadata_differences,omitempty"`
}

type comparisonProvenance struct {
	EmitLaneCommit       string            `json:"emitlane_commit"`
	GitDirty             *bool             `json:"git_dirty"`
	Seed                 *uint64           `json:"seed"`
	ValidReleaseEvidence *bool             `json:"valid_release_evidence"`
	Machine              comparisonMachine `json:"machine"`
	PostgreSQL           map[string]any    `json:"postgresql"`
	Kafka                map[string]any    `json:"kafka"`
	PayloadSize          int               `json:"payload_size"`
	Configuration        map[string]any    `json:"configuration"`
}

type comparisonRow struct {
	Baseline               comparisonSummary `json:"baseline"`
	Candidate              comparisonSummary `json:"candidate"`
	ThroughputDeltaPercent float64           `json:"throughput_delta_percent"`
	P95DeltaPercent        float64           `json:"p95_delta_percent"`
	P99DeltaPercent        float64           `json:"p99_delta_percent"`
	EnvironmentCompatible  bool              `json:"environment_compatible"`
	EnvironmentDifferences []string          `json:"environment_differences,omitempty"`
	EvidenceIssues         []string          `json:"evidence_issues,omitempty"`
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
		for index := range document.Runs {
			inheritComparisonProvenance(&document.Runs[index], document)
		}
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
		differences := append([]string{}, base.MetadataDifferences...)
		differences = append(differences, current.MetadataDifferences...)
		differences = append(differences, environmentDifferences(base.Provenance, current.Provenance)...)
		differences = uniqueStrings(differences)
		rows = append(rows, comparisonRow{
			Baseline: base, Candidate: current,
			ThroughputDeltaPercent: percentDelta(base.Throughput, current.Throughput),
			P95DeltaPercent:        percentDelta(base.LatencyP95, current.LatencyP95),
			P99DeltaPercent:        percentDelta(base.LatencyP99, current.LatencyP99),
			EnvironmentCompatible:  len(differences) == 0,
			EnvironmentDifferences: differences,
			EvidenceIssues:         append(evidenceIssues("baseline", base.Provenance), evidenceIssues("candidate", current.Provenance)...),
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
		if item.RunCount == 1 {
			item.Provenance = run.provenance()
		} else {
			item.MetadataDifferences = append(item.MetadataDifferences, environmentDifferences(item.Provenance, run.provenance())...)
		}
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
		item.MetadataDifferences = uniqueStrings(item.MetadataDifferences)
		result[key] = item.comparisonSummary
	}
	return result
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result
}

func (run comparisonRun) provenance() comparisonProvenance {
	machine := run.Machine
	if run.GoVersion != "" {
		machine.GoVersion = run.GoVersion
	}
	if run.OS != "" {
		machine.OS = run.OS
	}
	if run.Arch != "" {
		machine.Arch = run.Arch
	}
	return comparisonProvenance{
		EmitLaneCommit: run.EmitLaneCommit, GitDirty: run.GitDirty, Seed: run.Seed,
		ValidReleaseEvidence: run.ValidReleaseEvidence, Machine: machine,
		PostgreSQL: run.PostgreSQL, Kafka: run.Kafka, PayloadSize: run.PayloadSize,
		Configuration: run.Configuration,
	}
}

func inheritComparisonProvenance(run *comparisonRun, document comparisonDocument) {
	if run.EmitLaneCommit == "" {
		run.EmitLaneCommit = document.EmitLaneCommit
		if run.EmitLaneCommit == "" {
			run.EmitLaneCommit = document.BaselineCommit
		}
	}
	if run.GitDirty == nil {
		run.GitDirty = document.GitDirty
	}
	if run.Seed == nil {
		run.Seed = document.Seed
	}
	if run.ValidReleaseEvidence == nil {
		run.ValidReleaseEvidence = document.ValidReleaseEvidence
	}
	if run.Machine == (comparisonMachine{}) {
		run.Machine = document.Machine
	}
	if run.PostgreSQL == nil {
		run.PostgreSQL = document.PostgreSQL
	}
	if run.Kafka == nil {
		run.Kafka = document.Kafka
	}
	if run.PayloadSize == 0 {
		run.PayloadSize = document.PayloadSize
	}
	if run.Configuration == nil {
		run.Configuration = document.Configuration
	}
}

func environmentDifferences(baseline, candidate comparisonProvenance) []string {
	fields := []struct {
		name      string
		baseline  string
		candidate string
	}{
		{"go_version", baseline.Machine.GoVersion, candidate.Machine.GoVersion},
		{"os", baseline.Machine.OS, candidate.Machine.OS},
		{"arch", baseline.Machine.Arch, candidate.Machine.Arch},
		{"payload_size", fmt.Sprint(baseline.PayloadSize), fmt.Sprint(candidate.PayloadSize)},
	}
	differences := make([]string, 0)
	for _, field := range fields {
		if field.baseline == "" || field.baseline == "0" || field.candidate == "" || field.candidate == "0" {
			differences = append(differences, field.name+" missing")
			continue
		}
		if field.baseline != field.candidate {
			differences = append(differences, fmt.Sprintf("%s: baseline=%s candidate=%s", field.name, field.baseline, field.candidate))
		}
	}
	for _, field := range []struct {
		name  string
		key   string
		left  map[string]any
		right map[string]any
	}{
		{"postgresql", "version", baseline.PostgreSQL, candidate.PostgreSQL},
		{"postgresql", "fsync", baseline.PostgreSQL, candidate.PostgreSQL},
		{"postgresql", "full_page_writes", baseline.PostgreSQL, candidate.PostgreSQL},
		{"postgresql", "synchronous_commit", baseline.PostgreSQL, candidate.PostgreSQL},
		{"kafka", "version", baseline.Kafka, candidate.Kafka},
		{"kafka", "broker_count", baseline.Kafka, candidate.Kafka},
		{"kafka", "required_acks", baseline.Kafka, candidate.Kafka},
		{"configuration", "batch_size", baseline.Configuration, candidate.Configuration},
		{"configuration", "concurrency", baseline.Configuration, candidate.Configuration},
		{"configuration", "poll_interval", baseline.Configuration, candidate.Configuration},
		{"configuration", "lease_duration", baseline.Configuration, candidate.Configuration},
		{"configuration", "publish_timeout", baseline.Configuration, candidate.Configuration},
	} {
		left, leftOK := field.left[field.key]
		right, rightOK := field.right[field.key]
		if !leftOK || !rightOK {
			differences = append(differences, field.name+"."+field.key+" missing")
			continue
		}
		if fmt.Sprint(left) != fmt.Sprint(right) {
			differences = append(differences, fmt.Sprintf("%s.%s: baseline=%v candidate=%v", field.name, field.key, left, right))
		}
	}
	return differences
}

func evidenceIssues(label string, provenance comparisonProvenance) []string {
	issues := make([]string, 0, 3)
	if provenance.EmitLaneCommit == "" {
		issues = append(issues, label+" commit missing")
	}
	if provenance.GitDirty == nil {
		issues = append(issues, label+" git_dirty missing")
	} else if *provenance.GitDirty {
		issues = append(issues, label+" run is dirty")
	}
	if provenance.Seed == nil {
		issues = append(issues, label+" seed missing")
	}
	if provenance.ValidReleaseEvidence == nil {
		issues = append(issues, label+" valid_release_evidence missing")
	} else if !*provenance.ValidReleaseEvidence {
		issues = append(issues, label+" is not valid release evidence")
	}
	return issues
}

func percentDelta(baseline, candidate float64) float64 {
	if baseline == 0 {
		return 0
	}
	return (candidate - baseline) / baseline * 100
}
