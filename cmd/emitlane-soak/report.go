package main

import (
	"fmt"
	"strings"
	"time"
)

func reportMarkdown(r Result) string {
	result := "PASS"
	if r.ExitCode != 0 {
		result = "FAIL"
	}
	var b strings.Builder
	fmt.Fprintln(&b, "# EmitLane local soak report")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "**Result: %s**\n\n", result)
	fmt.Fprintf(&b, "- Run ID: `%s`\n- Profile: `%s`\n- Duration: `%s`\n- Seed: `%d`\n- Git commit: `%s`\n- Platform: `%s/%s`\n\n", r.RunID, r.Profile, time.Duration(r.DurationSeconds*float64(time.Second)).Round(time.Second), r.Seed, r.GitCommit, r.OS, r.Arch)
	fmt.Fprintln(&b, "## Timeline")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "![Committed and observed events, backlog, and injected faults](timeline.svg)")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "## Correctness")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "| Metric | Result |")
	fmt.Fprintln(&b, "|---|---:|")
	fmt.Fprintf(&b, "| Committed events | %d |\n| Unique delivered | %d |\n| Lost | %d |\n| Kafka records | %d |\n| At-least-once duplicates | %d |\n| Ordering regressions | %d |\n| Unexpected sequence skips | %d |\n\n", r.CommittedEvents, r.ObservedUniqueEvents, r.LostEvents, r.BrokerRecords, r.DuplicateRecords, r.OrderingRegressions, r.OrderingSkips)
	fmt.Fprintln(&b, "## Faults")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "| Fault | Count |")
	fmt.Fprintln(&b, "|---|---:|")
	fmt.Fprintf(&b, "| Relay graceful restarts | %d |\n| Relay crash takeovers | %d |\n| Kafka outages | %d |\n| Pause/resume cycles | %d |\n| Partition acquisitions | %d |\n| Partition handoffs | %d |\n\n", r.RelayRestarts, r.RelayCrashTakeovers, r.KafkaOutages, r.PauseCycles, r.PartitionAcquisitions, r.PartitionHandoffs)
	fmt.Fprintln(&b, "## Final state")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Pending: %d  \nInflight: %d  \nDead: %d  \nBlocked ordered streams: %d  \nGap streams: %d\n\n", r.PendingFinal, r.InflightFinal, r.DeadFinal, r.BlockedStreamsFinal, r.GapStreamsFinal)
	fmt.Fprintln(&b, "## Performance")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Throughput: %.1f committed events/sec  \nLatency p50/p95/p99: %.2f / %.2f / %.2f ms  \nRecovery: %.2f seconds\n\n", r.ThroughputEventsSec, r.LatencyP50Millis, r.LatencyP95Millis, r.LatencyP99Millis, r.BacklogRecoverySecs)
	fmt.Fprintln(&b, "## Verdict")
	fmt.Fprintln(&b)
	if result == "PASS" {
		fmt.Fprintln(&b, "PASS: all committed event IDs were observed and no ordered stream regressed.")
	} else {
		fmt.Fprintf(&b, "FAIL: %s.\n", r.FailureReason)
		if len(r.Diagnostics) > 0 {
			d := r.Diagnostics[0]
			fmt.Fprintf(&b, "\nFirst diagnostic: %s", d.Kind)
			if d.Stream != "" {
				fmt.Fprintf(&b, " in `%s`", d.Stream)
			}
			if d.PreviousSequence != 0 || d.ObservedSequence != 0 {
				fmt.Fprintf(&b, " (previous %d, observed %d)", d.PreviousSequence, d.ObservedSequence)
			}
			fmt.Fprintln(&b, ".")
		}
	}
	return b.String()
}
