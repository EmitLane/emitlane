package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/integrity"
)

type commandExit int

func (e commandExit) Error() string { return "" }
func (e commandExit) ExitCode() int { return int(e) }

func integrityCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane integrity check|stream")
	}
	switch args[0] {
	case "check":
		return integrityCheckCmd(args[1:])
	case "stream":
		return integrityStreamCmd(args[1:])
	default:
		return fmt.Errorf("usage: emitlane integrity check|stream")
	}
}

func integrityCheckCmd(args []string) error {
	fs := flag.NewFlagSet("integrity check", flag.ContinueOnError)
	full := fs.Bool("full", false, "verify all retained relevant protocol state")
	jsonOutput := fs.Bool("json", false, "print JSON")
	strict := fs.Bool("strict", false, "treat warnings as a failing result")
	timeout := fs.Duration("timeout", 30*time.Second, "overall check timeout")
	maxFindings := fs.Int("max-findings", integrity.DefaultMaxFindings, "maximum detailed findings")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *timeout <= 0 || *maxFindings < 1 || *maxFindings > integrity.MaximumMaxFindings {
		return fmt.Errorf("usage: emitlane integrity check [--full] [--json] [--strict] [--timeout 30s] [--max-findings 100]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	verifier, pool, err := openIntegrityVerifier(ctx, *timeout, *maxFindings)
	if err != nil {
		return err
	}
	defer pool.Close()
	mode := integrity.ModeSummary
	if *full {
		mode = integrity.ModeFull
	}
	report, err := verifier.Check(ctx, mode)
	if err != nil {
		return err
	}
	if *jsonOutput {
		if err := writeJSON(report); err != nil {
			return err
		}
	} else {
		printIntegrityReport(report)
	}
	if code := integrity.ExitCode(report, *strict); code != 0 {
		return commandExit(code)
	}
	return nil
}

func integrityStreamCmd(args []string) error {
	fs := flag.NewFlagSet("integrity stream", flag.ContinueOnError)
	destination := fs.String("destination", "", "destination")
	key := fs.String("key", "", "ordering key")
	jsonOutput := fs.Bool("json", false, "print JSON")
	strict := fs.Bool("strict", false, "treat warnings as a failing result")
	timeout := fs.Duration("timeout", 30*time.Second, "overall check timeout")
	maxFindings := fs.Int("max-findings", integrity.DefaultMaxFindings, "maximum detailed findings")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *destination == "" || *key == "" || *timeout <= 0 || *maxFindings < 1 || *maxFindings > integrity.MaximumMaxFindings {
		return fmt.Errorf("usage: emitlane integrity stream --destination name --key key [--json] [--strict] [--timeout 30s] [--max-findings 100]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	verifier, pool, err := openIntegrityVerifier(ctx, *timeout, *maxFindings)
	if err != nil {
		return err
	}
	defer pool.Close()
	report, err := verifier.InspectStream(ctx, *destination, *key)
	if err != nil {
		return err
	}
	if *jsonOutput {
		if err := writeJSON(report); err != nil {
			return err
		}
	} else {
		printIntegrityStream(report)
	}
	if code := integrity.ExitCode(report.Report, *strict); code != 0 {
		return commandExit(code)
	}
	return nil
}

func openIntegrityVerifier(ctx context.Context, statementTimeout time.Duration, maxFindings int) (*integrity.Verifier, *pgxpool.Pool, error) {
	databaseURL, err := requireDatabaseURL()
	if err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	config := integrity.DefaultConfig()
	config.StatementTimeout = statementTimeout
	config.MaxFindings = maxFindings
	verifier, err := integrity.NewVerifier(pool, config)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return verifier, pool, nil
}

func printIntegrityReport(report integrity.Report) {
	fmt.Printf("integrity %s: %s\n", report.Mode, report.Result)
	fmt.Printf("violations: %d  warnings: %d  findings: %d", report.Summary.Violations, report.Summary.Warnings, len(report.Findings))
	if report.FindingsTruncated {
		fmt.Print(" (truncated)")
	}
	fmt.Println()
	fmt.Printf("events: %d pending, %d inflight, %d dead, %d stale leases\n",
		report.Summary.PendingEvents, report.Summary.InflightEvents, report.Summary.DeadEvents, report.Summary.StaleEventLeases)
	fmt.Printf("inbox: %d pending, %d inflight, %d retry wait, %d processed, %d dead, %d stale leases\n",
		report.Summary.InboxPending, report.Summary.InboxInflight, report.Summary.InboxRetryWait,
		report.Summary.InboxProcessed, report.Summary.InboxDead, report.Summary.InboxStaleLeases)
	fmt.Printf("ordering: %d streams, %d blocked (%d gap, %d retry wait, %d dead), %d/%d partitions owned\n",
		report.Summary.OrderedStreams, report.Summary.BlockedStreams, report.Summary.GapStreams,
		report.Summary.RetryWaitStreams, report.Summary.DeadBlockedStreams,
		report.Summary.OwnedPartitions, report.Summary.OrderingPartitions)
	printIntegrityFindings(report.Findings)
}

func printIntegrityStream(report integrity.StreamInspection) {
	fmt.Printf("integrity stream: %s\n", report.Result)
	fmt.Printf("destination: %s\nordering key: %s\nexpected partition: %d\n", report.Destination, report.OrderingKey, report.ExpectedPartition)
	if report.Stream != nil {
		fmt.Printf("cursor: start=%d next=%d partition=%d\n", report.Stream.StartSequence, report.Stream.NextSequence, report.Stream.PartitionID)
	}
	fmt.Printf("condition: %s\n", report.BlockingCondition)
	if report.Partition != nil {
		fmt.Printf("ownership: owner=%s epoch=%d", report.Partition.LeaseOwner, report.Partition.Epoch)
		if report.Partition.LeaseUntil != nil {
			fmt.Printf(" lease_until=%s", report.Partition.LeaseUntil.Format(time.RFC3339))
		}
		if report.Partition.HandoffNotBefore != nil {
			fmt.Printf(" handoff_not_before=%s", report.Partition.HandoffNotBefore.Format(time.RFC3339))
		}
		fmt.Println()
	}
	fmt.Printf("retained relevant events: %d", len(report.Events))
	if report.EventsTruncated {
		fmt.Print(" (truncated)")
	}
	fmt.Println()
	for _, event := range report.Events {
		fmt.Printf("  seq=%d status=%s attempts=%d id=%s\n", event.Sequence, event.Status, event.Attempts, event.ID)
	}
	printIntegrityFindings(report.Findings)
}

func printIntegrityFindings(findings []integrity.Finding) {
	if len(findings) == 0 {
		return
	}
	fmt.Fprintln(os.Stdout, "findings:")
	for _, finding := range findings {
		fmt.Printf("  [%s] %s: %s", finding.Severity, finding.Code, finding.Message)
		if finding.Destination != "" {
			fmt.Printf(" destination=%s", finding.Destination)
		}
		if finding.Consumer != "" {
			fmt.Printf(" consumer=%s", finding.Consumer)
		}
		if finding.EventID != "" {
			fmt.Printf(" event_id=%s", finding.EventID)
		}
		if finding.PartitionID != nil {
			fmt.Printf(" partition=%d", *finding.PartitionID)
		}
		if finding.SourcePartition != nil {
			fmt.Printf(" source_partition=%d", *finding.SourcePartition)
		}
		fmt.Println()
	}
}
