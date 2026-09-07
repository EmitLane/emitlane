package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/google/uuid"

	adminapi "github.com/emitlane/emitlane/internal/admin"
)

func inboxCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane inbox stats|dead|inspect|retry")
	}
	switch args[0] {
	case "stats":
		return inboxStatsCmd(args[1:])
	case "dead":
		return inboxDeadCmd(args[1:])
	case "inspect":
		return inboxInspectCmd(args[1:])
	case "retry":
		return inboxRetryCmd(args[1:])
	default:
		return fmt.Errorf("usage: emitlane inbox stats|dead|inspect|retry")
	}
}

func inboxStatsCmd(args []string) error {
	fs := flag.NewFlagSet("inbox stats", flag.ContinueOnError)
	consumer := fs.String("consumer", "", "consumer name")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane inbox stats [--consumer name] [--json]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	stats, err := service.InboxStats(ctx, *consumer)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(stats)
	}
	fmt.Printf("processed: %d\npending: %d\ninflight: %d (%d stale)\nretry_wait: %d (%d due)\ndead: %d\nblocked partitions: %d\n",
		stats.Processed, stats.Pending, stats.Inflight, stats.StaleInflight,
		stats.RetryWait, stats.DueRetries, stats.Dead, stats.BlockedPartitions)
	return nil
}

func inboxDeadCmd(args []string) error {
	fs := flag.NewFlagSet("inbox dead", flag.ContinueOnError)
	consumer := fs.String("consumer", "", "consumer name")
	limit := fs.Int("limit", adminapi.DefaultPageSize, "maximum rows")
	offset := fs.Int("offset", 0, "row offset")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane inbox dead [--consumer name] [--limit 50] [--offset 0] [--json]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	events, err := service.ListDeadInbox(ctx, adminapi.InboxDeadFilter{Consumer: *consumer, Limit: *limit, Offset: *offset})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(map[string]any{"items": events, "limit": *limit, "offset": *offset})
	}
	if len(events) == 0 {
		fmt.Println("no dead Inbox events")
		return nil
	}
	fmt.Printf("%-24s  %-36s  %8s  %-30s  %s\n", "CONSUMER", "EVENT_ID", "ATTEMPTS", "SOURCE", "LAST_ERROR")
	for _, event := range events {
		source := "-"
		if event.SourcePartition != nil && event.SourceOffset != nil {
			source = fmt.Sprintf("%s[%d]@%d", event.SourceTopic, *event.SourcePartition, *event.SourceOffset)
		}
		fmt.Printf("%-24s  %-36s  %8d  %-30s  %s\n", event.Consumer, event.EventID, event.Attempts, source, oneLine(event.LastError, 80))
	}
	return nil
}

func inboxInspectCmd(args []string) error {
	fs := flag.NewFlagSet("inbox inspect", flag.ContinueOnError)
	consumer := fs.String("consumer", "", "consumer name")
	eventID := fs.String("event-id", "", "event UUID")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane inbox inspect --consumer name --event-id uuid [--json]")
	}
	id, err := uuid.Parse(*eventID)
	if err != nil {
		return fmt.Errorf("event-id must be a UUID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	event, err := service.InspectInbox(ctx, *consumer, id)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(event)
	}
	fmt.Printf("consumer: %s\nevent id: %s\nstatus: %s\nattempts: %d\navailable at: %s\n",
		event.Consumer, event.EventID, event.Status, event.Attempts, event.AvailableAt.Format(time.RFC3339))
	if event.SourcePartition != nil && event.SourceOffset != nil {
		fmt.Printf("source: %s[%d]@%d\n", event.SourceTopic, *event.SourcePartition, *event.SourceOffset)
	}
	if event.LastError != "" {
		fmt.Printf("last error: %s\n", oneLine(event.LastError, 120))
	}
	return nil
}

func inboxRetryCmd(args []string) error {
	fs := flag.NewFlagSet("inbox retry", flag.ContinueOnError)
	consumer := fs.String("consumer", "", "consumer name")
	eventID := fs.String("event-id", "", "event UUID")
	reason := fs.String("reason", "", "required operator reason")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane inbox retry --consumer name --event-id uuid --reason reason")
	}
	id, err := uuid.Parse(*eventID)
	if err != nil {
		return fmt.Errorf("event-id must be a UUID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := service.RetryDeadInbox(ctx, *consumer, id, adminapi.Mutation{Actor: "cli", Reason: *reason}); err != nil {
		return err
	}
	fmt.Printf("Inbox event %s/%s scheduled for retry\n", *consumer, id)
	return nil
}
