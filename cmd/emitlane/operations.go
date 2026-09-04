package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/relay"
	"github.com/emitlane/emitlane/storage/postgres"
)

func openAdminService(ctx context.Context) (*adminapi.Service, *pgxpool.Pool, error) {
	url, err := requireDatabaseURL()
	if err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, nil, err
	}
	if err := requireCurrentSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, nil, err
	}
	store, err := postgres.NewStore(pool)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	service, err := adminapi.NewService(store, relay.DefaultConfig().PresenceStaleAfter, nil)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return service, pool, nil
}

func writeJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func statsCmd(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane stats [--json]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	stats, err := service.Stats(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(stats)
	}
	fmt.Printf("paused: %t\npending: %d (%d due, %d scheduled)\ninflight: %d\ndelivered retained: %d\ndead: %d\noldest pending: %.1fs\nrelays: %d active, %d stale, %d stopped\n",
		stats.Paused, stats.Pending, stats.PendingDue, stats.PendingScheduled, stats.Inflight, stats.DeliveredRetained, stats.Dead,
		stats.OldestPendingSeconds, stats.ActiveRelays, stats.StaleRelays, stats.StoppedRelays)
	fmt.Printf("ordering: %d streams, %d blocked (%d gap, %d dead), %d owned partitions, %d handoff\n",
		stats.OrderedStreams, stats.BlockedOrderedStreams, stats.GapStreams, stats.DeadBlockedStreams,
		stats.OwnedPartitions, stats.HandoffPartitions)
	return nil
}

func eventsCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane events list|inspect")
	}
	switch args[0] {
	case "list":
		return eventsListCmd(args[1:])
	case "inspect":
		return eventsInspectCmd(args[1:])
	default:
		return fmt.Errorf("usage: emitlane events list|inspect")
	}
}

func eventsListCmd(args []string) error {
	fs := flag.NewFlagSet("events list", flag.ContinueOnError)
	status := fs.String("status", "", "comma-separated statuses")
	destination := fs.String("destination", "", "destination")
	eventType := fs.String("type", "", "event type")
	from := fs.String("from", "", "created from RFC3339")
	to := fs.String("to", "", "created before RFC3339")
	limit := fs.Int("limit", adminapi.DefaultPageSize, "page size")
	cursorRaw := fs.String("cursor", "", "pagination cursor")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane events list [--status status] [--destination name] [--type type] [--json]")
	}
	filter, err := cliEventFilter(*status, *destination, *eventType, *from, *to, *limit, *cursorRaw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	page, err := service.ListEvents(ctx, filter)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(page)
	}
	if len(page.Events) == 0 {
		fmt.Println("no events")
		return nil
	}
	fmt.Printf("%-36s  %-10s  %-24s  %-24s  %8s  %s\n", "ID", "STATUS", "DESTINATION", "TYPE", "ATTEMPTS", "CREATED")
	for _, event := range page.Events {
		fmt.Printf("%-36s  %-10s  %-24s  %-24s  %8d  %s\n", event.ID, event.Status,
			oneLine(event.Destination, 24), oneLine(event.Type, 24), event.Attempts, event.CreatedAt.Format(time.RFC3339))
	}
	if page.NextCursor != "" {
		fmt.Printf("next cursor: %s\n", page.NextCursor)
	}
	return nil
}

func eventsInspectCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane events inspect <event-id> [--json] [--payload]")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return fmt.Errorf("event-id must be a UUID")
	}
	fs := flag.NewFlagSet("events inspect", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "print JSON")
	payload := fs.Bool("payload", false, "include base64 payload, key and headers")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane events inspect <event-id> [--json] [--payload]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	event, err := service.InspectEvent(ctx, id, *payload)
	if err != nil {
		return err
	}
	if *jsonOutput || *payload {
		return writeJSON(event)
	}
	fmt.Printf("id: %s\nstatus: %s\ndestination: %s\ntype: %s\nattempts: %d\ncreated: %s\navailable: %s\n",
		event.ID, event.Status, event.Destination, event.Type, event.Attempts,
		event.CreatedAt.Format(time.RFC3339), event.AvailableAt.Format(time.RFC3339))
	if event.LastError != "" {
		fmt.Printf("last error: %s\n", oneLine(event.LastError, 500))
	}
	if event.ReplayedFromEventID != nil {
		fmt.Printf("replayed from: %s\nreplay batch: %s\n", event.ReplayedFromEventID, event.ReplayBatchID)
	}
	if event.OrderingKey != "" {
		fmt.Printf("ordering key: %s\nsequence: %d\npartition: %d\n",
			event.OrderingKey, event.OrderingSequence, *event.OrderingPartition)
	}
	return nil
}

func relayCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane relay status|pause|resume")
	}
	if args[0] == "status" {
		fs := flag.NewFlagSet("relay status", flag.ContinueOnError)
		jsonOutput := fs.Bool("json", false, "print JSON")
		if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
			return fmt.Errorf("usage: emitlane relay status [--json]")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		service, pool, err := openAdminService(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()
		state, err := service.ControlState(ctx)
		if err != nil {
			return err
		}
		instances, err := service.ListRelays(ctx)
		if err != nil {
			return err
		}
		if *jsonOutput {
			return writeJSON(map[string]any{"control": state, "relays": instances})
		}
		fmt.Printf("paused: %t\nreason: %s\nupdated by: %s at %s\n", state.Paused, state.Reason, state.UpdatedBy, state.UpdatedAt.Format(time.RFC3339))
		for _, instance := range instances {
			fmt.Printf("%s  %-7s  %s  %s\n", instance.InstanceID, instance.State, instance.Hostname, instance.Version)
		}
		return nil
	}
	if args[0] != "pause" && args[0] != "resume" {
		return fmt.Errorf("usage: emitlane relay status|pause|resume")
	}
	fs := flag.NewFlagSet("relay "+args[0], flag.ContinueOnError)
	reason := fs.String("reason", "", "operator reason")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane relay %s --reason reason", args[0])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	state, err := service.SetPaused(ctx, args[0] == "pause", adminapi.Mutation{Actor: "cli", Reason: *reason})
	if err != nil {
		return err
	}
	fmt.Printf("relay paused: %t\n", state.Paused)
	return nil
}

func replayCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane replay event|range")
	}
	switch args[0] {
	case "event":
		return replayEventCmd(args[1:])
	case "range":
		return replayRangeCmd(args[1:])
	default:
		return fmt.Errorf("usage: emitlane replay event|range")
	}
}

func replayEventCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane replay event <event-id> --reason reason")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return fmt.Errorf("event-id must be a UUID")
	}
	fs := flag.NewFlagSet("replay event", flag.ContinueOnError)
	reason := fs.String("reason", "", "operator reason")
	jsonOutput := fs.Bool("json", false, "print JSON")
	unordered := fs.Bool("unordered", false, "replay an ordered source outside its historical stream")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane replay event <event-id> --reason reason")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	mutation := adminapi.Mutation{Actor: "cli", Reason: *reason}
	if *unordered {
		mutation.OrderingMode = "unordered"
	}
	result, err := service.ReplayEvent(ctx, id, mutation)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(result)
	}
	fmt.Printf("created replay event %s in batch %s\n", result.EventIDs[0], result.ReplayBatchID)
	return nil
}

func replayRangeCmd(args []string) error {
	fs := flag.NewFlagSet("replay range", flag.ContinueOnError)
	status := fs.String("status", "", "delivered or dead (default delivered)")
	destination := fs.String("destination", "", "destination")
	eventType := fs.String("type", "", "event type")
	from := fs.String("from", "", "created from RFC3339")
	to := fs.String("to", "", "created before RFC3339")
	reason := fs.String("reason", "", "operator reason")
	limit := fs.Int("limit", adminapi.MaxReplayBatch, "maximum events to replay")
	execute := fs.Bool("execute", false, "execute the replay; default is preview")
	jsonOutput := fs.Bool("json", false, "print JSON")
	unordered := fs.Bool("unordered", false, "replay ordered sources outside their historical streams")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane replay range [selector] --reason reason [--execute]")
	}
	filter, err := cliEventFilter(*status, *destination, *eventType, *from, *to, *limit, "")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if !*execute {
		preview, err := service.PreviewReplay(ctx, filter)
		if err != nil {
			return err
		}
		if *jsonOutput {
			return writeJSON(preview)
		}
		fmt.Printf("preview: %d eligible event(s); execution limit: %d\n", preview.Count, preview.Limit)
		for _, event := range preview.Sample {
			fmt.Printf("%s  %s  %s\n", event.ID, event.Status, event.Type)
		}
		fmt.Println("no changes made; pass --execute to create replay events")
		return nil
	}
	mutation := adminapi.Mutation{Actor: "cli", Reason: *reason}
	if *unordered {
		mutation.OrderingMode = "unordered"
	}
	result, err := service.ReplayBatch(ctx, filter, mutation)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(result)
	}
	fmt.Printf("created %d replay event(s) in batch %s\n", result.Created, result.ReplayBatchID)
	return nil
}

func orderingCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: emitlane ordering streams|inspect|partitions")
	}
	switch args[0] {
	case "streams":
		return orderingStreamsCmd(args[1:])
	case "inspect":
		return orderingInspectCmd(args[1:])
	case "partitions":
		return orderingPartitionsCmd(args[1:])
	default:
		return fmt.Errorf("usage: emitlane ordering streams|inspect|partitions")
	}
}

func orderingStreamsCmd(args []string) error {
	fs := flag.NewFlagSet("ordering streams", flag.ContinueOnError)
	state := fs.String("state", "", "computed stream state")
	destination := fs.String("destination", "", "destination")
	partitionRaw := fs.Int("partition", -1, "virtual partition 0..63")
	blocked := fs.Bool("blocked", false, "show only blocked streams")
	limit := fs.Int("limit", adminapi.DefaultPageSize, "page size")
	cursorRaw := fs.String("cursor", "", "pagination cursor")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane ordering streams [--blocked] [--json]")
	}
	cursor, err := adminapi.DecodeOrderingStreamCursor(*cursorRaw)
	if err != nil {
		return err
	}
	var partition *int16
	if *partitionRaw >= 0 {
		value := int16(*partitionRaw)
		partition = &value
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	page, err := service.ListOrderingStreams(ctx, adminapi.OrderingStreamFilter{
		State: *state, Destination: *destination, Partition: partition, BlockedOnly: *blocked,
		Limit: *limit, Cursor: cursor,
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(page)
	}
	for _, stream := range page.Streams {
		fmt.Printf("%-14s  p%02d  next=%-8d  %-24s  %s\n", stream.State, stream.Partition,
			stream.NextSequence, oneLine(stream.Destination, 24), stream.OrderingKey)
	}
	if page.NextCursor != "" {
		fmt.Printf("next cursor: %s\n", page.NextCursor)
	}
	return nil
}

func orderingInspectCmd(args []string) error {
	fs := flag.NewFlagSet("ordering inspect", flag.ContinueOnError)
	destination := fs.String("destination", "", "destination")
	key := fs.String("key", "", "ordering key")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *destination == "" || *key == "" {
		return fmt.Errorf("usage: emitlane ordering inspect --destination name --key key [--json]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	stream, err := service.InspectOrderingStream(ctx, *destination, *key)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(stream)
	}
	fmt.Printf("destination: %s\nordering key: %s\npartition: %d\nstart sequence: %d\nnext sequence: %d\nstate: %s\n",
		stream.Destination, stream.OrderingKey, stream.Partition, stream.StartSequence, stream.NextSequence, stream.State)
	if stream.LowestFutureSequence != nil {
		fmt.Printf("lowest future sequence: %d\ngap size: %d\ngap age: %.1fs\n",
			*stream.LowestFutureSequence, stream.GapSize, stream.GapAgeSeconds)
	}
	return nil
}

func orderingPartitionsCmd(args []string) error {
	fs := flag.NewFlagSet("ordering partitions", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane ordering partitions [--json]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	partitions, err := service.ListOrderingPartitions(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(map[string]any{"partitions": partitions})
	}
	for _, partition := range partitions {
		fmt.Printf("%02d  %-7s  desired=%-24s actual=%-24s epoch=%d\n", partition.PartitionID,
			partition.State, partition.DesiredOwner, partition.ActualOwner, partition.Epoch)
	}
	return nil
}

func auditCmd(args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: emitlane audit list [--action action] [--json]")
	}
	fs := flag.NewFlagSet("audit list", flag.ContinueOnError)
	action := fs.String("action", "", "filter action")
	limit := fs.Int("limit", adminapi.DefaultPageSize, "page size")
	cursorRaw := fs.String("cursor", "", "pagination cursor")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return fmt.Errorf("usage: emitlane audit list [--action action] [--json]")
	}
	cursor, err := adminapi.DecodeCursor(*cursorRaw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, pool, err := openAdminService(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	page, err := service.ListAudit(ctx, adminapi.AuditFilter{Action: *action, Limit: *limit, Cursor: cursor})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(page)
	}
	for _, record := range page.Records {
		fmt.Printf("%s  %-14s  %-12s  %s\n", record.CreatedAt.Format(time.RFC3339), record.Action, record.Actor, oneLine(record.Reason, 80))
	}
	if page.NextCursor != "" {
		fmt.Printf("next cursor: %s\n", page.NextCursor)
	}
	return nil
}

func cliEventFilter(status, destination, eventType, fromRaw, toRaw string, limit int, cursorRaw string) (adminapi.EventFilter, error) {
	var statuses []string
	if strings.TrimSpace(status) != "" {
		statuses = strings.Split(status, ",")
	}
	from, err := cliTime(fromRaw, "from")
	if err != nil {
		return adminapi.EventFilter{}, err
	}
	to, err := cliTime(toRaw, "to")
	if err != nil {
		return adminapi.EventFilter{}, err
	}
	cursor, err := adminapi.DecodeCursor(cursorRaw)
	if err != nil {
		return adminapi.EventFilter{}, err
	}
	return adminapi.EventFilter{Statuses: statuses, Destination: destination, EventType: eventType,
		CreatedFrom: from, CreatedTo: to, Limit: limit, Cursor: cursor}, nil
}

func cliTime(raw, name string) (*time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("--%s must be RFC3339", name)
	}
	return &value, nil
}
