package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/relay"
	"github.com/emitlane/emitlane/storage/postgres"
)

func deadCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: emitlane dead list|retry <event-id>")
	}
	url, err := requireDatabaseURL()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	store, err := postgres.NewStore(pool)
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: emitlane dead list")
		}
		events, err := store.ListDead(ctx, 100)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			fmt.Println("no dead events")
			return nil
		}
		fmt.Printf("%-36s  %-24s  %8s  %s\n", "ID", "TYPE", "ATTEMPTS", "LAST_ERROR")
		for _, ev := range events {
			errMsg := oneLine(ev.LastError, 80)
			fmt.Printf("%-36s  %-24s  %8d  %s\n", ev.ID, ev.Type, ev.Attempts, errMsg)
		}
		return nil
	case "retry":
		if len(args) < 2 {
			return fmt.Errorf("usage: emitlane dead retry <event-id> [--reason reason]")
		}
		id, err := uuid.Parse(args[1])
		if err != nil {
			return fmt.Errorf("event-id must be a UUID")
		}
		fs := flag.NewFlagSet("dead retry", flag.ContinueOnError)
		reason := fs.String("reason", "", "operator reason")
		if err := fs.Parse(args[2:]); err != nil || fs.NArg() != 0 {
			return fmt.Errorf("usage: emitlane dead retry <event-id> [--reason reason]")
		}
		service, err := adminapi.NewService(store, relay.DefaultConfig().PresenceStaleAfter, nil)
		if err != nil {
			return err
		}
		if err := service.RetryDead(ctx, id, adminapi.Mutation{Actor: "cli", Reason: *reason}); err != nil {
			return err
		}
		fmt.Printf("event %s moved to pending\n", id)
		return nil
	default:
		return fmt.Errorf("usage: emitlane dead list|retry <event-id>")
	}
}

func oneLine(value string, maxRunes int) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	runes := []rune(value)
	if len(runes) > maxRunes {
		runes = runes[:maxRunes]
	}
	return string(runes)
}
