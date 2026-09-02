package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/storage/postgres"
)

func migrateCmd(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: emitlane migrate up|down")
	}
	url, err := requireDatabaseURL()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	switch args[0] {
	case "up":
		if err := postgres.MigrateUp(ctx, pool); err != nil {
			return err
		}
		v, err := postgres.SchemaVersion(ctx, pool)
		if err != nil {
			return err
		}
		fmt.Printf("migrations applied: schema version %d\n", v)
		return nil
	case "down":
		if err := postgres.MigrateDown(ctx, pool); err != nil {
			return err
		}
		fmt.Println("migration rolled back")
		return nil
	default:
		return fmt.Errorf("usage: emitlane migrate up|down")
	}
}

func requireDatabaseURL() (string, error) {
	url := strings.TrimSpace(os.Getenv("EMITLANE_DATABASE_URL"))
	if url == "" {
		return "", fmt.Errorf("EMITLANE_DATABASE_URL is required")
	}
	return url, nil
}
