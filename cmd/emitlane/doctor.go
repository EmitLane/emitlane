package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/config"
	"github.com/emitlane/emitlane/storage/postgres"
)

func doctorCmd(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: emitlane doctor")
	}
	fmt.Println("EmitLane doctor")
	fmt.Println()

	failed := 0
	check := func(name string, err error, detail string) {
		if err != nil {
			failed++
			fmt.Printf("✗ %s\n  %s\n", name, err)
			return
		}
		if detail != "" {
			fmt.Printf("✓ %s\n  %s\n", name, detail)
			return
		}
		fmt.Printf("✓ %s\n", name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	loadedConfig, configErr := config.Load()
	if configErr != nil {
		check("configuration", configErr, "")
	} else {
		detail := "admin API disabled"
		if loadedConfig.Admin.Enabled {
			detail = "admin API enabled on " + loadedConfig.Admin.Addr
		}
		check("configuration", nil, detail)
	}

	url, urlErr := requireDatabaseURL()
	var pool *pgxpool.Pool
	if urlErr != nil {
		check("PostgreSQL connection", urlErr, "")
	} else {
		var err error
		pool, err = pgxpool.New(ctx, url)
		if err != nil {
			check("PostgreSQL connection", err, "")
		} else {
			defer pool.Close()
			if err := pool.Ping(ctx); err != nil {
				check("PostgreSQL connection", err, "")
			} else {
				check("PostgreSQL connection", nil, "")
			}
		}
	}

	if pool != nil {
		exists, err := postgres.SchemaExists(ctx, pool)
		if err != nil {
			check("EmitLane schema", err, "")
		} else if !exists {
			check("EmitLane schema", fmt.Errorf("schema emitlane does not exist; run emitlane migrate up"), "")
		} else {
			v, verr := postgres.SchemaVersion(ctx, pool)
			if verr != nil {
				check("EmitLane schema", verr, "")
			} else {
				check("EmitLane schema", nil, fmt.Sprintf("version %d (binary expects %d)", v, postgres.CurrentSchemaVersion()))
			}
			if v != postgres.CurrentSchemaVersion() {
				failed++
				fmt.Printf("✗ migrations\n  applied version %d, expected %d\n", v, postgres.CurrentSchemaVersion())
			} else {
				check("migrations", nil, fmt.Sprintf("schema version %d", v))
			}
		}
		for _, idx := range postgres.RequiredIndexes() {
			ok, ierr := postgres.IndexExists(ctx, pool, idx)
			if ierr != nil {
				check(idx, ierr, "")
				continue
			}
			if !ok {
				check(idx, fmt.Errorf("missing"), "")
				continue
			}
			check(idx, nil, "")
		}
		for _, table := range postgres.RequiredTables() {
			ok, tableErr := postgres.TableExists(ctx, pool, table)
			if tableErr != nil {
				check("table "+table, tableErr, "")
			} else if !ok {
				check("table "+table, fmt.Errorf("missing"), "")
			} else {
				check("table "+table, nil, "")
			}
		}
		for _, column := range []string{"replayed_from_event_id", "replay_batch_id"} {
			ok, columnErr := postgres.ColumnExists(ctx, pool, "outbox_events", column)
			if columnErr != nil {
				check("outbox_events."+column, columnErr, "")
			} else if !ok {
				check("outbox_events."+column, fmt.Errorf("missing"), "")
			} else {
				check("outbox_events."+column, nil, "")
			}
		}
		controlExists, controlErr := postgres.RuntimeControlExists(ctx, pool)
		if controlErr != nil {
			check("runtime control singleton", controlErr, "")
		} else if !controlExists {
			check("runtime control singleton", fmt.Errorf("missing"), "")
		} else {
			check("runtime control singleton", nil, "")
		}
		privileges, err := postgres.CheckRelayPrivileges(ctx, pool)
		if err != nil {
			check("relay database permissions", err, "")
		} else {
			missing := make([]string, 0, 4)
			if !privileges.SchemaUsage {
				missing = append(missing, "USAGE on schema emitlane")
			}
			if !privileges.Select {
				missing = append(missing, "SELECT on emitlane.outbox_events")
			}
			if !privileges.Update {
				missing = append(missing, "UPDATE on emitlane.outbox_events")
			}
			if len(missing) > 0 {
				check("relay database permissions", fmt.Errorf("missing %s", strings.Join(missing, ", ")), "")
			} else {
				check("relay database permissions", nil, "SELECT and UPDATE available")
			}
			if !privileges.Delete {
				fmt.Println("! delivered cleanup permission\n  DELETE is unavailable; set EMITLANE_RETENTION_DELIVERED=0 or grant DELETE")
			}
		}
		operability, operabilityErr := postgres.CheckOperabilityPrivileges(ctx, pool)
		if operabilityErr != nil {
			check("v0.2 runtime database permissions", operabilityErr, "")
		} else {
			missing := make([]string, 0, 8)
			if !operability.ControlSelect {
				missing = append(missing, "SELECT runtime_control")
			}
			if !operability.PresenceSelect || !operability.PresenceInsert || !operability.PresenceUpdate {
				missing = append(missing, "SELECT/INSERT/UPDATE relay_instances")
			}
			if loadedConfig.Admin.Enabled {
				if !operability.ControlUpdate {
					missing = append(missing, "UPDATE runtime_control")
				}
				if !operability.OutboxInsert {
					missing = append(missing, "INSERT outbox_events")
				}
				if !operability.AuditSelect || !operability.AuditInsert {
					missing = append(missing, "SELECT/INSERT admin_audit_log")
				}
			}
			if len(missing) > 0 {
				check("v0.2 runtime database permissions", fmt.Errorf("missing %s", strings.Join(missing, ", ")), "")
			} else {
				check("v0.2 runtime database permissions", nil, "available")
			}
		}
		nctx, ncancel := context.WithTimeout(ctx, 5*time.Second)
		check("LISTEN/NOTIFY", postgres.PingListenNotify(nctx, url), "")
		ncancel()
		controlCtx, controlCancel := context.WithTimeout(ctx, 5*time.Second)
		check("control LISTEN/NOTIFY", postgres.PingControlNotify(controlCtx, url), "")
		controlCancel()
	}

	brokers := strings.TrimSpace(os.Getenv("EMITLANE_KAFKA_BROKERS"))
	if brokers == "" {
		check("Kafka broker connectivity", fmt.Errorf("EMITLANE_KAFKA_BROKERS is required"), "")
	} else {
		pub, err := kafka.NewPublisher(kafka.Config{
			Brokers:          splitCSV(brokers),
			ClientID:         "emitlane-doctor",
			PublishTimeout:   5 * time.Second,
			AutoCreateTopics: false,
		})
		if err != nil {
			check("Kafka broker connectivity", err, "")
		} else {
			pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
			err = pub.Ping(pctx)
			pcancel()
			_ = pub.Close()
			check("Kafka broker connectivity", err, brokers)
		}
	}

	fmt.Println()
	if failed > 0 {
		fmt.Printf("%d check(s) failed.\n", failed)
		return fmt.Errorf("%d critical check(s) failed", failed)
	}
	fmt.Println("All checks passed.")
	return nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
