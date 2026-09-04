package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emitlane/emitlane/migrations"
)

const currentSchemaVersion = 3

const migrationLockID int64 = 0x454d49544c414e45 // "EMITLANE"

const ensureMigrations = `
CREATE SCHEMA IF NOT EXISTS emitlane;

CREATE TABLE IF NOT EXISTS emitlane.schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

// CurrentSchemaVersion is the latest migration this binary knows how to apply.
func CurrentSchemaVersion() int {
	return currentSchemaVersion
}

// MigrateUp applies all embedded up migrations that have not been recorded.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := beginMigration(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, ensureMigrations); err != nil {
		return fmt.Errorf("migrate: ensure schema: %w", err)
	}
	applied, err := appliedVersions(ctx, tx)
	if err != nil {
		return err
	}
	entries, err := migrationFiles(".up.sql")
	if err != nil {
		return err
	}
	for _, e := range entries {
		version, err := versionOf(e.Name())
		if err != nil {
			return err
		}
		if applied[version] {
			continue
		}
		body, err := fs.ReadFile(migrations.SQL, e.Name())
		if err != nil {
			return fmt.Errorf("migrate: read %s: %w", e.Name(), err)
		}
		if strings.TrimSpace(string(body)) != "" {
			if _, err := tx.Exec(ctx, string(body)); err != nil {
				return fmt.Errorf("migrate: apply version %d: %w", version, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO emitlane.schema_migrations (version) VALUES ($1)`, version); err != nil {
			return fmt.Errorf("migrate: record up %d: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit up: %w", err)
	}
	return nil
}

// MigrateDown rolls back the latest applied migration.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := beginMigration(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, ensureMigrations); err != nil {
		return fmt.Errorf("migrate: ensure schema: %w", err)
	}
	var version *int
	err = tx.QueryRow(ctx, `SELECT MAX(version) FROM emitlane.schema_migrations`).Scan(&version)
	if err != nil {
		return fmt.Errorf("migrate: current version: %w", err)
	}
	if version == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrate: commit down: %w", err)
		}
		return nil
	}
	name, err := downMigrationName(*version)
	if err != nil {
		return err
	}
	body, err := fs.ReadFile(migrations.SQL, name)
	if err != nil {
		return fmt.Errorf("migrate: read down for version %d: %w", *version, err)
	}
	if strings.TrimSpace(string(body)) != "" {
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("migrate: apply version %d: %w", *version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit down: %w", err)
	}
	return nil
}

// SchemaVersion returns the current applied migration version, or 0 if none.
func SchemaVersion(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'emitlane' AND table_name = 'schema_migrations'
    )`).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	var version *int
	if err := pool.QueryRow(ctx, `SELECT MAX(version) FROM emitlane.schema_migrations`).Scan(&version); err != nil {
		return 0, err
	}
	if version == nil {
		return 0, nil
	}
	return *version, nil
}

func beginMigration(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: begin: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("migrate: acquire lock: %w", err)
	}
	return tx, nil
}

func appliedVersions(ctx context.Context, tx pgx.Tx) (map[int]bool, error) {
	rows, err := tx.Query(ctx, `SELECT version FROM emitlane.schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("migrate: list versions: %w", err)
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func migrationFiles(suffix string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(migrations.SQL, ".")
	if err != nil {
		return nil, fmt.Errorf("migrate: list: %w", err)
	}
	var filtered []fs.DirEntry
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

func versionOf(name string) (int, error) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("migrate: unexpected name %s", name)
	}
	v, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("migrate: version in %s: %w", name, err)
	}
	return v, nil
}

func downMigrationName(version int) (string, error) {
	entries, err := migrationFiles(".down.sql")
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		v, err := versionOf(entry.Name())
		if err != nil {
			return "", err
		}
		if v == version {
			return entry.Name(), nil
		}
	}
	return "", fmt.Errorf("migrate: down migration for version %d not found", version)
}

// RequiredIndexes are the indexes this binary relies on for delivery and
// bounded operational queries.
func RequiredIndexes() []string {
	return []string{
		"outbox_pending_idx",
		"outbox_inflight_lease_idx",
		"outbox_dead_idx",
		"outbox_created_idx",
		"outbox_status_created_idx",
		"outbox_destination_type_created_idx",
		"outbox_replay_batch_idx",
		"admin_audit_created_idx",
		"outbox_ordering_sequence_unique_idx",
		"outbox_ordered_claim_idx",
		"ordering_stream_partition_idx",
	}
}

func RequiredTables() []string {
	return []string{
		"outbox_events",
		"inbox_events",
		"runtime_control",
		"relay_instances",
		"admin_audit_log",
		"ordering_streams",
		"ordering_partitions",
	}
}

func RequiredConstraints() []struct{ Table, Name string } {
	return []struct{ Table, Name string }{
		{Table: "outbox_events", Name: "outbox_ordering_state_check"},
		{Table: "ordering_streams", Name: "ordering_stream_partition_check"},
		{Table: "ordering_streams", Name: "ordering_stream_start_check"},
		{Table: "ordering_streams", Name: "ordering_stream_next_check"},
		{Table: "ordering_partitions", Name: "ordering_partition_lease_check"},
		{Table: "ordering_partitions", Name: "ordering_partition_epoch_check"},
	}
}

func TableExists(ctx context.Context, pool *pgxpool.Pool, tableName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'emitlane' AND table_name = $1
    )`, tableName).Scan(&exists)
	return exists, err
}

// ColumnExists reports whether columnName exists on a table in the emitlane
// schema. Doctor uses this to detect a migration record that does not match the
// actual v0.2 schema.
func ColumnExists(ctx context.Context, pool *pgxpool.Pool, tableName, columnName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'emitlane' AND table_name = $1 AND column_name = $2
    )`, tableName, columnName).Scan(&exists)
	return exists, err
}

func RuntimeControlExists(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM emitlane.runtime_control WHERE singleton = TRUE
    )`).Scan(&exists)
	return exists, err
}

// IndexExists reports whether indexName exists in the emitlane schema.
func IndexExists(ctx context.Context, pool *pgxpool.Pool, indexName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'emitlane' AND indexname = $1
)`, indexName).Scan(&exists)
	return exists, err
}

func ConstraintExists(ctx context.Context, pool *pgxpool.Pool, tableName, constraintName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_schema='emitlane' AND table_name=$1 AND constraint_name=$2
)`, tableName, constraintName).Scan(&exists)
	return exists, err
}

func OrderingPartitionCount(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.ordering_partitions`).Scan(&count)
	return count, err
}

// SchemaExists reports whether the emitlane schema is present.
func SchemaExists(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM information_schema.schemata WHERE schema_name = 'emitlane'
    )`).Scan(&exists)
	return exists, err
}

// PingListenNotify verifies LISTEN/NOTIFY on a dedicated connection.
func PingListenNotify(ctx context.Context, databaseURL string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, conn)
	if _, err := conn.Exec(ctx, `LISTEN emitlane_events`); err != nil {
		return fmt.Errorf("LISTEN: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_notify('emitlane_events', 'doctor')`); err != nil {
		return fmt.Errorf("NOTIFY: %w", err)
	}
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		return fmt.Errorf("wait for notification: %w", err)
	}
	if n == nil || n.Channel != notifyChannel {
		return fmt.Errorf("unexpected notification")
	}
	return nil
}

// PingControlNotify verifies the low-latency pause/resume notification path.
func PingControlNotify(ctx context.Context, databaseURL string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, conn)
	if _, err := conn.Exec(ctx, `LISTEN emitlane_control`); err != nil {
		return fmt.Errorf("LISTEN control: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_notify('emitlane_control', 'doctor')`); err != nil {
		return fmt.Errorf("NOTIFY control: %w", err)
	}
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		return fmt.Errorf("wait for control notification: %w", err)
	}
	if n == nil || n.Channel != "emitlane_control" {
		return fmt.Errorf("unexpected control notification")
	}
	return nil
}

// RelayPrivileges reports the table privileges needed by the standalone relay.
// Delete is required only when delivered-event cleanup is enabled.
type RelayPrivileges struct {
	SchemaUsage bool
	Select      bool
	Update      bool
	Delete      bool
}

type OperabilityPrivileges struct {
	ControlSelect  bool
	ControlUpdate  bool
	PresenceSelect bool
	PresenceInsert bool
	PresenceUpdate bool
	OutboxInsert   bool
	AuditSelect    bool
	AuditInsert    bool
}

type OrderingPrivileges struct {
	StreamsSelect    bool
	StreamsInsert    bool
	StreamsUpdate    bool
	PartitionsSelect bool
	PartitionsUpdate bool
}

// CheckRelayPrivileges inspects the current PostgreSQL role without mutating
// application data.
func CheckRelayPrivileges(ctx context.Context, pool *pgxpool.Pool) (RelayPrivileges, error) {
	var p RelayPrivileges
	err := pool.QueryRow(ctx, `
SELECT
    has_schema_privilege(current_user, 'emitlane', 'USAGE'),
    has_table_privilege(current_user, 'emitlane.outbox_events', 'SELECT'),
    has_table_privilege(current_user, 'emitlane.outbox_events', 'UPDATE'),
    has_table_privilege(current_user, 'emitlane.outbox_events', 'DELETE')
`).Scan(&p.SchemaUsage, &p.Select, &p.Update, &p.Delete)
	if err != nil {
		return RelayPrivileges{}, fmt.Errorf("check relay privileges: %w", err)
	}
	return p, nil
}

func CheckOperabilityPrivileges(ctx context.Context, pool *pgxpool.Pool) (OperabilityPrivileges, error) {
	var p OperabilityPrivileges
	err := pool.QueryRow(ctx, `
SELECT
    has_table_privilege(current_user, 'emitlane.runtime_control', 'SELECT'),
    has_table_privilege(current_user, 'emitlane.runtime_control', 'UPDATE'),
    has_table_privilege(current_user, 'emitlane.relay_instances', 'SELECT'),
    has_table_privilege(current_user, 'emitlane.relay_instances', 'INSERT'),
    has_table_privilege(current_user, 'emitlane.relay_instances', 'UPDATE'),
    has_table_privilege(current_user, 'emitlane.outbox_events', 'INSERT'),
    has_table_privilege(current_user, 'emitlane.admin_audit_log', 'SELECT'),
    has_table_privilege(current_user, 'emitlane.admin_audit_log', 'INSERT')
`).Scan(&p.ControlSelect, &p.ControlUpdate, &p.PresenceSelect, &p.PresenceInsert,
		&p.PresenceUpdate, &p.OutboxInsert, &p.AuditSelect, &p.AuditInsert)
	if err != nil {
		return OperabilityPrivileges{}, fmt.Errorf("check operability privileges: %w", err)
	}
	return p, nil
}

func CheckOrderingPrivileges(ctx context.Context, pool *pgxpool.Pool) (OrderingPrivileges, error) {
	var privileges OrderingPrivileges
	err := pool.QueryRow(ctx, `
SELECT
    has_table_privilege(current_user, 'emitlane.ordering_streams', 'SELECT'),
    has_table_privilege(current_user, 'emitlane.ordering_streams', 'INSERT'),
    has_table_privilege(current_user, 'emitlane.ordering_streams', 'UPDATE'),
    has_table_privilege(current_user, 'emitlane.ordering_partitions', 'SELECT'),
    has_table_privilege(current_user, 'emitlane.ordering_partitions', 'UPDATE')
`).Scan(&privileges.StreamsSelect, &privileges.StreamsInsert, &privileges.StreamsUpdate,
		&privileges.PartitionsSelect, &privileges.PartitionsUpdate)
	if err != nil {
		return OrderingPrivileges{}, fmt.Errorf("check ordering privileges: %w", err)
	}
	return privileges, nil
}
