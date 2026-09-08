DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM emitlane.inbox_events
        WHERE status <> 'processed' OR processed_at IS NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'cannot downgrade EmitLane schema v4 while active managed Inbox state exists',
            HINT = 'process or deliberately retire pending, inflight, retry_wait, and dead Inbox rows before retrying';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS inbox_legacy_insert_guard ON emitlane.inbox_events;
DROP FUNCTION IF EXISTS emitlane.guard_inbox_legacy_insert();

DROP INDEX IF EXISTS emitlane.inbox_processed_retention_idx;
DROP INDEX IF EXISTS emitlane.inbox_source_unique_idx;
DROP INDEX IF EXISTS emitlane.inbox_dead_idx;
DROP INDEX IF EXISTS emitlane.inbox_expired_lease_idx;
DROP INDEX IF EXISTS emitlane.inbox_due_idx;

ALTER TABLE emitlane.inbox_events
    DROP CONSTRAINT IF EXISTS inbox_source_metadata_check,
    DROP CONSTRAINT IF EXISTS inbox_processed_state_check,
    DROP CONSTRAINT IF EXISTS inbox_lease_state_check,
    DROP CONSTRAINT IF EXISTS inbox_attempts_check,
    DROP CONSTRAINT IF EXISTS inbox_status_check,
    DROP COLUMN IF EXISTS source_timestamp,
    DROP COLUMN IF EXISTS source_offset,
    DROP COLUMN IF EXISTS source_partition,
    DROP COLUMN IF EXISTS source_topic,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS first_seen_at,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS lease_until,
    DROP COLUMN IF EXISTS lease_token,
    DROP COLUMN IF EXISTS lease_owner,
    DROP COLUMN IF EXISTS available_at,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS status,
    ALTER COLUMN processed_at SET NOT NULL;

DELETE FROM emitlane.schema_migrations WHERE version = 4;
