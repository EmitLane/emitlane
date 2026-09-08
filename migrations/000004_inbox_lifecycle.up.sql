ALTER TABLE emitlane.inbox_events
    ALTER COLUMN processed_at DROP NOT NULL,
    ADD COLUMN status TEXT NOT NULL DEFAULT 'processed',
    ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN lease_owner TEXT,
    ADD COLUMN lease_token UUID,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD COLUMN last_error TEXT,
    ADD COLUMN first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN source_topic TEXT,
    ADD COLUMN source_partition INTEGER,
    ADD COLUMN source_offset BIGINT,
    ADD COLUMN source_timestamp TIMESTAMPTZ,
    ADD CONSTRAINT inbox_status_check CHECK (
        status IN ('pending', 'inflight', 'retry_wait', 'processed', 'dead')
    ),
    ADD CONSTRAINT inbox_attempts_check CHECK (attempts >= 0),
    ADD CONSTRAINT inbox_lease_state_check CHECK (
        (
            status = 'inflight'
            AND lease_owner IS NOT NULL
            AND BTRIM(lease_owner) <> ''
            AND lease_token IS NOT NULL
            AND lease_until IS NOT NULL
        )
        OR
        (
            status <> 'inflight'
            AND lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_until IS NULL
        )
    ),
    ADD CONSTRAINT inbox_processed_state_check CHECK (
        (status = 'processed') = (processed_at IS NOT NULL)
    ),
    ADD CONSTRAINT inbox_source_metadata_check CHECK (
        (
            source_topic IS NULL
            AND source_partition IS NULL
            AND source_offset IS NULL
            AND source_timestamp IS NULL
            AND status = 'processed'
        )
        OR
        (
            source_topic IS NOT NULL
            AND BTRIM(source_topic) <> ''
            AND source_partition >= 0
            AND source_offset >= 0
        )
    );

CREATE INDEX inbox_due_idx
ON emitlane.inbox_events (available_at, first_seen_at, consumer, event_id)
WHERE status IN ('pending', 'retry_wait');

CREATE INDEX inbox_expired_lease_idx
ON emitlane.inbox_events (lease_until, consumer, event_id)
WHERE status = 'inflight';

CREATE INDEX inbox_dead_idx
ON emitlane.inbox_events (consumer, updated_at DESC, event_id)
WHERE status = 'dead';

CREATE UNIQUE INDEX inbox_source_unique_idx
ON emitlane.inbox_events (consumer, source_topic, source_partition, source_offset)
WHERE source_topic IS NOT NULL;

CREATE INDEX inbox_processed_retention_idx
ON emitlane.inbox_events (consumer, processed_at, event_id)
WHERE status = 'processed';

-- Released v0.4 binaries omit status and therefore insert the default
-- processed lifecycle. Do not let such a helper silently treat active managed
-- work as an already-processed duplicate.
CREATE FUNCTION emitlane.guard_inbox_legacy_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'processed'
       AND EXISTS (
            SELECT 1
            FROM emitlane.inbox_events AS existing
            WHERE existing.consumer = NEW.consumer
              AND existing.event_id = NEW.event_id
              AND existing.status <> 'processed'
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'legacy Inbox processing conflicts with active managed lifecycle row',
            CONSTRAINT = 'inbox_legacy_active_conflict';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER inbox_legacy_insert_guard
BEFORE INSERT ON emitlane.inbox_events
FOR EACH ROW
EXECUTE FUNCTION emitlane.guard_inbox_legacy_insert();
