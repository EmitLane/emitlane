DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM emitlane.ordering_streams)
       OR EXISTS (
            SELECT 1
            FROM emitlane.outbox_events
            WHERE ordering_key IS NOT NULL
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'cannot downgrade EmitLane schema v3 while ordered stream state exists',
            HINT = 'retain schema v3 or deliberately retire ordered events and stream metadata before retrying';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS outbox_ordered_claim_guard ON emitlane.outbox_events;
DROP FUNCTION IF EXISTS emitlane.guard_ordered_event_claim();

DROP INDEX IF EXISTS emitlane.ordering_stream_partition_idx;
DROP TABLE IF EXISTS emitlane.ordering_partitions;
DROP TABLE IF EXISTS emitlane.ordering_streams;

DROP INDEX IF EXISTS emitlane.outbox_ordered_claim_idx;
DROP INDEX IF EXISTS emitlane.outbox_ordering_sequence_unique_idx;

ALTER TABLE emitlane.outbox_events
    DROP CONSTRAINT IF EXISTS outbox_ordering_state_check,
    DROP COLUMN IF EXISTS ordering_partition,
    DROP COLUMN IF EXISTS ordering_sequence,
    DROP COLUMN IF EXISTS ordering_key;

ALTER TABLE emitlane.relay_instances
    DROP COLUMN IF EXISTS ordering_capable;

DELETE FROM emitlane.schema_migrations WHERE version = 3;
