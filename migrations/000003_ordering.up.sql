ALTER TABLE emitlane.outbox_events
    ADD COLUMN ordering_key TEXT,
    ADD COLUMN ordering_sequence BIGINT,
    ADD COLUMN ordering_partition SMALLINT,
    ADD CONSTRAINT outbox_ordering_state_check CHECK (
        (
            ordering_key IS NULL
            AND ordering_sequence IS NULL
            AND ordering_partition IS NULL
        )
        OR
        (
            ordering_key IS NOT NULL
            AND BTRIM(ordering_key) <> ''
            AND ordering_sequence > 0
            AND ordering_partition BETWEEN 0 AND 63
        )
    );

CREATE UNIQUE INDEX outbox_ordering_sequence_unique_idx
ON emitlane.outbox_events (
    destination,
    ordering_key,
    ordering_sequence
)
WHERE ordering_key IS NOT NULL;

CREATE INDEX outbox_ordered_claim_idx
ON emitlane.outbox_events (
    ordering_partition,
    ordering_sequence,
    available_at,
    created_at,
    id
)
WHERE ordering_key IS NOT NULL AND status IN ('pending', 'inflight');

CREATE TABLE emitlane.ordering_streams (
    destination TEXT NOT NULL,
    ordering_key TEXT NOT NULL,
    partition_id SMALLINT NOT NULL,
    start_sequence BIGINT NOT NULL,
    next_sequence BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (destination, ordering_key),

    CONSTRAINT ordering_stream_destination_check CHECK (BTRIM(destination) <> ''),
    CONSTRAINT ordering_stream_key_check CHECK (BTRIM(ordering_key) <> ''),
    CONSTRAINT ordering_stream_partition_check CHECK (partition_id BETWEEN 0 AND 63),
    CONSTRAINT ordering_stream_start_check CHECK (start_sequence > 0),
    CONSTRAINT ordering_stream_next_check CHECK (next_sequence >= start_sequence)
);

CREATE INDEX ordering_stream_partition_idx
ON emitlane.ordering_streams (partition_id, destination, ordering_key);

CREATE TABLE emitlane.ordering_partitions (
    partition_id SMALLINT PRIMARY KEY,
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    epoch BIGINT NOT NULL DEFAULT 0,
    handoff_not_before TIMESTAMPTZ,
    publish_timeout_ms INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ordering_partition_id_check CHECK (partition_id BETWEEN 0 AND 63),
    CONSTRAINT ordering_partition_epoch_check CHECK (epoch >= 0),
    CONSTRAINT ordering_partition_publish_timeout_check CHECK (
        publish_timeout_ms IS NULL OR publish_timeout_ms > 0
    ),
    CONSTRAINT ordering_partition_lease_check CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR
        (lease_owner IS NOT NULL AND BTRIM(lease_owner) <> '' AND lease_until IS NOT NULL)
    )
);

INSERT INTO emitlane.ordering_partitions (partition_id)
SELECT value::SMALLINT
FROM generate_series(0, 63) AS value;

ALTER TABLE emitlane.relay_instances
    ADD COLUMN ordering_capable BOOLEAN NOT NULL DEFAULT FALSE;

-- Released v0.2 Relays do not filter ordering columns. Silently skip their
-- attempt to claim an ordered row unless the owner is both v0.3-capable and the
-- current valid virtual-partition owner. PostgreSQL remains the authority.
CREATE FUNCTION emitlane.guard_ordered_event_claim()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.ordering_key IS NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.status = 'inflight'
       AND (OLD.status <> 'inflight' OR NEW.lease_owner IS DISTINCT FROM OLD.lease_owner)
       AND NOT EXISTS (
            SELECT 1
            FROM emitlane.ordering_partitions AS partition
            JOIN emitlane.relay_instances AS relay
              ON relay.instance_id = NEW.lease_owner
             AND relay.ordering_capable = TRUE
             AND relay.stopped_at IS NULL
            WHERE partition.partition_id = OLD.ordering_partition
              AND partition.lease_owner = NEW.lease_owner
              AND partition.lease_until > NOW()
              AND COALESCE(partition.handoff_not_before, '-infinity'::timestamptz) <= NOW()
       ) THEN
        RETURN NULL;
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER outbox_ordered_claim_guard
BEFORE UPDATE ON emitlane.outbox_events
FOR EACH ROW
EXECUTE FUNCTION emitlane.guard_ordered_event_claim();
