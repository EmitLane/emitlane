CREATE SCHEMA IF NOT EXISTS emitlane;

CREATE FUNCTION emitlane.headers_are_strings(value JSONB)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN jsonb_typeof(value) <> 'object' THEN FALSE
        ELSE NOT EXISTS (
            SELECT 1
            FROM jsonb_each(value) AS item
            WHERE jsonb_typeof(item.value) <> 'string'
        )
    END
$$;

CREATE TABLE emitlane.outbox_events (
    id UUID PRIMARY KEY,

    destination TEXT NOT NULL,
    event_type TEXT NOT NULL,

    message_key BYTEA,

    payload BYTEA NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',

    headers JSONB NOT NULL DEFAULT '{}'::jsonb,

    schema_version INTEGER NOT NULL DEFAULT 1,

    correlation_id TEXT,
    causation_id TEXT,

    traceparent TEXT,
    tracestate TEXT,

    status TEXT NOT NULL DEFAULT 'pending',

    attempts INTEGER NOT NULL DEFAULT 0,

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    lease_owner TEXT,
    lease_until TIMESTAMPTZ,

    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,

    CONSTRAINT outbox_destination_check
        CHECK (BTRIM(destination) <> ''),

    CONSTRAINT outbox_event_type_check
        CHECK (BTRIM(event_type) <> ''),

    CONSTRAINT outbox_content_type_check
        CHECK (BTRIM(content_type) <> ''),

    CONSTRAINT outbox_headers_object_check
        CHECK (emitlane.headers_are_strings(headers)),

    CONSTRAINT outbox_schema_version_check
        CHECK (schema_version > 0),

    CONSTRAINT outbox_attempts_check
        CHECK (attempts >= 0),

    CONSTRAINT outbox_status_check
        CHECK (status IN (
            'pending',
            'inflight',
            'delivered',
            'dead'
        )),

    CONSTRAINT outbox_lease_state_check
        CHECK (
            (status = 'inflight' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
            OR
            (status <> 'inflight' AND lease_owner IS NULL AND lease_until IS NULL)
        ),

    CONSTRAINT outbox_delivery_state_check
        CHECK ((status = 'delivered') = (delivered_at IS NOT NULL))
);

CREATE INDEX outbox_pending_idx
ON emitlane.outbox_events (
    available_at,
    created_at,
    id
)
WHERE status = 'pending';

CREATE INDEX outbox_inflight_lease_idx
ON emitlane.outbox_events (lease_until)
WHERE status = 'inflight';

CREATE INDEX outbox_dead_idx
ON emitlane.outbox_events (created_at)
WHERE status = 'dead';

CREATE TABLE emitlane.inbox_events (
    consumer TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        consumer,
        event_id
    ),

    CONSTRAINT inbox_consumer_check
        CHECK (BTRIM(consumer) <> '')
);
