ALTER TABLE emitlane.outbox_events
    ADD COLUMN replayed_from_event_id UUID,
    ADD COLUMN replay_batch_id UUID;

-- Supports stable keyset pagination and time-bounded operational queries.
CREATE INDEX outbox_created_idx
ON emitlane.outbox_events (created_at DESC, id DESC);

-- Supports status-filtered inspection and replay without scanning unrelated rows.
CREATE INDEX outbox_status_created_idx
ON emitlane.outbox_events (status, created_at DESC, id DESC);

-- Supports destination/type replay selectors and their deterministic ordering.
CREATE INDEX outbox_destination_type_created_idx
ON emitlane.outbox_events (destination, event_type, created_at DESC, id DESC);

-- Supports inspection of an executed replay batch. Original events remain independent.
CREATE INDEX outbox_replay_batch_idx
ON emitlane.outbox_events (replay_batch_id, created_at DESC, id DESC)
WHERE replay_batch_id IS NOT NULL;

CREATE TABLE emitlane.runtime_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT NOT NULL DEFAULT 'migration',

    CONSTRAINT runtime_control_singleton_check CHECK (singleton),
    CONSTRAINT runtime_control_actor_check CHECK (BTRIM(updated_by) <> '')
);

INSERT INTO emitlane.runtime_control (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE emitlane.relay_instances (
    instance_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    version TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ,

    CONSTRAINT relay_instance_id_check CHECK (BTRIM(instance_id) <> ''),
    CONSTRAINT relay_hostname_check CHECK (BTRIM(hostname) <> ''),
    CONSTRAINT relay_version_check CHECK (BTRIM(version) <> '')
);

CREATE TABLE emitlane.admin_audit_log (
    id UUID PRIMARY KEY,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT,
    request_id TEXT,
    target_event_id UUID,
    replay_batch_id UUID,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT admin_audit_action_check CHECK (BTRIM(action) <> ''),
    CONSTRAINT admin_audit_actor_check CHECK (BTRIM(actor) <> ''),
    CONSTRAINT admin_audit_details_object_check CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX admin_audit_created_idx
ON emitlane.admin_audit_log (created_at DESC, id DESC);

