# PostgreSQL data model

PostgreSQL is the durable source of truth. Payload is opaque `BYTEA`; routing,
state, tracing, and diagnostics use explicit columns. Migration version 1 owns
only the objects listed below.

## Outbox table

```sql
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
    delivered_at TIMESTAMPTZ
);
```

Migration constraints enforce:

- non-blank destination, event type, and content type;
- an object of string-valued headers;
- positive schema version and non-negative attempts;
- status in `pending`, `inflight`, `delivered`, or `dead`;
- lease owner/expiry present only for `inflight` rows;
- `delivered_at` present exactly for `delivered` rows.

Strict ordering columns are intentionally not part of v0.1.

## Indexes

```sql
CREATE INDEX outbox_pending_idx
ON emitlane.outbox_events (available_at, created_at, id)
WHERE status = 'pending';

CREATE INDEX outbox_inflight_lease_idx
ON emitlane.outbox_events (lease_until)
WHERE status = 'inflight';

CREATE INDEX outbox_dead_idx
ON emitlane.outbox_events (created_at)
WHERE status = 'dead';
```

## Inbox table

```sql
CREATE TABLE emitlane.inbox_events (
    consumer TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, event_id)
);
```

The composite key lets independent consumers process the same event while
serializing duplicate attempts for one consumer.

## State machine

```text
pending ──claim──> inflight ──ACK──> delivered
   ▲                  │
   │                  ├──retryable failure──> pending (future available_at)
   │                  └──permanent/exhausted──> dead
   │
   └──── operator retry of dead resets attempts
```

### Claim

One short transaction selects due `pending` rows and expired `inflight` rows
using `FOR UPDATE SKIP LOCKED`, sets a new owner and lease, and commits before
returning. Claim alone does not increment `attempts`.

### Publish attempt

The current owner conditionally increments `attempts` immediately before
broker I/O. `attempts` therefore means the number of broker calls started.
There is an unavoidable narrow crash window after this increment and before
entering the client; it may over-count but cannot delete or lose an event.

### Result transitions

Delivered, retry, dead, and begin-attempt updates require both
`status='inflight'` and the expected `lease_owner`. A stale worker cannot
overwrite a row reclaimed by another relay.

Retry sets a PostgreSQL-clock `available_at`, stores a bounded valid-UTF-8
`last_error`, and clears the lease. Dead rows retain payload and metadata and
are never deleted automatically. Delivered cleanup uses bounded batches and
the PostgreSQL clock.

## Migrations

Migration SQL is embedded in the binary and tracked in
`emitlane.schema_migrations`. `emitlane migrate up` is idempotent and protected
by a PostgreSQL advisory transaction lock.

The down migration explicitly drops only the tables and helper function created
by version 1. It never uses `DROP SCHEMA ... CASCADE`. If application-owned
objects exist in the `emitlane` schema, they and the schema are preserved.

No normal writer, relay, Inbox, or migration path requires PostgreSQL
superuser privileges.
