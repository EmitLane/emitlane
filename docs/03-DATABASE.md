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

## Schema version 2

Migration `000002_operability` is additive to the released v1 schema. It adds
nullable `replayed_from_event_id` and `replay_batch_id` columns without foreign
keys, so retention of an original row cannot make a replay row undeletable.

It also adds:

- `runtime_control`: one durable pause/resume row;
- `relay_instances`: best-effort presence and clean-stop timestamps;
- `admin_audit_log`: payload-free audit records for operator mutations;
- indexes for `(created_at,id)` keyset pagination, status and destination/type
  filters, replay batch lookup and audit pagination.

Retry updates the existing dead row. Replay inserts a new pending row with a new
UUIDv7 and provenance. Pause/resume, retry, and replay write their audit record
in the same transaction as the state mutation. The v1 migration files are
immutable.

## Schema version 3

Migration `000003_ordered_delivery` is additive to the released v2 schema. It
adds nullable `ordering_key`, `ordering_sequence`, and `ordering_partition`
columns to outbox rows. A check constraint requires all three or none, and a
partial unique index protects `(destination, ordering_key, ordering_sequence)`.
Unordered rows keep all three null and use the existing claim indexes and SQL.

`emitlane.ordering_streams` stores one durable cursor per
`(destination, ordering_key)`:

```text
partition_id, start_sequence, next_sequence, updated_at
```

The writer creates or validates this row inside the caller's transaction. The
cursor advances atomically with the expected event's delivered transition and
is not removed by delivered-event retention.

`emitlane.ordering_partitions` contains exactly 64 seeded rows. Each row stores
the authoritative lease owner/expiry, monotonically increasing epoch, handoff
barrier, and prior owner's publish timeout. Ownership transactions are short;
Kafka I/O never occurs while one is open. `relay_instances.ordering_capable`
keeps released v0.2 processes out of ordered membership.

The v3 down migration refuses while any ordered stream or ordered outbox row
exists. It will not silently erase sequence progress. See
[Ordered delivery](ORDERED_DELIVERY.md) for the claim and fencing protocol.
