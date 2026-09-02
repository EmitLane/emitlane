# Architecture

## Runtime topology

```text
                         ┌────────────────────────────┐
                         │       Application          │
                         │                            │
                         │ PostgreSQL business write  │
                         │           +                │
                         │ EmitLane Writer SDK        │
                         └─────────────┬──────────────┘
                                       │
                                 SAME TRANSACTION
                                       │
                                       ▼
                         ┌────────────────────────────┐
                         │         PostgreSQL         │
                         │                            │
                         │ business tables            │
                         │ emitlane.outbox_events     │
                         │ emitlane.inbox_events      │
                         └─────────────┬──────────────┘
                                       │
                          LISTEN/NOTIFY │ polling fallback
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────┐
│                        EmitLane                                │
│                                                                │
│ Claim → Commit claim → Publish → Mark delivered                │
│                                                                │
│ Retry │ Backoff │ Lease │ Dead │ Telemetry                     │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
                          Kafka
                            │
                            ▼
                       Consumer App
                            │
                       Inbox helper
                            │
                      business transaction
```

## Components

### Writer SDK

Responsibilities:

- accepts caller-owned PostgreSQL transaction;
- validates minimal event metadata;
- inserts outbox event;
- optionally executes `pg_notify` in the same transaction;
- never publishes directly to Kafka.

Non-responsibilities:

- does not begin/commit the business transaction by default;
- does not serialize only to JSON;
- does not wait for relay delivery.

### Relay

Responsibilities:

- waits for work (`LISTEN/NOTIFY` + polling timeout);
- claims a bounded batch;
- commits claim quickly;
- publishes outside DB transaction;
- transitions success/failure state;
- reclaims expired leases;
- exports metrics/traces/logs.

### Inbox SDK

Responsibilities:

- lets a consumer record `(consumer, event_id)` in the same DB transaction as local business effects;
- prevents repeat DB-side execution for the same consumer/event pair;
- does not pretend to make arbitrary external calls exactly once.

### Admin API — post-v0.1 operability feature

Responsibilities:

- inspect events;
- list dead/stuck work;
- retry/replay;
- relay pause/resume;
- stats;
- audit mutations.

### CLI

Responsibilities:

- migrations;
- diagnostics;
- dead-event listing and retry;
- version/build information;
- `doctor` checks.

## Deployment modes

### Standalone — recommended production mode

```text
Application → PostgreSQL
               ↑
        EmitLane binary → Kafka
```

Advantages:

- lifecycle separated from application;
- independent scaling;
- clearer failure domains;
- easier operability.

### Embedded — convenience mode

```go
cfg := relay.DefaultConfig()
cfg.InstanceID = relay.NewInstanceID()
rly, err := relay.New(cfg, store, publisher)
if err != nil {
    return err
}
go rly.Run(ctx)
```

Use cases:

- local development;
- smaller monoliths;
- simple deployments.

Standalone remains the recommended production mode.

## Core worker algorithm

Pseudo-code:

```text
loop:
    wait until:
        notification received
        OR poll interval expires

    batch = claimDueOrExpired(min(batchSize, workerCapacity))

    if batch empty:
        continue

    for event in batch concurrently up to configured limit:
        result = publisher.Publish(event)

        if success:
            markDelivered(event)
        else:
            scheduleRetryOrDead(event)
```

The database transaction used to claim events must be committed **before broker I/O begins**.

## Claiming strategy

Use PostgreSQL row locking:

```sql
WITH picked AS (
    SELECT id
    FROM emitlane.outbox_events
    WHERE available_at <= NOW()
      AND (
            status = 'pending'
         OR (status = 'inflight' AND lease_until <= NOW())
      )
    ORDER BY available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE emitlane.outbox_events AS e
SET
    status = 'inflight',
    lease_owner = $2,
    lease_until = NOW() + ($3 * INTERVAL '1 millisecond')
FROM picked
WHERE e.id = picked.id
RETURNING e.*;
```

Properties:

- competing relay instances skip locked rows;
- the claim transaction is short;
- a worker crash leaves an expiring lease instead of a permanent lock;
- old in-flight rows can be reclaimed.

## Wake-up strategy

Use PostgreSQL `NOTIFY` only as a wake-up signal.

```text
INSERT event
     │
     ├── pg_notify(...)
     │
COMMIT
     │
     ▼
LISTEN wakes relay
```

Important PostgreSQL semantics:

- notifications inside a transaction are delivered only after successful commit;
- rollback suppresses notification;
- duplicate same-channel/same-payload notifications in the same transaction may be folded;
- therefore notification cannot be the durable queue;
- table polling remains the source-of-truth fallback.

## Proposed packages

Go module: `github.com/emitlane/emitlane`

```text
emitlane/
├── cmd/emitlane/
├── outbox/
│   ├── event.go
│   ├── writer.go
│   └── json.go
├── inbox/
│   └── processor.go
├── relay/
│   ├── relay.go
│   ├── store.go
│   ├── retry.go
│   └── lease.go
├── broker/
│   ├── publisher.go
│   └── kafka/
├── storage/
│   └── postgres/
├── telemetry/
│   ├── metrics.go
│   └── tracing.go
├── migrations/
├── internal/
├── examples/ecommerce/
└── docker-compose.example.yml
```

## Dependency direction

Suggested layering:

```text
outbox        → small DB abstractions only
inbox         → small DB abstractions only
relay         → storage port + publisher port + telemetry ports
broker/kafka  → implements publisher
storage/pg    → implements storage
cmd           → composition root
```

Do not let core packages depend on Kafka-specific types.

## Proposed technology choices

- Go;
- `pgx/v5`;
- Kafka: `franz-go`;
- logging: standard `log/slog`;
- metrics: Prometheus client;
- tracing: OpenTelemetry;
- integration environments: Testcontainers;
- Docker for demo/runtime packaging.

## v0.2 operational plane

The operational plane is deliberately PostgreSQL-backed and separate from the
delivery port. `runtime_control` is the durable cluster-wide pause state;
`LISTEN emitlane_control` only accelerates propagation and a two-second control
poll remains the fallback. The PostgreSQL `Claim` statement also checks that
pause is false, closing the read/claim race across processes.

Relay heartbeat rows are visibility data, not ownership or correctness data.
Failure to register or heartbeat is observable but never stops delivery. Active,
stale and stopped are derived from timestamps and the configured stale threshold.

The Admin API and CLI call the same service. Mutations and their audit record
commit in one PostgreSQL transaction. Replay clones raw stored bytes into a new
UUIDv7 event and never performs broker I/O in that transaction. The ordinary
relay later publishes the clone under the existing at-least-once protocol.
