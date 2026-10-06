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
                 EmitLane managed consumer
              poll → claim → handler tx → offset
                            │
                            ▼
               business data + Inbox processed
                    SAME POSTGRESQL TRANSACTION
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

The v0.4 `inbox.Process` helpers remain available when applications own Kafka
polling and offset commits.

### Managed consumer

Responsibilities:

- owns franz-go group polling, assignment, revoke, pause/resume, and manual
  offset commits;
- resolves a stable event UUID and claims `(consumer, event_id)` with a unique
  lease token;
- supplies the business handler a caller-visible `pgx.Tx`;
- marks Inbox `processed` in that same transaction;
- commits the Kafka offset only after durable processing is known;
- durably schedules retry or dead state without committing unresolved offsets;
- keeps each Kafka partition serial while allowing configured cross-partition
  concurrency.

The managed runtime does not make HTTP, email, filesystem, or other external
effects transactional. A downstream EmitLane Outbox row should be written in
the handler transaction instead.

### Admin API

Responsibilities:

- inspect events;
- list dead/stuck work;
- retry/replay;
- relay pause/resume;
- stats;
- audit mutations.

### CLI

Responsibilities:

- migrations and dependency/schema diagnostics;
- redacted event, relay, ordering, and Inbox inspection;
- audited pause/resume, dead retry, and replay;
- read-only integrity checks;
- version/build information.

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

The unordered claim path uses two bounded `FOR UPDATE SKIP LOCKED` scans:
first due `pending` rows, then expired `inflight` rows for remaining capacity.
Both exclude ordered events and require the durable runtime-control row to be
unpaused. This avoids the former combined pending-or-expired `OR` query.

The selected rows receive `status=inflight`, the instance owner and a
PostgreSQL-clock lease expiry in the same short transaction. Claim commits
before the rows are dispatched. Every claim is capped by free worker slots;
there is no prefetched queue of leased events.

The separate ordered claim joins stream cursors and partition authority. It
requires the expected sequence, matching owner/epoch, a valid partition lease,
a passed handoff barrier and unpaused control state. Scheduler refill alternates
ordered and unordered priority when both populations have work.

The authoritative SQL lives in [unordered storage](../storage/postgres/store.go)
and [ordered storage](../storage/postgres/ordered_delivery.go). See
[Ordered delivery](ORDERED_DELIVERY.md) for fencing and timing assumptions.

Outbox event leases are not renewed during publish. Publish timeout must remain
below the event lease. Ordered partition leases and managed Inbox leases have
separate renewal protocols. Expired event leases remain automatically recoverable.

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

## Package layout

Go module: `github.com/emitlane/emitlane`

```text
emitlane/
├── cmd/emitlane/
├── outbox/
│   ├── event.go
│   ├── writer.go
│   └── json.go
├── inbox/
│   ├── processor.go
│   └── lifecycle.go
├── consumer/
│   ├── message.go
│   ├── runtime.go
│   └── source.go
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

Package layering:

```text
outbox        → small DB abstractions only
inbox         → small DB abstractions only
relay         → storage port + publisher port + telemetry ports
broker/kafka  → implements publisher and managed consumer source
storage/postgres → implements storage
cmd           → composition root
```

Do not let core packages depend on Kafka-specific types.

## Technology choices

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

## v0.3 ordered data plane

Ordered delivery is an additive path beside the unchanged unordered claim path.
The writer initializes durable stream metadata inside the application's
transaction. Relay presence feeds deterministic desired ownership for 64
virtual partitions, while PostgreSQL partition leases and epochs remain the
authority.

```text
application transaction
  ├─ business version N
  ├─ ordering_streams initialization/validation
  └─ outbox event (ordering key, N, virtual partition)

Relay reconcile ──> partition lease + epoch + handoff barrier
Relay claim     ──> only event matching stream.next_sequence
Relay publish   ──> Kafka key affinity, outside SQL transaction
Relay ACK       ──> atomic event delivered + stream advance
```

The final pre-send fence requires enough partition lease for the configured
publish timeout and safety margin. Ownership handoff waits at least the prior
publish window plus that margin, preventing stale network sends from appearing
after a replacement owner has advanced the stream under the documented Kafka
client bound. See [Ordered delivery](ORDERED_DELIVERY.md).
