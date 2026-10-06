# Accepted design decisions

This is a public summary of decisions reflected in the v0.9.1 code. It contains
current technical rationale, not internal planning notes or new scope proposals.
When changing one of these behaviors, record the replacement decision and
update the affected public contract before implementation.

## Business transaction ownership

Writer accepts `pgx.Tx` and never commits the caller's transaction. This avoids
accidental autocommit through a pool interface and makes business/Outbox
atomicity explicit. Managed consumers own the handler transaction and atomically
commit business effects with Inbox `processed`. External network effects need
idempotency or a downstream Outbox write.

## PostgreSQL authority and bounded claims

PostgreSQL stores events, leases, attempts, cursor progress and control state.
LISTEN/NOTIFY only wakes the Relay. Short SKIP LOCKED claims commit before Kafka
I/O and claim no more work than free publish slots. Unordered claims select due
pending work first, then expired leases; ordered work uses a separate path.
See [Architecture](../02-ARCHITECTURE.md).

## Bounded Kafka attempts and at-least-once recovery

Kafka publishing uses `acks=all`, disables producer idempotence and client
record retries. Database retry owns the durable attempt budget. This bounds
ambiguous client attempts and avoids producer sequence state falsely
acknowledging a different event after an unresolved earlier send. A broker ACK
followed by process death before PostgreSQL acknowledgement can duplicate an
event. This is intentional at-least-once delivery, not exactly once.
See [Delivery guarantees](../DELIVERY_GUARANTEES.md).

## Opt-in ordering

Ordered streams use application-owned sequences and 64 virtual partitions.
Partition leases, epochs, handoff barriers and a bounded publish window fence
old owners; delivered state and stream advance commit together. Gaps and dead
expected events block their stream rather than being silently skipped. Unordered
replay does not rewrite historical cursor progress. See
[Ordered delivery](../ORDERED_DELIVERY.md).

## Stable identities and reserved headers

Event UUID is the consumer deduplication identity. Relay reconstructs its named
reserved metadata headers from durable event fields and removes spoofed case or
whitespace variants. It preserves ordinary custom headers and persisted unordered
replay provenance; it does not reserve the entire `emitlane-*` prefix. See
[Compatibility](../COMPATIBILITY.md) for exact wire names and guarantees.

## Recovery and observability

Finite failure budgets lead to durable `dead` state. Event payloads remain
recoverable until explicit operator action; Inbox recovery additionally needs
Kafka retention because Inbox stores source coordinates rather than payloads.
Operator mutations and audit commit atomically. Read-only integrity reports,
redacted inspection and snapshot freshness metrics expose state without mutating
it. See [Operations](../OPERATIONS.md) and [Backup and restore](../BACKUP_RESTORE.md).

## Schema evolution

Published migrations are immutable. History must be a contiguous known prefix;
standalone Relay requires current schema 4. Migration runners serialize, and
protected down migrations lock tables before their safety checks. Binary
rollback and database downgrade are distinct procedures. See
[Upgrading](../UPGRADING.md).
