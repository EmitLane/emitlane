# Delivery guarantees

EmitLane atomically persists business changes and outbox events when both writes
happen in the same PostgreSQL transaction. The relay publishes with
**at-least-once** semantics. Duplicates are possible. Consumers must be
idempotent.

EmitLane does **not** provide end-to-end exactly-once delivery.

## Producer

If the application writes business rows and calls `outbox.Writer.Enqueue` in the
same `pgx.Tx`, commit stores both or neither. Rollback leaves no outbox event.

The writer never publishes to Kafka. `pg_notify` on channel `emitlane_events` is
only a wake-up signal, emitted in the same transaction. PostgreSQL is the source
of truth.

## Relay

Broker delivery is at least once.

The claim transaction marks rows `inflight` and assigns `lease_owner` /
`lease_until`. That transaction is committed **before** any Kafka I/O. A process
crash after claim and before publish leaves an expiring lease; another worker
recovers the event without consuming a publish attempt.

Kafka acknowledgement is required before the row is marked `delivered`. If the
process dies after broker ACK and before the database update, the event remains
recoverable and **may be published again**. That duplicate window is intentional.

The Kafka producer uses `acks=all`, disables Kafka producer idempotence, and
performs no client record retries. EmitLane deliberately chooses a bounded,
ambiguous at-least-once attempt: cancellation may race with Kafka accepting the
request, and PostgreSQL retry may duplicate it. Disabling producer sequence
state prevents a later distinct event from being falsely acknowledged against
an unresolved earlier sequence.

## Attempts

`attempts` is the number of broker publish attempts that have been started. The
owning relay increments it conditionally immediately before the Kafka call; a
claim alone does not consume an attempt. The Kafka header is one-based. After
`max_attempts` failed publishes, or a classified permanent broker error, the row
becomes `dead`. Dead events are never deleted automatically.

There is an unavoidable narrow crash window after recording an attempt and
before entering the Kafka client. This can over-count an attempt, but it cannot
silently lose or delete the event.

## Consumer / Inbox

`inbox.Process` inserts `(consumer, event_id)` in the caller-owned transaction
and runs the callback only for a new pair. The same event may be processed by
different consumers. Duplicate protection is the database primary key.

Inbox prevents duplicate **local database** side effects only when those effects
commit with the inbox row. External side effects (HTTP, card charges, email)
still need their own idempotency keys. Pass the stable event ID where the
downstream API supports it.

The v0.5 managed consumer owns Kafka polling and offsets while preserving the
same durable identity. It claims an Inbox lease, runs the handler, and marks
`processed` in the same `pgx.Tx` as protected business changes. Only after that
transaction commits does it commit the Kafka offset. A failure or crash between
those commits causes redelivery, not repeated protected writes.

Retryable failures roll back the handler transaction and durably wait in
`retry_wait`. Permanent or exhausted failures become `dead` and block later
offsets in that source partition until an audited retry. Other partitions may
continue. Every attempt uses a new lease token, so a stale handler cannot commit
its Inbox transition or business transaction.

This is duplicate-safe transactional processing for PostgreSQL effects, not
exactly-once Kafka consumption or exactly-once external side effects. Inbox does
not store the Kafka payload; dead recovery depends on Kafka retention.

## Ordering

Unordered events do not guarantee global or per-aggregate order across
independent transactions. Kafka partitioning follows the record key.

v0.3 ordered events opt into a per-`(destination, ordering_key)` contract using
an application-owned positive sequence. EmitLane will not begin N+1 until N has
durably reached delivered. Duplicate N is possible after Kafka ACK and before
the PostgreSQL transition, but N+1 remains blocked; consumers still deduplicate.
The guarantee depends on the bounded publish/handoff assumptions documented in
[Ordered delivery](ORDERED_DELIVERY.md). It is not global, cross-topic, or
exactly-once ordering.

## Retention

Delivered events may be deleted after `EMITLANE_RETENTION_DELIVERED` (default
7 days). Dead events are never deleted by this cleanup. Ordered stream cursors
survive delivered-row cleanup, so historical sequence numbers remain rejected.

Managed Inbox processed markers have no automatic retention. If an operator
later removes one, replaying that event can run the handler again. Choose any
explicit Inbox retention policy together with the Kafka replay horizon.
