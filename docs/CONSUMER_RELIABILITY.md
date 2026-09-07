# Consumer reliability

EmitLane v0.5 adds a managed Kafka consumer that combines durable Inbox state,
partition-ordered processing, PostgreSQL transaction fencing, and Kafka offset
management. The public guarantee is **duplicate-safe transactional processing**,
not exactly-once consumption.

## Guarantee boundary

For each configured consumer and event ID, EmitLane runs the handler with a
caller-visible `pgx.Tx`. Business writes and the Inbox `processed` transition
commit in that same PostgreSQL transaction. A redelivery after that commit sees
the processed marker and does not run the handler again.

The guarantee covers only PostgreSQL effects written through the handler
transaction. HTTP calls, email, files, and other external effects are not made
transactional. To produce downstream work, enqueue an EmitLane Outbox event in
the handler transaction.

Kafka delivery remains at least once. EmitLane does not claim exactly-once Kafka
consumption, exactly-once external effects, global ordering, or cross-partition
ordering.

## Identity and source coordinates

The default resolver reads `emitlane-event-id` from Kafka headers and requires
exactly one distinct, valid UUID value. Repeated equal values are accepted;
missing, malformed, or conflicting values are protocol errors and block the
partition without committing its offset. Applications consuming records from
non-EmitLane producers may install an explicit identity resolver. Payloads and
keys are never silently hashed into identities.

The durable identity remains `(consumer, event_id)`. The first observed Kafka
coordinates are stored for diagnostics, and `(consumer, source_topic,
source_partition, source_offset)` is unique when present. The same event ID at a
later offset is a duplicate for that consumer and is safe to commit after the
processed marker is observed. The same source coordinates with a different
event ID are a protocol conflict.

The configured consumer name is a stable application/version namespace, not a
pod or process identifier. Keep it consistent across all members of one logical
consumer deployment; use `InstanceID` only for ephemeral lease ownership.

Inbox does not persist the Kafka key, headers, or payload. A dead record remains
in Kafka and blocks its source partition, so Kafka retention must exceed the
operator response and retry window.

## Schema v4 lifecycle

Migration `000004_inbox_lifecycle` evolves `emitlane.inbox_events` without
changing migrations 1–3 or the primary key. Existing rows become valid
`processed` rows and keep their original `processed_at` value.

```text
pending ──claim──> inflight ──transaction commit──> processed
                      │
                      ├── retryable failure ──> retry_wait ──due claim──> inflight
                      └── permanent/exhausted failure ──> dead

dead ──explicit audited retry──> retry_wait ──claim──> inflight
```

Managed rows contain lifecycle status, attempts, availability, lease owner,
unique lease token, lease expiry, a bounded last error, first/updated timestamps,
and Kafka source metadata. `attempts` increments only when a handler attempt is
successfully claimed. Retry preserves the event identity and attempt count.

Schema constraints require:

- one of `pending`, `inflight`, `retry_wait`, `processed`, or `dead`;
- a non-negative attempt count;
- all lease fields only on `inflight` rows;
- `processed_at` only on `processed` rows;
- all Kafka source fields together, with a non-negative partition and offset;
- managed active rows to have source metadata.

Indexes cover due retries, expired leases, dead listing, source lookup, and
processed retention. There is no automatic destructive Inbox retention.

The v4 down migration refuses to run while any row is not a legacy-compatible
processed marker. This prevents rollback from erasing active managed state.

## Processing and offset protocol

For each partition, records are processed serially:

```text
fetch record
→ resolve UUID identity
→ create/load Inbox row
→ claim with owner, new token, lease and incremented attempt
→ begin PostgreSQL transaction
→ run handler(ctx, tx, message)
→ mark processed in that transaction WHERE lease_token = active token
→ commit PostgreSQL transaction
→ commit Kafka offset
```

Offsets advance only after durable `processed` state is known, including the
already-processed duplicate path. They never advance for pending, inflight,
retrying, dead, or protocol-invalid records. Therefore an unresolved lower
offset blocks later offsets in the same partition. Other partitions continue
up to the configured concurrency bound.

If PostgreSQL commit succeeds but the Kafka offset commit fails or the process
dies first, Kafka redelivers. The Inbox row is already processed, so the handler
and any Outbox write protected by its transaction do not run twice.

## Leases and fencing

Every claim uses a new random lease token. Lease renewal, processed, retry, and
dead transitions all require the active token. Owner names alone are not
sufficient fencing.

The runtime renews the lease while a handler is active. A proven renewal loss
cancels the handler context. Cancellation is advisory: the final conditional
processed update is the authoritative fence. If an expired attempt wakes after
another process has reclaimed the row, its update affects no row and its entire
business transaction rolls back.

Handler timeout must be positive. It may exceed one lease interval because the
runtime renews the lease while the handler is active. The renewal interval must
be less than half the lease duration, and the maintenance poll must be shorter
than the lease duration. Configuration is validated before Kafka or handler
work begins. Handlers must honor context cancellation; the token-conditional
`processed` update remains the fence even when user code returns late.

## Retry and dead behavior

Retry delay is bounded exponential backoff with jitter. A retryable handler
failure rolls back the business transaction, then token-conditionally records
`retry_wait`, a PostgreSQL-clock `available_at`, and a bounded safe error.
`inbox.Permanent(err)` explicitly classifies a non-retryable error; strings are
never used for classification. Exhausted and permanent attempts become `dead`.

A dead record blocks its Kafka partition. EmitLane does not commit past it,
skip it, or run later offsets in that partition. An operator must issue an
authenticated/audited retry with a non-empty reason. Retry keeps the same event
ID, source coordinates, and attempt history. Health endpoints do not fail merely
because domain work is retrying or dead; CLI, Admin API, logs, metrics, and
integrity diagnostics expose the condition.

Operators can inspect and recover without reading payloads:

```text
emitlane inbox stats [--consumer billing-v1] [--json]
emitlane inbox dead [--consumer billing-v1] [--limit 50] [--offset 0] [--json]
emitlane inbox inspect --consumer billing-v1 --event-id <uuid> [--json]
emitlane inbox retry --consumer billing-v1 --event-id <uuid> --reason "handler fixed"
```

The authenticated Admin API exposes the equivalent `/v1/inbox/*` reads and
audited retry mutation.

## Rebalance and crash recovery

On partition revoke, the runtime stops accepting new records for that partition
and cancels/drains active work within a configured bound. It never commits an
unfinished offset. An uncommitted record is assigned again. If the old database
transaction committed, the replacement observes `processed`; otherwise it
waits for release or lease expiry and reclaims safely.

Crash outcomes are intentionally recoverable:

- before claim: Kafka redelivery;
- after claim: lease expiry and reclaim;
- during handler transaction: PostgreSQL rollback and retry;
- after database commit but before offset commit: duplicate suppression and a
  later successful offset commit.

## Legacy compatibility and upgrade

`inbox.Process` and `inbox.ProcessStrict` retain their v0.4 signatures and
duplicate behavior on schema v4. They insert lifecycle-complete `processed`
rows. When a legacy helper encounters an active managed row, it returns a typed
conflict instead of treating unfinished work as a processed duplicate.

Upgrade in this order:

1. migrate schema v3 to v4;
2. continue running v0.4 binaries that use legacy Inbox helpers if needed;
3. deploy v0.5 binaries;
4. enable managed consumers.

Do not run the legacy helper from a managed handler for the same consumer and
event identity.

## Recommended composition

```text
application transaction
→ source Outbox row
→ Relay publishes to Kafka
→ managed consumer claims Inbox row
→ handler business writes
→ optional downstream Outbox row in the same handler transaction
→ Inbox processed in the same transaction
→ PostgreSQL commit
→ Kafka offset commit
```

Pruning processed Inbox markers is an explicit operator choice. Replaying an
event after its marker was pruned can run the handler again; retention must be
selected together with the Kafka replay horizon.

## Telemetry dimensions

Managed consumer metrics cover records by bounded result, processing duration,
retry and duplicate counts, active handlers, dead/paused partitions, assignment
callbacks, and lag. `consumer` and `topic` labels are configuration-controlled
dimensions and must remain bounded. Event IDs, offsets, partitions, lease
tokens, raw errors, and payload-derived values are never metric labels.

Consumer spans extract W3C `traceparent` and `tracestate`, then propagate the
resulting context through the handler transaction and any Outbox enqueue.
Malformed trace context never changes processing correctness. Payloads,
credentials, and lease tokens are not logged.
