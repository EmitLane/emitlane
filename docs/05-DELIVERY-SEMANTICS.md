# Delivery semantics and failure modes

## Public guarantee statement

EmitLane should publish a short guarantee statement prominently:

> EmitLane atomically persists business changes and outbox events when both writes happen in the same PostgreSQL transaction. The relay publishes with at-least-once semantics. Duplicates are possible. Consumers must be idempotent.

## Why exactly-once is not claimed

Consider:

```text
1. Relay publishes event to Kafka
2. Kafka ACKs
3. Relay process crashes
4. PostgreSQL never receives `status = delivered`
5. Lease expires
6. Another worker republishes
```

Kafka may receive the same logical event twice.

Trying to hide this fact makes the product less trustworthy.

## Lease-based publish lifecycle

### Claim phase

```text
BEGIN
  choose pending rows using SKIP LOCKED
  mark inflight
  set owner + lease expiry
COMMIT
```

### Publish phase

```text
network call to Kafka
```

### Acknowledge phase

```text
success → delivered
failure → pending at future available_at OR dead
```

No database transaction is intentionally held open during Kafka network I/O.

## Critical crash scenarios

### A. Crash before claim commit

Expected result:

- transaction rolls back;
- event remains pending;
- another relay can claim it.

### B. Crash after claim commit but before publish

Expected result:

- event remains inflight;
- lease expires;
- another worker recovers it;
- event is eventually retried.

### C. Crash while broker is processing

Outcome may be unknown to the relay.

Expected model:

- safe recovery favors possible duplicate over silent loss;
- lease expiry permits retry.

### D. Broker ACK then crash before delivered update

Expected result:

- duplicate is possible;
- event must not be silently discarded;
- Inbox/idempotent consumer handles repeat processing.

### E. Kafka unavailable

Expected result:

- attempts fail;
- exponential backoff with jitter;
- events remain durable;
- no busy retry loop;
- oldest-pending metric rises;
- after policy exhaustion poison/permanent failures become dead.

### F. PostgreSQL unavailable

Expected result:

- relay cannot safely mutate state;
- process retries DB connection with bounded backoff;
- broker publish should not proceed for events that cannot be safely claimed.

### G. Worker dies while owning many leases

Expected result:

- leases expire;
- surviving workers reclaim;
- no permanent stuck state.

## Retry policy

Implementation uses configurable exponential backoff plus full jitter:

```text
cap = min(maxDelay, baseDelay * 2^(attempt-1))
delay = random(0, cap)
```

`attempt` is one-based and counts broker calls already started. With the
defaults, the first failure is scheduled uniformly between zero and one second;
later caps are 2s, 4s, 8s, and so on up to 30 minutes. The tenth failed attempt
moves the event to `dead` rather than scheduling another retry.

Reason: after broker recovery many relay instances must not retry simultaneously and cause a thundering herd.

## Error classification

v0.1 may start with one retry policy, but internal design should leave room for:

- retryable broker/network errors;
- permanent configuration errors;
- invalid destination;
- malformed local metadata.

Do not create a huge error taxonomy before real integration behavior is understood.

## Dead-letter behavior

After configured exhaustion:

```text
status = dead
```

Keep:

- payload;
- metadata;
- trace context;
- attempt count;
- last error;
- original ID.

Operator actions:

```text
emitlane dead list
emitlane dead inspect <id>
emitlane dead retry <id>
emitlane dead retry --all
```

## Inbox semantics

For a database consumer:

```text
BEGIN
  create `(consumer,event_id)` inbox marker
  mutate local business data
COMMIT
```

If the same event arrives again:

```text
marker conflict → skip local DB mutation
```

This gives effective deduplication for effects enclosed by that same transaction.

## Failure principle

When faced with a choice between:

```text
possible duplicate
```

and

```text
possible silent event loss
```

EmitLane chooses the duplicate and makes deduplication explicit.

## Pause, retry, and replay

Pause prevents new claims across v0.2+ PostgreSQL relays. Work already claimed may
finish, including the delivered acknowledgement after Kafka ACK. Paused relays
remain healthy; pause is operator state, not a dependency failure.

Retry is the same logical event and keeps its ID. It is allowed only from `dead`,
resets attempts, and returns the row to `pending`.

Replay is intentionally different: it creates a new UUIDv7 event identity with a
new delivery lifecycle. The source remains unchanged. A replay can therefore run
downstream business logic again, and Inbox correctly treats it as a new event.
Replay still has at-least-once semantics and can itself be duplicated in the
Kafka-ACK/database-ACK crash window.

For an ordered source, default replay returns a conflict because reusing its
historical domain sequence would violate stream progress. Explicit unordered
replay creates a new ID with no active ordering fields, preserves original
ordering metadata as provenance headers, and leaves the stream cursor unchanged.

## Ordered streams

v0.3 ordering is opt-in per `(destination, ordering_key)`. The application
assigns a positive monotonic sequence. Only the event equal to the stream's
durable `next_sequence` can be claimed. Missing, retrying, and dead expected
events block later sequence numbers but do not block independent streams.

Kafka ACK and stream advancement are not atomic. If the process dies between
them, the current sequence may be published again. The atomic PostgreSQL
delivered/advance transition keeps N+1 blocked until N is durable. Partition
leases, epochs, a final pre-send fence, a bounded publish timeout, and delayed
handoff prevent a stale owner from sending N after a replacement has progressed
to N+1 under the documented client timing assumptions.

This remains at-least-once delivery. It is not global ordering, cross-topic
ordering, sequence generation, or end-to-end exactly once. See
[Ordered delivery](ORDERED_DELIVERY.md).
