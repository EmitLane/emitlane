# Ordered delivery

EmitLane v0.3 adds opt-in ordering for one domain stream while preserving the
existing unordered, at-least-once path. PostgreSQL remains the durable source
of truth and Kafka acknowledgement is never presented as end-to-end
exactly-once delivery.

## Guarantee

An ordered stream is the pair `(destination, ordering_key)`. If the application
assigns unique, monotonically increasing sequence numbers, and every missing or
dead sequence is eventually supplied or retried successfully, EmitLane does not
intentionally begin delivery of sequence `N+1` until sequence `N` has made its
durable `delivered` transition.

Duplicates of the current sequence remain possible around the Kafka-ACK / SQL
commit crash window. Thus `1, 2, 2, 3` is valid, while `1, 3, 2` and
`1, 2, 1` are not. Consumers still need Inbox or equivalent idempotency.

The guarantee is per stream and per destination. It is not global ordering,
cross-topic ordering, exactly-once delivery, or automatic domain sequencing.

## Producer contract and stream start

The application supplies `OrderingKey` and `Sequence` on `outbox.Event`.
Unordered events leave all ordering fields at zero values. Ordered events use a
non-blank key and a positive sequence. `OrderingStartSequence` defaults to one
when a stream is first created; an explicit positive start may adopt an existing
aggregate above one and cannot exceed the event sequence.

The writer initializes `emitlane.ordering_streams` in the same caller-owned
transaction as the outbox insert and business mutation. Initialization uses a
conflict-safe insert followed by a locked read. Existing metadata must have the
same virtual partition, an explicit start must match, and a sequence below the
durable `next_sequence` is rejected. A partial unique index prevents two events
from occupying the same `(destination, ordering_key, ordering_sequence)`.

EmitLane never derives domain order from UUIDs, timestamps, insert order,
transaction start time, or database identities.

For Kafka partition affinity, an ordered event with no message key uses the
UTF-8 bytes of `OrderingKey`. An explicit key is accepted only when it is
exactly those bytes. A conflict is rejected rather than silently rewritten.

## Commit inversion, gaps, retries, and dead events

`ordering_streams.next_sequence` is the only scheduling cursor. If sequence 12
commits while a stream expects 11, sequence 12 is durable but ineligible. Once
11 commits and reaches its durable delivered transition, 12 becomes eligible.

If a future sequence exists while the expected sequence does not, the stream is
reported as `gap`; EmitLane does not infer that the missing event is safe to
skip. A failed expected event is `retry_wait` until its `available_at`. A dead
expected event is `dead_blocked`. In every case, later sequences remain blocked.
The existing dead-event retry changes the same event back to pending; successful
delivery then advances the stream normally. v0.3 has no force-advance,
auto-timeout, or skip operation.

Independent streams are independent. Claiming uses row locks on eligible events,
so one blocked stream does not block another and two sequences from one stream
cannot be claimed concurrently.

## Virtual partition mapping

v0.3 has exactly 64 virtual partitions. This is a protocol constant, not
configuration. The partition input is:

```text
destination + NUL byte + ordering_key
```

The bytes are hashed with standard 64-bit FNV-1a and reduced modulo 64. The NUL
separator prevents ambiguous concatenations. Deterministic vectors are kept in
tests. Changing the count or mapping requires an explicit repartitioning design.

## Desired and actual ownership

Relay presence remains the membership source. Among active v0.3-capable Relay
instances, rendezvous hashing chooses a deterministic desired owner for each
partition. The score hashes a versioned partition identity and instance ID with
FNV-1a 64-bit; the highest score wins, with instance ID as the deterministic tie
breaker.

Desired ownership is advisory. `emitlane.ordering_partitions` is authoritative.
It contains 64 seeded rows with an owner, expiry, monotonically increasing epoch,
handoff barrier, and the owner's maximum publish window. A Relay periodically:

- renews desired partitions it still owns;
- stops claiming from partitions it no longer desires;
- releases no-longer-desired partitions with a handoff barrier;
- acquires desired unowned or expired partitions;
- continues renewing while the cluster is intentionally paused.

Acquisition and release are short PostgreSQL transactions. Every ownership
change increments `epoch`. Delivery does not require a long-lived database
transaction and no transaction is held during Kafka I/O.

Schema v3 marks Relay presence rows as ordering-capable. A database claim guard
prevents a v0.2 Relay from moving an ordered row to inflight, even though its
released query does not know about ordering columns. This keeps an ordered row
blocked rather than misordered during an additive rolling upgrade. Operators
must still wait until all relevant Relays are v0.3-capable before producers
start creating ordered streams, because premature ordered volume can starve an
old Relay's bounded unordered claim batch.

## Epoch fencing

An ordered claim returns the current partition epoch. Begin-attempt, retry,
dead, and delivered/stream-advance mutations require all of:

- the event is inflight and leased to the Relay instance;
- the partition is leased to that instance;
- the partition epoch equals the claimed epoch;
- the partition lease is still valid;
- the handoff barrier has passed.

The delivered transition and `next_sequence = N + 1` happen in one PostgreSQL
transaction and only when the stream still expects N. If SQL fails after Kafka
ACK, neither durable transition occurs: N remains recoverable, a duplicate N is
possible, and N+1 remains blocked. A stale epoch cannot claim, start an attempt,
mark a result, or advance a stream.

## Stale-publish timing invariant

Database fencing alone cannot recall a Kafka request already sent by an old
owner. Ordered delivery therefore uses a bounded-publish and delayed-handoff
protocol.

Let:

```text
P = the old owner's configured PublishTimeout
M = the fixed ordered-publish safety margin
L = the old owner's partition lease expiry
H = the takeover handoff_not_before
```

Before each ordered broker call, one atomic SQL statement validates owner and
epoch, increments the attempt, and requires at least `P + M` of lease remaining.
The Relay then creates a fresh context bounded by P, checks that it is not
already cancelled, and performs exactly one synchronous franz-go publish. The
partition row persists P as the owner's maximum publish window.

On graceful release, the old owner sets a barrier of at least `now + P + M`.
On expired-owner takeover, the new owner sets a barrier of at least
`takeover_time + old_P + M`. The new owner may lease and renew the partition
during this interval but cannot claim or publish ordered work before H. Thus:

```text
H >= ownership loss/takeover + maximum prior publish window + safety margin
```

franz-go is configured with Kafka producer idempotence disabled, `acks=all`,
zero record retries, a record delivery timeout and produce request timeout no
greater than P. Disabling producer idempotence is intentional: franz-go can
cancel a non-idempotent in-flight request at the caller deadline instead of
keeping it live to resolve producer-sequence state. The relay never calls
Publish with an already expired or cancelled context.

Cancellation bounds the old Relay client's participation; it does not recall a
request that Kafka may already have accepted. If the acknowledgement is lost,
the durable outbox retry may therefore publish N again. The handoff barrier
keeps a replacement from starting until the old client has stopped
participating plus the safety margin, so duplicates remain before advancement:
`N,N,N+1` is valid while `N,N+1,N` is not under the documented client/network
bound. Kafka transactions are not introduced: transactional IDs
would require per-partition producer lifecycle and fencing coordination while
still not atomically committing PostgreSQL state, so they do not remove the
ACK/SQL crash duplicate and would add a second durable protocol.

Residual assumptions are explicit: after the client deadline, the broker or
network must not retain an unaccepted request and process it outside the bounded
handoff window. A request already accepted before the ambiguous failure may be
visible after cancellation and may be duplicated by retry. An arbitrarily
faulty broker/network that delays accepting old request bytes past the client
bound can violate strict non-regression, as can external publication to the
topic with the same stream key. Clock comparisons and lease/barrier creation
use PostgreSQL time, so host clock skew does not weaken the database invariant.

## Ordered claim and publish flow

An ordered row is eligible only when it is pending and due, or safely
recoverable inflight; its sequence equals `ordering_streams.next_sequence`; its
partition is owned by the Relay at a valid epoch; the handoff barrier has
passed; and durable pause is false. Claim commits before broker I/O.

The Relay adds authoritative broker headers:

```text
emitlane-ordering-key
emitlane-sequence
emitlane-ordering-partition
```

These system values replace conflicting user headers. The effective Kafka key
keeps all records for a stream on one Kafka partition.

## Pause, replay, and retention

Pause prevents all new unordered and ordered claims. Partition reconciliation
and renewal continue so an intentional pause does not cause needless ownership
churn. Already-started broker calls retain the existing shutdown/at-least-once
semantics.

Default replay of an ordered source fails with an explicit conflict. An operator
may request `ordering_mode=unordered` (CLI `--unordered`). The clone gets a new
UUID, has no active ordering key/sequence/partition, and cannot alter stream
progress. Its headers retain `emitlane-original-ordering-key` and
`emitlane-original-sequence` as audit provenance. EmitLane never invents a new
domain sequence.

Delivered-event retention may remove historical outbox rows but never removes
`ordering_streams`. The durable next sequence therefore survives cleanup and an
old sequence cannot be reintroduced. v0.3 does not auto-delete stream metadata.

## Operational state

Admin API and CLI inspection compute, without exposing payloads:

- expected sequence and next event identity/status/attempts;
- lowest future sequence, gap size, and gap age;
- stream state: `ready`, `inflight`, `retry_wait`, `gap`, or `dead_blocked`;
- virtual partition, desired/actual owner, epoch, lease and handoff state.

Metrics aggregate bounded counts and durations. Ordering keys, event IDs,
request IDs, Relay instance IDs, and owner names are never metric labels.
Structured logs and operation-scoped traces may contain ordering identity and
epoch but never payloads. Doctor validates schema version 3, both ordering
tables, exactly 64 seed rows, required columns, constraints, indexes, and
ownership query privileges.

## Migration, permissions, and downgrade

Migration 3 is additive: nullable outbox ordering columns, durable stream and
partition tables, Relay capability metadata, indexes, and constraints. Existing
unordered writes and the v0.2 unordered path remain valid without per-event
stream or partition work.

Safe rollout is: migrate to v3; upgrade all Relays; verify ownership; then
upgrade producers and enable ordered writes. Writer roles need insert/select/
update on `ordering_streams`. Relay roles need select/update on ordering tables
plus existing outbox and presence privileges. Operator roles need read access to
ordering state. No role needs superuser.

The v3 down migration refuses while any ordered stream metadata or ordered
outbox row exists. Sequencing history must be deliberately retired outside the
migration before downgrade; it is never silently destroyed.

## Explicit non-guarantees

v0.3 does not provide global or cross-destination order, sequence allocation,
gap repair, skipping, exactly-once publication or consumption, ordering against
external producers, ordering after an unbounded Kafka client violation, strict
latency for a hot stream, or automatic deletion of stream state. Domain sequence
validity and eventual resolution of missing/dead sequences remain application
and operator responsibilities.
