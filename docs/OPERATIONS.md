# Operations runbook

## Observe

Run `emitlane stats`, `emitlane relay status`, and `emitlane doctor` first.
Prometheus exposes queue depth, oldest pending age, pause state, active/stale
relays, replay counts, mutation results, and control/presence failures. A stale
relay is a visibility signal; leases remain the delivery recovery mechanism.

For ordered delivery, inspect blocked streams and ownership before changing
event state:

```bash
emitlane ordering streams --blocked
emitlane ordering inspect --destination orders.events --key order:123
emitlane ordering partitions
```

`gap` means the expected sequence is absent while a future sequence exists.
`retry_wait` means the expected event is delayed. `dead_blocked` means the
expected event is dead. v0.3 intentionally has no skip or force-advance command.

Check durable protocol invariants without changing state:

```bash
emitlane integrity check
emitlane integrity check --full --json --timeout 2m
emitlane integrity stream --destination orders.events --key order:123
```

Summary mode is appropriate for frequent production diagnostics. Full mode scans
all retained active ordering state in one read-only repeatable-read snapshot;
run it off peak with a deliberate timeout. Exit 0 means no violations, exit 1
means the check could not complete, and exit 2 means a violation. Warnings are
exit 0 unless `--strict` is set. A bounded findings list never makes summary
counts partial. See [integrity verification](INTEGRITY.md).

Event list and inspection hide payload and headers by default:

```bash
emitlane events list --status dead --destination orders.events
emitlane events inspect <event-id> --json
emitlane audit list --json
```

Managed Inbox state is separate from Outbox relay state:

```bash
emitlane inbox stats --consumer billing-v1
emitlane inbox dead --consumer billing-v1 --limit 50
emitlane inbox inspect --consumer billing-v1 --event-id <uuid> --json
emitlane inbox retry --consumer billing-v1 --event-id <uuid> \
  --reason "handler defect fixed"
```

`retry_wait` is automatic. `dead` deliberately blocks only the affected Kafka
partition; later offsets in that partition must not be skipped. The retry
mutation requires a reason, preserves identity/source/attempt history, and is
audited. Kafka retains the payload, so confirm that its retention horizon still
covers the blocked record before retrying. Inbox reads never expose payload.

## Pause and resume

```bash
emitlane relay pause --reason "Kafka maintenance"
emitlane relay status
emitlane relay resume --reason "maintenance complete"
```

Pause is durable and cluster-wide for v0.2 PostgreSQL relays. PostgreSQL blocks
new claims atomically; notifications make the change fast and polling is the
fallback. In-flight publishes may finish. A paused relay remains healthy and
ready, so orchestration should not restart it.

During a mixed v0.1/v0.2 rollout, a v0.1 relay does not understand pause. Stop
old binaries before relying on the pause control.

v0.3 Relays continue heartbeat and ordered partition lease renewal while
paused. No new ordered or unordered claims start, but in-flight work may finish.

## Retry and replay

Retry only a dead event when its original identity should continue:

```bash
emitlane dead retry <event-id> --reason "dependency repaired"
```

Replay a delivered/dead event only after reviewing [replay safety](REPLAY.md):

```bash
emitlane replay event <event-id> --reason "consumer bug fixed"
emitlane replay range --destination orders.events --type order.created \
  --from 2026-09-02T00:00:00Z --to 2026-09-02T01:00:00Z \
  --reason "consumer incident"
# Preview output makes no changes. Repeat with --execute after review.
```

An ordered historical event is rejected by default. If the business decision is
to deliver it outside its historical stream, acknowledge that explicitly:

```bash
emitlane replay event <event-id> --reason "consumer recovery" --unordered
```

The new event has no active ordering fields; provenance headers preserve the
original key and sequence, and the original stream cursor does not move.

All mutations are durable and audited. Audit contains IDs, actor, reason,
request ID, time, and safe counts—never payload or credentials.

## Failure handling

- Kafka unavailable: leave relay running; events retry and remain recoverable.
- Process death before publish: the lease expires and another relay reclaims.
- Process death after Kafka ACK: a duplicate is possible; consumers use Inbox
  or downstream idempotency.
- Poison event: inspect the redacted metadata and last error; it remains `dead`
  until an operator retries or replays it.
- Presence write failure: delivery continues; alert on the failure metric.
- Control read failure: no new work is claimed until PostgreSQL is readable.
- Ordered gap: supply the missing domain sequence; later events remain durable.
- Ordered dead block: repair and retry the same dead event ID; do not replay it
  into the historical stream.
- Stale Relay or ownership handoff: allow lease expiry plus the displayed
  `handoff_not_before` barrier; recovery is automatic.
- Integrity violation: capture JSON evidence, avoid ad-hoc SQL repair, and use a
  targeted stream check plus audit history to isolate the invariant and cause.
- Integrity warning: inspect the named stream/partition, but do not treat a gap,
  retry wait, stale lease, or expected fencing race as automatic corruption.
- Managed consumer retry: leave the process running; PostgreSQL `available_at`
  is durable and the source partition resumes when due.
- Managed consumer dead: repair the handler/domain condition and use
  `emitlane inbox retry` with an operator reason. Do not advance Kafka offsets
  manually past the record.
- Consumer crash/rebalance: an unfinished transaction rolls back and the lease
  expires; a committed transaction is recognized as processed on redelivery.
- Offset commit failure: do not remove the Inbox row. Redelivery is expected and
  safely commits the already-processed record.

`GET /v1/integrity` is an authenticated, bounded summary check. It is not a
liveness or readiness endpoint and intentionally does not accept full mode.
