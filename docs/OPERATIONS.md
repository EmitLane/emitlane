# Operations runbook

## Observe

Run `emitlane stats`, `emitlane relay status`, and `emitlane doctor` first.
Prometheus exposes queue depth, oldest pending age, pause state, active/stale
relays, replay counts, mutation results, and control/presence failures. A stale
relay is a visibility signal; leases remain the delivery recovery mechanism.

Event list and inspection hide payload and headers by default:

```bash
emitlane events list --status dead --destination orders.events
emitlane events inspect <event-id> --json
emitlane audit list --json
```

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

