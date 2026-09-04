# Observability and operability

## Philosophy

A production outbox is not complete when it can publish. It is complete when operators can understand and repair delivery behavior.

Core question:

> **Where is my event?**

## Prometheus metrics

v0.1 exports:

```text
emitlane_events_enqueued_total
emitlane_events_delivered_total
emitlane_events_failed_total
emitlane_events_dead_total
emitlane_events_retried_total

emitlane_pending_events
emitlane_inflight_events
emitlane_dead_events

emitlane_delivery_duration_seconds
emitlane_publish_duration_seconds
emitlane_oldest_pending_seconds

emitlane_ordering_streams
emitlane_ordering_streams_blocked
emitlane_ordering_streams_gap
emitlane_ordering_streams_dead_blocked
emitlane_ordering_partitions_owned
emitlane_ordering_partitions_handoff
emitlane_ordering_partition_acquisitions_total
emitlane_ordering_partition_rebalances_total
emitlane_ordering_delivery_wait_seconds
emitlane_ordering_gap_age_seconds
```

The failure counter has one bounded `result` label (`retryable` or
`permanent`). Event IDs, destinations, keys, correlation IDs, and error text
are never labels.

`emitlane_events_enqueued_total` is a process-local Writer SDK counter and is
incremented after a successful INSERT call when `outbox.WithMetrics` is used.
Because the caller owns commit/rollback and a standalone relay runs in a
different process, it cannot represent globally committed enqueue volume. Use
the durable queue gauges and application-side commit metrics for that view.

The single most important health signal is likely:

```text
oldest pending event age
```

A queue depth of 10 can be normal. An oldest pending age of three hours usually is not.

## OpenTelemetry

Implemented async trace:

```text
order.create
   │
   ├── db.transaction
   │
   └── emitlane.enqueue
             │
             ▼
      emitlane.publish / emitlane.ordering.publish
             │
             ▼
          kafka.send
             │
             ▼
       order.consume
```

Store/propagate standard W3C context:

```text
traceparent
tracestate
```

Also keep application-level correlation metadata:

```text
correlation_id
causation_id
```

Ordered operations add bounded spans for partition acquire/renew/handoff,
ordered claim, ordered publish, and atomic stream advance. Ownership is not
represented by a lifetime span. Trace attributes may contain event/ordering
identity for diagnosis, but never payload.

## Logging

Use structured logging via `log/slog`.

Example:

```json
{
  "level": "error",
  "event_id": "019...",
  "destination": "orders.events",
  "attempt": 4,
  "worker": "relay-3",
  "error": "kafka: broker unavailable"
}
```

Avoid logging full event payloads by default because payloads may contain secrets or PII.

## CLI

```bash
emitlane version
emitlane migrate up
emitlane dead list
emitlane dead retry EVENT_ID
emitlane doctor
emitlane ordering streams --blocked
emitlane ordering inspect --destination orders.events --key order:123
emitlane ordering partitions
```

Status, redacted event inspection, audited replay, ordering inspection, and the
benchmark harness are implemented.

### `doctor`

Implemented checks include:

```text
✓ PostgreSQL connection
✓ schema version
✓ required permissions
✓ Kafka connectivity
✓ expected indexes
✓ LISTEN/NOTIFY path
✓ clock sanity
✓ relay instance visibility
✓ schema v3 ordering tables, indexes, and constraints
✓ exactly 64 virtual partition rows
✓ ordering ownership query permissions
```

`doctor` should become a differentiating DX feature.

## Admin API

Proposed endpoints:

```text
GET  /v1/events
GET  /v1/events/{id}
GET  /v1/stats
GET  /v1/dead

POST /v1/events/{id}/retry
POST /v1/events/{id}/replay

POST /v1/relay/pause
POST /v1/relay/resume

GET /healthz
GET /readyz

GET /v1/ordering/streams
GET /v1/ordering/stream
GET /v1/ordering/partitions
```

Mutating operator actions are audit logged in the same transaction as their
state change. Ordering reads never expose payload.

## Replay

Implemented CLI example:

```bash
emitlane replay range \
  --destination orders.events \
  --from "2026-09-01T10:00:00Z" \
  --to "2026-09-01T11:00:00Z"
```

Replay properties:

- create a new event identity while preserving source provenance;
- attach a separate replay identifier/provenance;
- make it visible to downstream consumers that a delivery is a replay;
- do not accidentally destroy dedup semantics without documentation.

Ordered historical events require explicit `--unordered`; see
[Replay safety](REPLAY.md).

## Dashboard — later

Illustrative information architecture:

```text
┌─────────────────────────────────────────────────┐
│ EmitLane                            ● Healthy   │
├─────────────────────────────────────────────────┤
│ Pending      Inflight      Dead      Delivered  │
│   142           8           2          84.2M    │
│                                                 │
│ Oldest pending                                  │
│ 1.8 sec                                         │
│                                                 │
│ Delivery rate                                   │
│ █████████████████████  8,421 events/sec         │
│                                                 │
├─────────────────────────────────────────────────┤
│ Recent failures                                 │
│ payment.created   KafkaTimeout        retrying  │
│ order.shipped     InvalidTopic        dead      │
└─────────────────────────────────────────────────┘
```

Do not build this before the relay core is trustworthy.
