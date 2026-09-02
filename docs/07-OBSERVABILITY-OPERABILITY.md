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
      emitlane.publish
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
```

Status, general event inspection, replay, and benchmark commands remain
post-v0.1 roadmap items.

### `doctor`

Desired checks:

```text
✓ PostgreSQL connection
✓ schema version
✓ required permissions
✓ Kafka connectivity
✓ expected indexes
✓ LISTEN/NOTIFY path
✓ clock sanity
✓ relay instance visibility
```

`doctor` should become a differentiating DX feature.

## Admin API — post-v0.1

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
```

All mutating operator actions should eventually be audit logged.

## Replay

Example future CLI:

```bash
emitlane replay \
  --destination orders.events \
  --from "2026-09-01T10:00:00Z" \
  --to "2026-09-01T11:00:00Z"
```

Replay design goals:

- preserve original event identity where useful;
- attach a separate replay identifier/provenance;
- make it visible to downstream consumers that a delivery is a replay;
- do not accidentally destroy dedup semantics without documentation.

Replay semantics require a separate detailed design before implementation.

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
