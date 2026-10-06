# Observability and operability

This reference describes the v0.9.1 instruments in
[`telemetry/metrics.go`](../telemetry/metrics.go). For ready-to-adapt Prometheus
rules and Grafana queries, start with [Monitoring](MONITORING.md).

## Prometheus metrics

The standalone Relay serves `/metrics`, `/healthz`, and `/readyz` on
`EMITLANE_HTTP_ADDR` (default `:8080`). SDK applications attach their own registry
and HTTP handler. Health probes and metrics are separate from the Admin API and
have no built-in authentication. Protect this listener at the deployment layer.

All 55 instruments are listed below. Counters count observations since process
start; gauges report current observations; histograms expose `_bucket`, `_sum`
and `_count` series. Names ending in `_seconds` use seconds, except
`stats_last_success_timestamp_seconds`, which is a Unix timestamp.
`relay_worker_saturation_ratio` is active publishes divided by configured
capacity. Other instruments count the records, operations, workers, partitions
or streams described in the table; `relay_paused` uses 0/1.

| Instrument | Type | Labels | Meaning |
|---|---|---|---|
| `emitlane_stats_interval_seconds` | gauge | — | Configured Relay database snapshot interval; zero means disabled or no Relay configured. |
| `emitlane_stats_last_success_timestamp_seconds` | gauge | — | Unix time of the last successful Relay database snapshot; zero until the first success. |
| `emitlane_stats_snapshot_failures_total` | counter | — | Failed Relay database snapshot reads; queue gauges retain their last successful values. |
| `emitlane_events_enqueued_total` | counter | — | Successful Writer INSERT calls; the caller-owned transaction may still roll back. |
| `emitlane_events_delivered_total` | counter | — | Outbox events marked delivered after broker acknowledgement. |
| `emitlane_events_failed_total` | counter | result | Broker publish attempts that failed. |
| `emitlane_events_retried_total` | counter | — | Failed events scheduled for retry. |
| `emitlane_events_dead_total` | counter | — | Events moved to dead state after policy exhaustion. |
| `emitlane_pending_events` | gauge | — | Current number of pending outbox events. |
| `emitlane_inflight_events` | gauge | — | Current number of inflight outbox events. |
| `emitlane_dead_events` | gauge | — | Current number of dead outbox events. |
| `emitlane_delivery_duration_seconds` | histogram | — | Time from event creation to delivered state. |
| `emitlane_publish_duration_seconds` | histogram | — | Broker publish latency after claim commit. |
| `emitlane_oldest_pending_seconds` | gauge | — | Age in seconds of the oldest pending outbox event. |
| `emitlane_relay_paused` | gauge | — | Whether durable cluster-wide relay pause is enabled (1) or disabled (0). |
| `emitlane_relay_instances_active` | gauge | — | Relay instances with a recent heartbeat and no stopped marker. |
| `emitlane_relay_instances_stale` | gauge | — | Relay instances whose heartbeat is older than the stale threshold. |
| `emitlane_replay_batches_total` | counter | — | Successfully committed single-event and batch replay operations. |
| `emitlane_replayed_events_total` | counter | — | New outbox events created by successfully committed replay operations. |
| `emitlane_admin_mutations_total` | counter | action, result | Administrative mutation attempts by bounded action and result. |
| `emitlane_control_read_failures_total` | counter | — | Failures reading durable relay control state. |
| `emitlane_relay_presence_failures_total` | counter | operation | Best-effort relay presence failures by operation. |
| `emitlane_ordering_streams` | gauge | — | Durable ordered streams. |
| `emitlane_ordering_streams_blocked` | gauge | — | Ordered streams blocked by retry wait, gap, or dead event. |
| `emitlane_ordering_streams_gap` | gauge | — | Ordered streams whose expected sequence is missing while a future sequence exists. |
| `emitlane_ordering_streams_dead_blocked` | gauge | — | Ordered streams blocked by a dead expected event. |
| `emitlane_ordering_partitions_owned` | gauge | — | Virtual ordering partitions with a valid owner outside handoff. |
| `emitlane_ordering_partitions_handoff` | gauge | — | Virtual ordering partitions waiting for the stale-publish handoff barrier. |
| `emitlane_ordering_partition_acquisitions_total` | counter | — | Virtual ordering partitions newly observed as owned by this Relay. |
| `emitlane_ordering_partition_rebalances_total` | counter | — | Desired ownership maps changed after Relay membership changes. |
| `emitlane_ordering_delivery_wait_seconds` | histogram | — | Wait from ordered event availability to broker publish start. |
| `emitlane_ordering_gap_age_seconds` | gauge | — | Age of the oldest currently observed ordered gap. |
| `emitlane_ordering_fenced_attempts_total` | counter | operation | Expected ordered transitions discarded after losing durable authority. |
| `emitlane_integrity_checks_total` | counter | mode, result | Completed integrity checks by bounded mode and result. |
| `emitlane_integrity_check_duration_seconds` | histogram | mode | Integrity check duration by bounded mode. |
| `emitlane_consumer_records_total` | counter | consumer, result | Managed consumer records by configured consumer and bounded result. |
| `emitlane_consumer_processing_duration_seconds` | histogram | consumer | Managed consumer processing duration by configured consumer. |
| `emitlane_consumer_retries_total` | counter | consumer | Durable managed consumer retries scheduled. |
| `emitlane_consumer_duplicates_total` | counter | consumer | Already-processed managed consumer deliveries. |
| `emitlane_consumer_dead_events` | gauge | consumer | Dead managed Inbox events observed by this runtime. |
| `emitlane_consumer_inflight` | gauge | consumer | Managed handlers currently executing. |
| `emitlane_consumer_rebalances_total` | counter | consumer | Managed consumer assignment callbacks. |
| `emitlane_consumer_paused_partitions` | gauge | consumer, reason | Partitions paused by bounded reason. |
| `emitlane_consumer_lag_records` | gauge | consumer, topic | Managed consumer lag by configured consumer and topic. |
| `emitlane_consumer_active_workers` | gauge | consumer | Managed consumer group workers currently running. |
| `emitlane_consumer_worker_capacity` | gauge | consumer | Configured managed consumer worker capacity. |
| `emitlane_consumer_backpressure_total` | counter | consumer, reason | Managed consumer entries into bounded blocked states. |
| `emitlane_consumer_poll_batch_size` | histogram | consumer | Records returned per managed consumer poll. |
| `emitlane_relay_active_workers` | gauge | — | Relay publishes currently executing. |
| `emitlane_relay_worker_capacity` | gauge | — | Configured maximum concurrent Relay publishes. |
| `emitlane_relay_worker_saturation_ratio` | gauge | — | Active Relay publishes divided by configured capacity. |
| `emitlane_relay_claim_size` | histogram | — | Events returned by one bounded Relay claim. |
| `emitlane_relay_claim_duration_seconds` | histogram | — | Duration of one bounded Relay claim operation. |
| `emitlane_relay_backpressure_total` | counter | reason | Relay entries into bounded backpressure states. |
| `emitlane_relay_wakeups_total` | counter | source | Coalesced Relay wake-ups by bounded source. |

Labels use bounded enums or configured consumer/topic names. Never put event
IDs, ordering keys, offsets, payload values, credentials, or raw errors into a
metric label. The failure result is `retryable` or `permanent`; consumer results
are `processed`, `duplicate`, `retry`, `dead`, `protocol_error`, or `error`.

`events_enqueued_total` increments after a successful Writer INSERT when
`outbox.WithMetrics` is configured. The caller can still roll back its
transaction; a Relay in another process cannot observe that counter. It is not
a count of globally committed events.

Queue, ordering, control and presence gauges are Relay database snapshots. A
failed read retains the last successful values: check `stats_interval_seconds`,
`stats_last_success_timestamp_seconds`, and `stats_snapshot_failures_total`
before trusting a flat queue graph. Managed consumer gauges are runtime
observations, not a complete durable Inbox inventory; use `emitlane inbox stats`
or `GET /v1/inbox/stats` for that inventory.

Watch oldest pending age together with depth, dead/blocked state and freshness.
A small queue can still contain an event stuck for hours. See
[Monitoring](MONITORING.md) for alert thresholds and their limitations.

## OpenTelemetry

EmitLane instruments these operations:

- Writer: `emitlane.enqueue`.
- Unordered Relay: `emitlane.claim`, `emitlane.publish`.
- Ordered storage and Relay: `emitlane.ordering.claim`,
  `emitlane.ordering.publish`, `emitlane.ordering.partition.reconcile`,
  `emitlane.ordering.partition.handoff`, `emitlane.ordering.stream.advance`.
- Legacy Inbox helper: `emitlane.inbox.process`.
- Managed consumer handler: `emitlane.consumer.process`.

Applications must configure an OpenTelemetry TracerProvider and exporter.
The standalone CLI configures W3C propagation but does not install a tracing
exporter or expose OTLP configuration. With the default no-op provider, these
instrumentation calls do not produce exported spans.

The Writer stores `traceparent`/`tracestate`; Relay publishing restores that
context, and the managed consumer extracts it and passes the resulting context
to the handler and downstream Outbox writes. Malformed trace headers do not
fail business processing. W3C baggage is not persisted. Application HTTP,
business-transaction and Kafka-client spans require application or client
instrumentation; EmitLane does not create a `kafka.send` span itself.

Correlation and causation IDs are application metadata, separate from trace
context. Span attributes may identify an event or ordering stream, but never
include its payload. Ownership is not represented by a lifetime span.

## Logging

Runtime logging uses `log/slog`. Relay failure logs include diagnostic event,
destination and attempt metadata. Keep payloads and credentials out of logs;
review application handlers and broker error handling for the same boundary.
Human-readable log text is not a parsing contract.

## Operator interfaces

Use [Operations](OPERATIONS.md) for status, redacted inspection, durable
pause/resume and dead retry; [Ordered delivery](ORDERED_DELIVERY.md) for stream
and partition diagnosis; [Consumer reliability](CONSUMER_RELIABILITY.md) for
Inbox lifecycle inspection; and [Integrity](INTEGRITY.md) for read-only checks.

`emitlane doctor` checks database connectivity, schema and permissions,
indexes/constraints, LISTEN/NOTIFY, clock sanity, Relay visibility, all 64
ordering partitions and Kafka connectivity. Its extra diagnostic privilege is
documented in [Quickstart database roles](QUICKSTART.md#postgresql-roles).
It does not prove that every consumer effect or Kafka record is correct.

The [Admin API](ADMIN_API.md) exposes operational routes on a separate listener.
List dead Outbox events with `GET /v1/events?status=dead`; there is no `/v1/dead`
route. The [Swagger UI](openapi/README.md) renders the same OpenAPI contract.
Mutation audit rows commit atomically with their database state change.

[Replay](REPLAY.md) creates a new identity and can repeat business effects. For
example:

```bash
emitlane replay range \
  --destination orders.events \
  --from "2026-09-01T10:00:00Z" \
  --to "2026-09-01T11:00:00Z" \
  --reason "rebuild after consumer incident"
```

Ordered sources additionally require `--unordered`. Grafana dashboards are
provided as deployment assets; there is no built-in web dashboard.
