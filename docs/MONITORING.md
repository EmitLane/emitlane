# Monitoring EmitLane

The starter [Prometheus rules](../deploy/monitoring/alerts.yml) and
[Grafana dashboard](../deploy/monitoring/grafana-dashboard.json) use the existing
runtime metrics plus v0.9 snapshot freshness instruments. Thresholds are examples,
not universal delivery SLOs. Adjust them to your workload, retry policy, lease
settings and maintenance windows before routing alerts to an on-call team.

## Install

1. Scrape each relay's `GET /metrics` on the private runtime listener. Start with
   [the example scrape configuration](../deploy/monitoring/prometheus.example.yml).
   Put relays sharing one PostgreSQL database in one job. Use a different job
   beginning with `emitlane` for each logical database; preserve any environment
   labels consistently on all its targets. Do not group unrelated databases
   under the same job.
2. Copy `alerts.yml` to the configured `rule_files` path. Check your entire
   Prometheus configuration with `promtool check config` before reloading it.
   Configure Alertmanager receivers and maintenance silences yourself.
3. Import `grafana-dashboard.json` using Grafana's dashboard import. Select the
   Prometheus datasource, job/database and instances. For provisioning, set
   `DS_PROMETHEUS` to your datasource UID in the dashboard JSON before installation.
4. Managed consumer metrics must be registered and served by the application
   running that consumer. Its configured consumer/topic labels appear after
   runtime observations. The standalone relay does not run your business handlers.

The runtime listener serves metrics and probes without Admin API authentication.
Restrict it to your monitoring network. Keep the authenticated admin listener
separate; a scrape credential is not an administrative credential.

## Read the signals correctly

Database-wide gauges (queue depth, ordering, heartbeat and pause state) repeat
on every relay. **Do not sum them across replicas.** The dashboard uses `max` for
cluster counts within the selected job. The maximum may briefly lag a decreasing
queue until every relay refreshes. Per-process counters can be summed when a
combined rate is useful. Consumer lag observations can repeat across group
members; inspect their maximum, not their sum.

`emitlane_stats_last_success_timestamp_seconds` advances after a complete
successful Relay database snapshot. Zero means none has completed yet.
`emitlane_stats_interval_seconds` is zero when collection is disabled or the
registry has no Relay. `emitlane_stats_snapshot_failures_total` counts failed
reads. Failures preserve the last successful gauges and timestamp; they do not
reset the queue to zero. A fresh scrape therefore does not prove a fresh database
snapshot. Keep clocks synchronized when comparing Unix timestamps.

Inbox `consumer_dead_events` is a runtime observation, not a durable total.
Use `emitlane inbox stats` for durable state. Missing consumer series means
unobserved/uninstrumented data, not zero lag or a healthy consumer.

The enqueue counter records successful Writer INSERT calls, including caller
transactions that later roll back. Delivered counters record Outbox transitions;
none proves a business handler received the event. Duplicate counters count
recognized redelivery, not duplicate business effects.

Integrity metrics only cover checks invoked in that runtime registry. Schedule
bounded authenticated `GET /v1/integrity` checks separately if you want these
alerts. A CLI check in another process is not exported by the relay. Capture its
JSON and exit status in your own job monitoring. Use a full CLI check off peak
when investigation needs a read-only snapshot of retained durable state.

This package does not measure broker ISR, replication, topic retention, database
connection wait, autovacuum, disk headroom or container memory. Install the
appropriate Kafka/PostgreSQL/host exporters and alerts. Go process RSS excludes
PostgreSQL, Kafka and other containers.

## Validate changes

`promtool` checks syntax and evaluates synthetic time series, including intentional
pause, independent instances, disabled/stale snapshot collection, transient spikes,
automatic consumer retries and counter resets. See the upstream
[rule testing format](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).
The dashboard uses Grafana's
[JSON model](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/view-dashboard-json-model/).

```bash
promtool check rules deploy/monitoring/alerts.yml
promtool test rules deploy/monitoring/alerts.test.yml
python3 deploy/monitoring/check-dashboard.py > /tmp/emitlane-dashboard-rules.json
promtool check rules /tmp/emitlane-dashboard-rules.json
```

CI validates the same files. Go tests check that dashboard queries reference
exposed EmitLane metric families. This validates configuration and query wiring;
check your own datasource, target labels, dashboards and alert routing in the
actual deployment as well.

## Alert runbooks

### EmitLaneTargetDown

Check process liveness and the scrape path. A failed readiness probe during a broker outage must not trigger a restart loop.

### EmitLaneStatsStale

Queue and ordering gauges retain their last successful values. Check PostgreSQL connectivity and pool capacity before trusting them.

### EmitLaneBacklogOld

Inspect relay status, broker failures, retries and ordering blocks. Do not delete pending events or assume a stopped relay has lost them.

### EmitLaneDeadEvents

Inspect redacted dead events and repair the cause. Retry the original identity after review; replay creates a new identity.

### EmitLaneOrderingGap

Inspect the affected stream and supply the missing domain sequence. Never advance the cursor manually.

### EmitLaneOrderingDeadBlocked

Repair and retry the expected dead event. Later events remain recoverable; historical replay must not move the cursor.

### EmitLanePausedBacklog

Confirm that the durable cluster pause is intentional. Resume with an operator reason when maintenance is complete.

### EmitLanePublishFailures

Inspect Kafka connectivity, authentication and acknowledgement errors. Keep retryable events durable; assess the configured retry budget.

### EmitLaneControlFailures

Check PostgreSQL and pool availability. New claims wait for readable control state; do not bypass pause checks.

### EmitLanePresenceFailures

Check PostgreSQL and connection capacity. Presence is visibility data; delivery recovery still depends on event leases.

### EmitLaneConsumerBlocked

Inspect Inbox state and the pause reason. Repair the cause; do not commit Kafka offsets past an unprocessed record.

### EmitLaneConsumerLag

Inspect blocked partitions, handler latency and worker capacity. Check Kafka retention headroom before increasing concurrency.

### EmitLaneRelaySaturated

Compare broker and claim latency with database pool capacity. Raising concurrency without capacity can slow recovery.

### EmitLaneIntegrityViolation

Capture the integrity JSON report and audit history. Avoid ad-hoc SQL repairs. Checks must be scheduled separately; absence of violations is not proof that a check ran.

### EmitLaneStaleRelays

Check the named process and lease takeover. Stale presence after process death is expected; it does not itself prove lost events.

For commands and recovery semantics, see [Operations](OPERATIONS.md),
[Integrity](INTEGRITY.md) and [Backup and restore](BACKUP_RESTORE.md).
