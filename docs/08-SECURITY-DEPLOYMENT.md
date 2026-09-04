# Security, configuration and deployment

## Database permissions

Avoid `SUPERUSER` requirements.

### Writer role

Needs roughly:

```text
USAGE on emitlane schema
INSERT on emitlane.outbox_events
SELECT, INSERT, UPDATE on emitlane.ordering_streams for ordered writes
ability to execute notification path if used
```

### Relay role

Needs roughly:

```text
SELECT
UPDATE
SELECT, UPDATE on emitlane.ordering_streams and emitlane.ordering_partitions
DELETE only if retention implementation requires it
LISTEN capability through normal connection
```

### Migration role

Can be separate and own DDL rights.

Exact GRANT scripts are in [QUICKSTART.md](QUICKSTART.md).
Operator inspection needs `SELECT` on both ordering tables. These permissions
do not require superuser. Ordering keys can contain business identifiers and
must not be placed in Prometheus labels.

## Payload safety

- payloads may contain sensitive data;
- logs must not emit payloads by default;
- Admin API should redact/disable payload viewing unless explicitly enabled;
- CLI inspect should have a clear opt-in to print raw payload;
- documentation must remind users that storing PII in an outbox creates retention obligations.

## Admin API

Default:

```text
enabled = false
```

If enabled, default bind:

```text
127.0.0.1
```

If configuration binds Admin API publicly (`0.0.0.0`) without authentication, recommended behavior is to reject startup rather than quietly expose control endpoints.

Authentication method is intentionally unresolved for post-v0.1 design.

## Configuration example

```yaml
database:
  url: "${DATABASE_URL}"

relay:
  batch_size: 100
  concurrency: 8
  poll_interval: 5s
  lease_duration: 30s
  ordering_rebalance_interval: 2s
  ordering_lease_duration: 30s
  ordering_safety_margin: 1s

retry:
  max_attempts: 10
  base_delay: 1s
  max_delay: 30m
  jitter: true

broker:
  type: kafka
  kafka:
    brokers:
      - kafka-1:9092
      - kafka-2:9092

telemetry:
  prometheus:
    enabled: true
    port: 9090
  otel:
    enabled: true

admin:
  enabled: false

retention:
  delivered: 168h
  dead: 0
```

## Docker target

Desired user experience:

```bash
docker run \
  -e EMITLANE_DATABASE_URL=... \
  -e EMITLANE_KAFKA_BROKERS=... \
  ghcr.io/emitlane/emitlane
```

Container should:

- run as non-root where practical;
- expose health/readiness endpoints;
- handle SIGTERM gracefully;
- stop claiming new work before shutdown;
- allow in-flight publish operations a bounded drain period.

## Kubernetes — later, not core v0.1

Helm direction:

```text
Deployment
Service
ConfigMap
Secret references
PodDisruptionBudget
ServiceMonitor (optional)
```

Do not require Kubernetes for normal use.

## Graceful shutdown

Target sequence:

```text
SIGTERM
↓
stop listening/claiming
↓
finish or cancel bounded in-flight publishes
↓
write safe final states where possible
↓
close Kafka
↓
close DB
```

If shutdown interrupts state acknowledgement, leases must still make eventual recovery possible.
