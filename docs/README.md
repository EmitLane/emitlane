# EmitLane documentation

Current code: **v0.9.1**, PostgreSQL schema **4**, Admin API **/v1**.
Start with [Quickstart](QUICKSTART.md), then choose the guide for your role.
[Release history](releases/README.md) separates past scope/evidence from current
operating instructions. Future plans and local agent instructions are not the
public product contract.

## Application integration

| Guide | What it covers |
|---|---|
| [Go API](04-GO-API.md) | Writer, legacy Inbox, managed consumer and embedded Relay usage |
| [Delivery guarantees](DELIVERY_GUARANTEES.md) | Atomicity, at-least-once delivery, retry, deduplication and retention |
| [Failure modes](FAILURE_MODES.md) | Crash windows, duplicates and recovery responsibilities |
| [Ordered delivery](ORDERED_DELIVERY.md) | Sequences, stream cursors, ownership and fencing assumptions |
| [Consumer reliability](CONSUMER_RELIABILITY.md) | Managed Inbox, offsets, retry/dead state and handler transactions |
| [Compatibility](COMPATIBILITY.md) | Public package, database, wire and operational contracts |

The numbered [delivery semantics](05-DELIVERY-SEMANTICS.md) and
[ordering](06-ORDERING.md) pages preserve old links and point to these guides.

## Deployment and operations

| Guide | What it covers |
|---|---|
| [Configuration](CONFIGURATION.md) | Environment variables, defaults and validation bounds |
| [Security and deployment](08-SECURITY-DEPLOYMENT.md) | Database roles, listeners, containers and probes |
| [Kafka security](KAFKA_SECURITY.md) | TLS/mTLS, SASL, secret files and client recreation |
| [Operations](OPERATIONS.md) | Inspection, pause/resume, dead recovery and incidents |
| [Admin API](ADMIN_API.md) | HTTP authentication, redaction, mutations and audit |
| [OpenAPI and Swagger UI](openapi/README.md) | Machine-readable contract and browser viewer |
| [Replay](REPLAY.md) | New identities, provenance, bounded selection and ordered sources |
| [Integrity](INTEGRITY.md) | Read-only invariant checks and result interpretation |
| [Observability](07-OBSERVABILITY-OPERABILITY.md) | All metrics, tracing and operator interfaces |
| [Monitoring](MONITORING.md) | Prometheus/Grafana assets, freshness and alert thresholds |
| [Upgrading](UPGRADING.md) | Migration order, binary rollback and downgrade guards |
| [Backup and restore](BACKUP_RESTORE.md) | PostgreSQL/Kafka reconciliation and deduplication horizons |

## Architecture and contributing

| Guide | What it covers |
|---|---|
| [Architecture](02-ARCHITECTURE.md) | Runtime topology, claim paths and package boundaries |
| [Database](03-DATABASE.md) | Schema evolution and durable state transitions |
| [Decisions](decisions/README.md) | Accepted design decisions behind current behavior |
| [Testing and benchmarks](09-TESTING-BENCHMARKS.md) | Authored coverage, CI and qualification boundaries |
| [Benchmarking](BENCHMARKING.md) | Reproducible harness scenarios and output |
| [Performance](PERFORMANCE.md) | Tuning, profiling and workload limits |
| [Relay soak](LOCAL_SOAK.md) | Dedicated fault runner and qualification evidence |
| [Releasing](RELEASING.md) | Release Please, GoReleaser and maintainer publication gates |
| [Contributing](../CONTRIBUTING.md) | Change process and correctness review |
| [Security policy](../SECURITY.md) | Supported release line and vulnerability reporting |
| [Changelog](../CHANGELOG.md) | Release dates and user-visible changes |
