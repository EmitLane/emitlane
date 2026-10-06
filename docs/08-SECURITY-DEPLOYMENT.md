# Security and deployment

This guide describes the implemented v0.9.1 deployment boundary. See the
[configuration reference](CONFIGURATION.md) for every environment setting and
[Kafka security](KAFKA_SECURITY.md) for TLS, mTLS, SASL and credential rotation.

## Database roles

Use a separate migration owner and non-superuser runtime roles. The
[Quickstart role grants](QUICKSTART.md#postgresql-roles) cover writer, relay,
consumer, read-only inspection and operator mutations on schema 4. Apply the
migrations before granting access; add business-table permissions separately.

Standalone startup and readiness read `schema_migrations`. Admin API uses the
relay pool and credentials, so enabling mutations also requires operator
privileges on that role. HTTP authentication does not supply SQL privileges.

Writer owns no business transaction. Managed handlers receive a runtime-owned
transaction and must neither commit nor roll it back. External effects require
a stable idempotency key or a downstream Outbox write in the handler transaction.

## Listeners and authentication

Admin API is disabled by default. Its default address is `127.0.0.1:8081`.
Explicit loopback binds can run without a token; wildcard and non-loopback binds
without `EMITLANE_ADMIN_TOKEN` fail startup validation. Configured tokens are
required for requests and compared in constant time after hashing.

The listener uses HTTP; it does not terminate TLS. For remote administration,
provide HTTPS through your private ingress or proxy and restrict network access.
Bearer authentication gives access to all Admin API operations; there are no
per-token scopes. See [Admin API](ADMIN_API.md) for redaction and audit behavior.

The separate runtime listener defaults to `:8080` and serves `/healthz`,
`/readyz` and `/metrics` without bearer authentication. Restrict it to the
monitoring/orchestration network. Durable pause and dead domain work do not fail
probes. Readiness checks PostgreSQL, current schema history and Kafka reachability;
it does not prove topic ACLs, retention or successful end-to-end processing.

## Configuration

The binary reads environment variables. A local plaintext development example:

```bash
export EMITLANE_DATABASE_URL='postgres://emitlane:emitlane@localhost:5432/emitlane?sslmode=disable'
export EMITLANE_KAFKA_BROKERS='localhost:19092'
export EMITLANE_HTTP_ADDR='127.0.0.1:8080'
export EMITLANE_ADMIN_ENABLED='false'
emitlane run
```

Provision production database and broker credentials through your secret system.
Use verified PostgreSQL TLS as configured through the connection URL, and
configure Kafka TLS/SASL as described in [Kafka security](KAFKA_SECURITY.md).
There is no YAML configuration loader or `otel.enabled` setting.

## Payload and diagnostic safety

Outbox payloads may contain PII or secrets; include them in your retention and
backup policy. Ordinary CLI/API inspection omits payload, key and headers.
CLI `events inspect --payload` explicitly prints stored content. HTTP inspection
requires both `EMITLANE_ADMIN_EXPOSE_PAYLOAD=true` and `?payload=true`.
Inbox inspection never exposes its source payload or lease token.

EmitLane does not log full event payloads by default. Application callbacks must
also avoid logging credentials or raw content. Ordering keys and correlation
metadata may contain business identifiers; keep them out of metric labels.

Operator mutations commit with their audit record in PostgreSQL. Replay creates
a new identity and may repeat business effects; read [Replay safety](REPLAY.md).

## Containers and orchestration

Published containers run as a non-root user. Pin a release tag, provide runtime
settings and apply `emitlane migrate up` as a separate deployment step before
starting the relay. Use the [Quickstart](QUICKSTART.md) for the Compose example;
its plaintext connections and development admin token are not production setup.

Use `/healthz` for liveness and `/readyz` for dependency readiness. A Kafka outage
should not restart a healthy relay repeatedly. Configure network controls for
both listeners, and scrape using the [Monitoring guide](MONITORING.md).

No Helm chart or Kubernetes-specific deployment package is supplied. The binary
can run under an ordinary process supervisor or in a container; orchestrator
configuration belongs to the deployment.

## Shutdown and restore

SIGTERM stops new claims and drains started publishes for `ShutdownTimeout`,
then cancels them. Database acknowledgements and cleanup can add time; this is
not a strict process-exit deadline. Give the supervisor additional grace.
Interrupted work remains recoverable by leases and may be duplicated after an
ambiguous Kafka acknowledgement.

Back up business data and EmitLane state consistently. PostgreSQL and Kafka
restore are separate operations; reconcile group offsets and retained records
before resuming consumers. See [Backup and restore](BACKUP_RESTORE.md) and
[Operations](OPERATIONS.md).
