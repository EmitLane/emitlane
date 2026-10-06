# Configuration

This is the standalone v0.9.1 environment reference, corresponding to
[`config.Load`](../config/config.go) and
[`config.LoadKafkaSecurity`](../config/kafka.go). The binary reads environment
variables; it has no YAML configuration loader or `--config` flag.

`run` and `doctor` load the complete configuration. Database-only CLI commands
use `EMITLANE_DATABASE_URL`; they do not require Kafka settings. Embedded SDK
applications configure their packages directly. An empty optional environment
setting generally selects its default. Durations use Go notation such as `250ms`,
`10s` and `168h`; documented disabling values also accept `0`.

## Database and process

| Variable | Default | Meaning and validation |
| --- | --- | --- |
| `EMITLANE_DATABASE_URL` | required | PostgreSQL connection URL. |
| `EMITLANE_HTTP_ADDR` | `:8080` | Unauthenticated health, readiness and metrics listener; restrict its network exposure. |
| `EMITLANE_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `EMITLANE_INSTANCE_ID` | hostname + random suffix | Unique process lease owner, without surrounding whitespace. Never share an ID between concurrent relays. |
| `EMITLANE_DB_MAX_CONNS` | `10` | Pool maximum, at least 1. |
| `EMITLANE_DB_MIN_CONNS` | `2` | Between 0 and the maximum. Lower it if reducing the maximum below 2. |
| `EMITLANE_DB_MAX_CONN_LIFETIME` | `1h` | Positive pool connection lifetime. |

## Kafka

| Variable | Default | Meaning and validation |
| --- | --- | --- |
| `EMITLANE_KAFKA_BROKERS` | required | Comma-separated seed broker addresses. |
| `EMITLANE_KAFKA_CLIENT_ID` | `emitlane` | Publisher client ID. |
| `EMITLANE_KAFKA_AUTO_CREATE_TOPICS` | `false` | Development convenience; provision production topics explicitly. |
| `EMITLANE_KAFKA_TLS_ENABLED` | `false` | Verified TLS 1.2 or later. |
| `EMITLANE_KAFKA_TLS_CA_FILE` | empty | Optional PEM trust bundle, replacing system roots. Requires TLS. |
| `EMITLANE_KAFKA_TLS_CERT_FILE` | empty | Optional mTLS certificate; supply together with key file. Requires TLS. |
| `EMITLANE_KAFKA_TLS_KEY_FILE` | empty | Optional mTLS private key; supply together with certificate file. Requires TLS. |
| `EMITLANE_KAFKA_TLS_SERVER_NAME` | empty | Certificate-name override for every broker connection; normally use each advertised broker's own hostname. Requires TLS. |
| `EMITLANE_KAFKA_SASL_MECHANISM` | empty | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`, case-insensitive. All require TLS. |
| `EMITLANE_KAFKA_SASL_USERNAME` | empty | Required with SASL; no unused credentials without a mechanism. |
| `EMITLANE_KAFKA_SASL_PASSWORD` | empty | Inline password, mutually exclusive with password file. |
| `EMITLANE_KAFKA_SASL_PASSWORD_FILE` | empty | Mounted password file, mutually exclusive with inline password. |

Secret files are validated and snapshotted at client construction. Recreate
clients to rotate credentials; see [Kafka security](KAFKA_SECURITY.md).

## Relay and retries

| Variable | Default | Meaning and validation |
| --- | --- | --- |
| `EMITLANE_RELAY_BATCH_SIZE` | `100` | Positive per-claim limit, additionally bounded by free worker slots. |
| `EMITLANE_RELAY_CONCURRENCY` | `4` | Positive maximum active publishes. |
| `EMITLANE_RELAY_POLL_INTERVAL` | `5s` | Positive polling fallback; standalone currently accepts at most 5s. |
| `EMITLANE_RELAY_LEASE_DURATION` | `30s` | Expiring event lease; must exceed publish timeout. No renewal during publish. |
| `EMITLANE_RETRY_MAX_ATTEMPTS` | `10` | At least 1; exhausted work becomes durable `dead`. |
| `EMITLANE_RETRY_BASE_DELAY` | `1s` | Positive exponential backoff base, with full jitter. |
| `EMITLANE_RETRY_MAX_DELAY` | `30m` | Backoff cap, at least the base delay. |
| `EMITLANE_PUBLISH_TIMEOUT` | `10s` | Positive bound, below event lease duration. |
| `EMITLANE_SHUTDOWN_TIMEOUT` | `15s` | Positive started-work drain period; cleanup can add time. |
| `EMITLANE_STATS_INTERVAL` | `5s` | Database snapshot refresh; `0` disables collection. |
| `EMITLANE_CONTROL_CHECK_INTERVAL` | `2s` | Positive durable pause-state poll interval. |
| `EMITLANE_RELAY_HEARTBEAT_INTERVAL` | `10s` | Positive relay presence heartbeat interval. |
| `EMITLANE_RELAY_STALE_AFTER` | `30s` | Presence threshold, greater than heartbeat interval. |

The standalone polling bound comes from `relay.Config.IdleBackoffMax`, which
remains 5s and has no environment setting. Embedded callers can raise it together
with `PollInterval`. Values above 5s currently fail standalone startup validation;
this guide records the existing limitation rather than changing the scheduler.

## Ordered partition ownership

| Variable | Default | Meaning and validation |
| --- | --- | --- |
| `EMITLANE_ORDERING_REBALANCE_INTERVAL` | `2s` | Positive reconciliation interval, shorter than partition lease. |
| `EMITLANE_ORDERING_LEASE_DURATION` | `30s` | Positive renewable partition lease, greater than publish timeout plus safety margin. |
| `EMITLANE_ORDERING_SAFETY_MARGIN` | `1s` | Positive addition to the bounded old-owner publish/handoff window. |

These settings do not renew outbox event leases. See
[Ordered delivery](ORDERED_DELIVERY.md) for protocol assumptions.

## Retention

| Variable | Default | Meaning and validation |
| --- | --- | --- |
| `EMITLANE_RETENTION_DELIVERED` | `168h` | Delivered-row retention; `0` disables cleanup. Non-negative. |
| `EMITLANE_RETENTION_INTERVAL` | `1m` | Non-negative cleanup interval; positive when retention is enabled. |
| `EMITLANE_RETENTION_BATCH` | `1000` | Non-negative delete batch size; positive when retention is enabled. |

Pending, inflight and dead events are not deleted by cleanup. Inbox markers and
ordered stream cursors have no automatic pruning. Retention must preserve the
required replay and restore horizons; see [Backup and restore](BACKUP_RESTORE.md).

## Admin API

| Variable | Default | Meaning and validation |
| --- | --- | --- |
| `EMITLANE_ADMIN_ENABLED` | `false` | Enable the separate operational listener. |
| `EMITLANE_ADMIN_ADDR` | `127.0.0.1:8081` | `host:port`; non-loopback or wildcard binds require a token. |
| `EMITLANE_ADMIN_TOKEN` | empty | Bearer token; at most 4096 bytes, no CR/LF. Optional only on explicit loopback. |
| `EMITLANE_ADMIN_EXPOSE_PAYLOAD` | `false` | Permit opt-in event payload/key/header inspection; ordinary reads stay redacted. |

The Admin API uses the relay's database credentials. The runtime listener does
not use Admin API bearer authentication. See [Admin API](ADMIN_API.md).

## Ecommerce and Compose settings

The example reads `DATABASE_URL`, `KAFKA_BROKERS`, `HTTP_ADDR` and `ORDERS_TOPIC`.
Their defaults are the local development PostgreSQL URL, `localhost:19092`,
`:8081` and `orders.events`. It also loads the shared `EMITLANE_KAFKA_*` security
settings. Its defaults are examples, not production credentials.

Compose host-port overrides are `EMITLANE_POSTGRES_PORT` (5432),
`EMITLANE_KAFKA_PORT` (19092), `EMITLANE_HTTP_PORT` (8080),
`EMITLANE_ADMIN_PORT` (8082) and `ECOMMERCE_HTTP_PORT` (8081). These are Compose
substitution variables, not standalone process configuration. Compose enables
Admin API on port 8082 with the development token `emitlane-local-admin`; the
standalone Admin API remains disabled by default.
