<div align="center">

# EmitLane

**Reliable delivery of committed PostgreSQL outbox events to Kafka.**

[![CI](https://github.com/EmitLane/emitlane/actions/workflows/ci.yml/badge.svg)](https://github.com/EmitLane/emitlane/actions/workflows/ci.yml)
[![CodeQL](https://github.com/EmitLane/emitlane/actions/workflows/codeql.yml/badge.svg)](https://github.com/EmitLane/emitlane/actions/workflows/codeql.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/emitlane/emitlane.svg)](https://pkg.go.dev/github.com/emitlane/emitlane)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`PostgreSQL outbox` → `EmitLane relay` → `Kafka` → `Inbox consumer`

</div>

EmitLane is a small Go reliability layer for the transactional outbox pattern.
Your application writes business data and an event in the same PostgreSQL
transaction; EmitLane then delivers the committed event to Kafka with explicit
**at-least-once** semantics.

It is designed for the failure cases that matter in production: process crashes,
Kafka outages, expired leases, duplicate delivery, poison events, and concurrent
relay instances.

> [!IMPORTANT]
> EmitLane deliberately does not claim end-to-end exactly-once delivery. A crash
> after Kafka acknowledges a message but before PostgreSQL records it as delivered
> can produce a duplicate. Consumers must use Inbox or another idempotency strategy.

## Why EmitLane

- **Atomic writes** — the writer uses the caller-owned `pgx` transaction, so the
  business change and outbox row commit or roll back together.
- **Crash-safe relay** — PostgreSQL remains the source of truth; `LISTEN/NOTIFY`
  only reduces latency and polling remains the recovery path.
- **Horizontal workers** — batches are claimed with `FOR UPDATE SKIP LOCKED`, then
  published outside the database transaction under renewable leases.
- **Explicit failures** — exponential backoff, jitter, bounded attempts, and a
  visible `dead` state for poison events.
- **Duplicate-safe consumers** — Inbox records message processing in the same local
  database transaction as the consumer's effects.
- **Operable by default** — structured logs, Prometheus metrics, OpenTelemetry,
  health endpoints, diagnostics, migrations, and dead-letter commands.

## Quickstart

Docker is the only prerequisite for the complete example:

```bash
docker compose -f docker-compose.example.yml up --build
```

This starts PostgreSQL, Kafka in KRaft mode, the EmitLane migrations and relay,
and an ecommerce producer/consumer example. In another terminal, create an order:

```bash
curl -sS -X POST http://localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"amount": 42}'
```

Then check the relay:

```bash
curl -sS http://localhost:8080/healthz
curl -sS http://localhost:8080/readyz
curl -sS http://localhost:8080/metrics | grep emitlane_
```

See the [complete quickstart](docs/QUICKSTART.md) for port overrides, a manual
local setup, database roles, configuration, and integration tests.

## Writer API

The application keeps ownership of its transaction. EmitLane only inserts the
outbox row using that transaction:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if err := orders.Create(ctx, tx, order); err != nil {
    return err
}

payload, err := outbox.JSON(OrderCreated{OrderID: order.ID})
if err != nil {
    return err
}

_, err = outbox.NewWriter().Enqueue(ctx, tx, outbox.Event{
    Destination: "orders.events",
    Type:        "order.created",
    Key:         []byte(order.ID),
    Payload:     payload,
})
if err != nil {
    return err
}

return tx.Commit(ctx)
```

Payloads are stored as opaque `BYTEA`. JSON is a convenience, not a storage
requirement.

## Delivery model

```text
pending ──claim──> processing ──Kafka ACK──> delivered
   ▲                    │
   │                    ├──publish failure──> pending (backoff + jitter)
   │                    ├──attempts exhausted──> dead
   └────lease expiry────┘
```

The important boundaries are intentional:

- no PostgreSQL transaction stays open during Kafka network I/O;
- an unacknowledged event remains recoverable after a crash;
- an acknowledged event can be published twice in the relay crash window;
- failed events are retained and become `dead` after retry exhaustion;
- strict or global ordering is not guaranteed in v0.1.

Read [delivery guarantees](docs/DELIVERY_GUARANTEES.md) and
[failure modes](docs/FAILURE_MODES.md) before adopting EmitLane in a critical path.

## CLI and operations

Build the CLI from source:

```bash
go build -o bin/emitlane ./cmd/emitlane
```

Available commands:

```text
emitlane migrate up
emitlane migrate down
emitlane doctor
emitlane run
emitlane dead list
emitlane dead retry <event-id>
emitlane version
```

Runtime endpoints:

- `GET /healthz` — process liveness;
- `GET /readyz` — PostgreSQL and Kafka readiness;
- `GET /metrics` — Prometheus metrics.

The minimum runtime configuration is:

```bash
export EMITLANE_DATABASE_URL='postgres://emitlane:emitlane@localhost:5432/emitlane?sslmode=disable'
export EMITLANE_KAFKA_BROKERS='localhost:19092'
```

Additional settings are documented in [.env.example](.env.example) and the
[quickstart](docs/QUICKSTART.md). Kafka topic auto-creation is disabled by
default and should only be enabled for local development.

## Development

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go test -tags=integration -count=1 -timeout=20m ./...
```

The integration suite requires Docker. Contributions should preserve the
project's core invariants; see [CONTRIBUTING.md](CONTRIBUTING.md) and
[SECURITY.md](SECURITY.md).

## Documentation

- [Architecture](docs/02-ARCHITECTURE.md)
- [Database model](docs/03-DATABASE.md)
- [Go API](docs/04-GO-API.md)
- [Delivery semantics](docs/05-DELIVERY-SEMANTICS.md)
- [Ordering model](docs/06-ORDERING.md)
- [Observability and operations](docs/07-OBSERVABILITY-OPERABILITY.md)
- [Security and deployment](docs/08-SECURITY-DEPLOYMENT.md)
- [Testing and benchmarks](docs/09-TESTING-BENCHMARKS.md)
- [Release process](docs/RELEASING.md)

## Project status

EmitLane is ready for its first `v0.1.0` release. Its core delivery and recovery
semantics are covered by real PostgreSQL and Kafka integration tests. As a
pre-1.0 project, APIs and operational defaults may still change during the
`v0.x` series. Release tags are created only by maintainers after the release
gate is explicitly enabled.

## License

EmitLane is available under the [Apache License 2.0](LICENSE).
