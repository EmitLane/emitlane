# Quickstart

Aim: first successful event in under ten minutes. Requires Docker.

```bash
git clone https://github.com/EmitLane/emitlane.git
cd emitlane
docker compose -f docker-compose.example.yml up --build
```

This starts PostgreSQL, Kafka in KRaft mode, EmitLane migrate + relay, and the
ecommerce example.

If the default host ports are occupied, override them without editing the
compose file:

```bash
EMITLANE_POSTGRES_PORT=15432 \
EMITLANE_KAFKA_PORT=29092 \
EMITLANE_HTTP_PORT=18080 \
EMITLANE_ADMIN_PORT=18082 \
ECOMMERCE_HTTP_PORT=18081 \
docker compose -f docker-compose.example.yml up --build
```

The example commands below use the default ports; substitute your overrides
when configured. The Compose Admin API uses port 8082 and the development token
`emitlane-local-admin`. See the [Swagger UI guide](openapi/README.md).

Create an order (business row + outbox event in one transaction):

```bash
curl -sS -X POST http://localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"amount": 42}'
```

The relay publishes ordered `order.created` sequence 1 to `orders.events`. Mark
the order paid to publish sequence 2 for the same stream:

```bash
curl -sS -X POST http://localhost:8081/orders/<order-id>/paid
```

The example consumer uses Inbox and writes a payment row. Check:

```bash
# liveness / metrics on the relay
curl -sS http://localhost:8080/healthz
curl -sS http://localhost:8080/readyz
curl -sS http://localhost:8080/metrics | grep emitlane_

# payment written by the inbox consumer (use the order id from the POST response)
curl -sS http://localhost:8081/payments/<order-id>
```

## Manual local run

```bash
# 1. Start PostgreSQL and Kafka (compose without the Go services, or your own).
export EMITLANE_DATABASE_URL='postgres://emitlane:emitlane@localhost:5432/emitlane?sslmode=disable'
export EMITLANE_KAFKA_BROKERS='localhost:19092'
export EMITLANE_KAFKA_AUTO_CREATE_TOPICS='true' # local development only

go run ./cmd/emitlane migrate up
go run ./cmd/emitlane doctor
go run ./cmd/emitlane run
```

In another terminal, run `go run ./examples/ecommerce` with `DATABASE_URL` and
`KAFKA_BROKERS`, then POST `/orders` as above.

## Writer snippet

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
    Payload:     payload,
    OrderingKey: "order:" + order.ID,
    Sequence:    order.Version,
})
if err != nil {
    return err
}

return tx.Commit(ctx)
```

Payloads are stored in PostgreSQL as application data. Do not log them by
default; they may contain PII.

## PostgreSQL roles

Apply migrations as a separate DDL owner before granting runtime access.
Create the login roles through your deployment tooling first, grant `CONNECT`
on the application database, and adapt the role names below. These grants cover
EmitLane schema 4; grant access to your own business tables separately.

```sql
GRANT USAGE ON SCHEMA emitlane
TO emitlane_writer, emitlane_relay, emitlane_consumer,
   emitlane_reader, emitlane_operator;

-- Writer: caller-owned transactions, including optional ordered writes.
GRANT INSERT ON TABLE emitlane.outbox_events TO emitlane_writer;
GRANT SELECT, INSERT, UPDATE ON TABLE emitlane.ordering_streams TO emitlane_writer;

-- Standalone relay: startup/readiness read the migration history.
GRANT SELECT ON TABLE emitlane.schema_migrations TO emitlane_relay;
GRANT SELECT, UPDATE ON TABLE emitlane.outbox_events TO emitlane_relay;
GRANT SELECT, UPDATE ON TABLE emitlane.ordering_streams,
    emitlane.ordering_partitions TO emitlane_relay;
GRANT SELECT ON TABLE emitlane.runtime_control TO emitlane_relay;
GRANT SELECT, INSERT, UPDATE ON TABLE emitlane.relay_instances TO emitlane_relay;
GRANT DELETE ON TABLE emitlane.outbox_events TO emitlane_relay;
-- Omit DELETE only when EMITLANE_RETENTION_DELIVERED=0.

-- Consumer: legacy helpers and managed Inbox lifecycle.
GRANT INSERT, SELECT, UPDATE ON TABLE emitlane.inbox_events TO emitlane_consumer;
GRANT SELECT ON TABLE emitlane.schema_migrations TO emitlane_consumer;
-- Add writer grants if its handler enqueues a downstream Outbox event.

-- Read-only CLI inspection and integrity checks.
GRANT SELECT ON TABLE emitlane.schema_migrations, emitlane.outbox_events,
    emitlane.inbox_events, emitlane.ordering_streams, emitlane.ordering_partitions,
    emitlane.runtime_control, emitlane.relay_instances, emitlane.admin_audit_log
TO emitlane_reader, emitlane_operator;

-- Operator mutations: pause/resume, retry, replay and transactional audit.
GRANT INSERT, UPDATE ON TABLE emitlane.outbox_events TO emitlane_operator;
GRANT UPDATE ON TABLE emitlane.runtime_control, emitlane.inbox_events
TO emitlane_operator;
GRANT INSERT ON TABLE emitlane.admin_audit_log TO emitlane_operator;
```

The standalone Admin API shares the relay's pool and database identity. If you
enable it, grant that identity the operator's read and mutation privileges too,
for example with `GRANT emitlane_operator TO emitlane_relay` when role
inheritance is configured. A CLI operator can use its own database credentials.
There is no separate Admin API database URL or per-token read-only role.

`doctor` checks relay permissions and, when Admin API is enabled, operational
mutation permissions. Its ordering check also requires `INSERT` on
`ordering_streams`, although relay delivery itself only needs `SELECT/UPDATE`
there. For a diagnostic role intended to pass that check, additionally grant
`INSERT ON TABLE emitlane.ordering_streams`; do not confuse this diagnostic
requirement with the relay's delivery requirements.

`pg_notify` / `LISTEN` do not require superuser. If your installation revokes
default function execution privileges, restore execution of `pg_notify` and
EmitLane's schema helper/trigger functions for the appropriate runtime roles.

## Configuration

The complete [environment configuration reference](CONFIGURATION.md) covers
all standalone settings, defaults, validation bounds, TLS/SASL and Admin API.
The example application's `DATABASE_URL`, `KAFKA_BROKERS`, `HTTP_ADDR` and
`ORDERS_TOPIC` settings are described there too. Never commit real credentials.

## Tests

Unit tests need no Docker:

```bash
go test -count=1 ./...
go test -race -count=1 ./...
```

Integration and failure-injection tests use Testcontainers (Docker required):

```bash
go test -tags=integration -count=1 -timeout=20m ./...
```
