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
ECOMMERCE_HTTP_PORT=18081 \
docker compose -f docker-compose.example.yml up --build
```

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

Do not use a superuser at runtime. Suggested grants (adjust role names):

```sql
GRANT USAGE ON SCHEMA emitlane TO emitlane_writer, emitlane_relay, emitlane_consumer;

GRANT INSERT ON TABLE emitlane.outbox_events TO emitlane_writer;
GRANT SELECT, INSERT, UPDATE ON TABLE emitlane.ordering_streams TO emitlane_writer;

GRANT SELECT, UPDATE ON TABLE emitlane.outbox_events TO emitlane_relay;
GRANT SELECT, UPDATE ON TABLE emitlane.ordering_streams, emitlane.ordering_partitions TO emitlane_relay;
GRANT SELECT ON TABLE emitlane.runtime_control TO emitlane_relay;
GRANT SELECT, INSERT, UPDATE ON TABLE emitlane.relay_instances TO emitlane_relay;
GRANT DELETE ON TABLE emitlane.outbox_events TO emitlane_relay; -- delivered cleanup only

GRANT INSERT, SELECT ON TABLE emitlane.inbox_events TO emitlane_consumer;
```

`pg_notify` / `LISTEN` do not require superuser.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `EMITLANE_DATABASE_URL` | required | PostgreSQL URL |
| `EMITLANE_KAFKA_BROKERS` | required | Comma-separated broker list |
| `EMITLANE_KAFKA_CLIENT_ID` | `emitlane` | Kafka client id |
| `EMITLANE_KAFKA_AUTO_CREATE_TOPICS` | `false` | Allow broker auto-create; intended for development |
| `EMITLANE_HTTP_ADDR` | `:8080` | Health/metrics listen address |
| `EMITLANE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `EMITLANE_INSTANCE_ID` | hostname + random | Unique relay instance |
| `EMITLANE_RELAY_BATCH_SIZE` | `100` | Claim batch size |
| `EMITLANE_RELAY_CONCURRENCY` | `4` | In-flight publishes |
| `EMITLANE_RELAY_POLL_INTERVAL` | `5s` | Polling fallback |
| `EMITLANE_RELAY_LEASE_DURATION` | `30s` | Must exceed publish timeout |
| `EMITLANE_ORDERING_REBALANCE_INTERVAL` | `2s` | Virtual-partition ownership refresh |
| `EMITLANE_ORDERING_LEASE_DURATION` | `30s` | Must exceed publish timeout + ordering safety margin |
| `EMITLANE_ORDERING_SAFETY_MARGIN` | `1s` | Added to the stale-publish handoff bound |
| `EMITLANE_RETRY_MAX_ATTEMPTS` | `10` | Then `dead` |
| `EMITLANE_RETRY_BASE_DELAY` | `1s` | Exponential backoff base |
| `EMITLANE_RETRY_MAX_DELAY` | `30m` | Backoff cap |
| `EMITLANE_PUBLISH_TIMEOUT` | `10s` | Must be `<` lease duration |
| `EMITLANE_SHUTDOWN_TIMEOUT` | `15s` | Drain on SIGTERM |
| `EMITLANE_STATS_INTERVAL` | `5s` | Queue gauge refresh |
| `EMITLANE_RETENTION_DELIVERED` | `168h` | `0` disables cleanup |
| `EMITLANE_RETENTION_INTERVAL` | `1m` | Cleanup ticker |
| `EMITLANE_RETENTION_BATCH` | `1000` | Cleanup batch size |
| `EMITLANE_DB_MAX_CONNS` | `10` | pgx pool |
| `EMITLANE_DB_MIN_CONNS` | `2` | pgx pool |
| `EMITLANE_DB_MAX_CONN_LIFETIME` | `1h` | pgx pool; must be greater than zero |

Never commit credentials. Outbox payloads are stored in PostgreSQL as application data.

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
