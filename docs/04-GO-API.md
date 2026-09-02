# Go API design

The v0.1 API is deliberately small and PostgreSQL-specific. It is pre-1.0 and
may evolve, but transaction ownership and delivery semantics are explicit.

## Outbox event

```go
type Event struct {
    ID string

    Destination string
    Type        string
    Key         []byte

    Payload     []byte
    ContentType string
    Headers     map[string]string

    SchemaVersion int
    CorrelationID string
    CausationID   string
    AvailableAt   time.Time
}
```

`Destination` and `Type` are required. Payload is opaque bytes and may be
empty. An empty ID is replaced with UUIDv7 before insertion. An empty
`AvailableAt` uses PostgreSQL `NOW()`. Strict ordering fields are intentionally
absent from v0.1.

## Writer

```go
func NewWriter(opts ...outbox.Option) *outbox.Writer

func (w *outbox.Writer) Enqueue(
    ctx context.Context,
    tx pgx.Tx,
    event outbox.Event,
) (string, error)
```

The application owns `pgx.Tx`. EmitLane neither starts nor commits it:

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

Using concrete `pgx.Tx` is intentional: a one-method database interface would
also accept `pgxpool.Pool` and make accidental autocommit possible.

Available writer options:

```go
outbox.WithMetrics(metrics)
outbox.WithoutNotify() // testing the mandatory polling fallback
```

## JSON helper

```go
func JSON(v any) ([]byte, error)
```

It uses the standard JSON encoder and returns serialization errors. JSON is a
convenience only; storage remains `BYTEA`.

## Inbox

```go
func Process(
    ctx context.Context,
    tx pgx.Tx,
    consumer string,
    eventID string,
    fn func(context.Context, pgx.Tx) error,
) error

func ProcessStrict(
    ctx context.Context,
    tx pgx.Tx,
    consumer string,
    eventID string,
    fn func(context.Context, pgx.Tx) error,
) error
```

`Process` treats an existing `(consumer, event_id)` marker as a successful
no-op. `ProcessStrict` returns `inbox.ErrAlreadyProcessed`. Both use a nested
pgx transaction/savepoint so a callback error rolls back the marker and local
database writes even if the caller later mishandles the outer transaction.

Inbox protects only effects committed in the same PostgreSQL transaction.
External APIs still require their own idempotency key, normally derived from
the stable event ID.

## Broker port and Kafka adapter

```go
type Publisher interface {
    Publish(ctx context.Context, message broker.Message) error
    Close() error
}
```

The relay depends only on this small interface. `broker/kafka` implements it
with franz-go; Kafka types do not leak into relay state.

## Relay

```go
func relay.DefaultConfig() relay.Config

func relay.New(
    cfg relay.Config,
    store relay.Store,
    publisher broker.Publisher,
    opts ...relay.Option,
) (*relay.Relay, error)

func (r *relay.Relay) Run(ctx context.Context) error
```

`relay.Config` controls batch size, concurrency, polling, leases, attempt
budget, retry delays, publish/shutdown timeouts, statistics, and delivered-row
retention. The standalone binary is the recommended production composition
root. Embedded use remains possible for applications that can own the
lifecycle correctly.

## Trace propagation

Writer captures W3C `traceparent`/`tracestate` from `context.Context`. Relay
restores that context for `emitlane.publish` and sends the active span context
as Kafka headers. Consumers can call `telemetry.ExtractTrace` before starting
their processing span.

## Compatibility

The module path is `github.com/emitlane/emitlane`. v0.1 is pre-1.0, so API
changes remain possible; exported surface is kept intentionally small.

## v0.2 relay capabilities

The required `relay.Store` interface is unchanged. v0.2 discovers optional
capabilities with type assertions:

- `relay.PauseState` for durable control checks;
- `relay.PresenceStore` for register/heartbeat/stopped visibility;
- `relay.StatsWithPresence` for metrics using the configured stale threshold.

This preserves source compatibility for custom v0.1 stores. The PostgreSQL store
implements all capabilities and atomically gates its `Claim` SQL on the durable
pause row. `relay.WithPresenceInfo` can attach hostname/version metadata without
affecting delivery behavior.
