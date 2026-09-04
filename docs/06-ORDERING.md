# Ordered delivery

EmitLane v0.3 provides opt-in ordered delivery for a stream identified by
`(destination, ordering_key)`. The application owns the positive, monotonically
increasing domain sequence; EmitLane never derives order from timestamps, UUIDs,
transaction start order, or database identities.

```go
_, err := writer.Enqueue(ctx, tx, outbox.Event{
    Destination: "orders.events",
    Type:        "order.paid",
    Payload:     payload,
    OrderingKey: "order:123",
    Sequence:    2,
})
```

The first event defaults the stream start to sequence 1. To adopt an aggregate
whose next event is 50, set `OrderingStartSequence: 50` on the first write.
The message key is automatically set to the ordering key; an explicitly supplied
key must match it exactly.

PostgreSQL stores a durable `next_sequence`. A future sequence remains pending
when an earlier sequence is missing, retrying, or dead. There is no automatic
skip. Independent streams continue normally. Delivered-row retention does not
remove stream progress.

Exactly 64 virtual partitions map streams to Relay instances using stable FNV-1a
hashing and rendezvous ownership. PostgreSQL leases and epochs are authoritative.
Every ordered state transition validates event ownership, partition ownership,
epoch, lease, and sequence. A bounded publish window plus a persisted handoff
barrier prevents a replacement owner from publishing until an old owner's
network-send window has elapsed.

The Kafka client uses `acks=all`, disables producer idempotence, and performs no
client record retries. This lets the Relay deadline cancel an unresolved
request within the bounded handoff window. Kafka may already have accepted a
request whose acknowledgement was lost, so PostgreSQL retry can still create
the documented at-least-once duplicate.

The guarantee remains at least once. A crash after Kafka ACK but before the
atomic delivered/stream-advance transaction can produce duplicate N. EmitLane
keeps N+1 blocked, so valid output can be `N, N, N+1`, but not `N, N+1, N` under
the documented bounded-client assumptions. Consumers still require Inbox or
another idempotency mechanism.

See [Ordered delivery](ORDERED_DELIVERY.md) for the full timing invariant,
rolling-upgrade protocol, replay behavior, operational states, limitations, and
failure analysis.
