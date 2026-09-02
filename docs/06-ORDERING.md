# Ordering design

## v0.1 position

Ordering is deliberately **not a v0.1 feature** beyond whatever ordering Kafka naturally provides for a chosen key/partition after events reach the publisher.

The relay must not advertise global database commit ordering.

## Why `created_at` or generated ID is insufficient

Example:

```text
TX A allocates event 15
TX B allocates event 16
TX B commits first
relay sees 16
TX A commits later
```

Database-generated identifiers do not automatically represent domain commit order.

## Intended later modes

### Default unordered mode

Goal: maximum throughput and simple worker sharing.

No promise that two events for the same domain aggregate are delivered in business sequence unless the application and broker setup provide one.

### Ordered aggregate mode

Application supplies:

```go
OrderingKey: "order:123"
Sequence:    15
```

Recommended sequence source:

- domain aggregate version;
- explicitly managed monotonic sequence owned by the business transaction.

Example:

```text
order.created  sequence=1
order.paid     sequence=2
order.shipped  sequence=3
```

## Virtual partition concept

Proposed later design:

```text
hash(OrderingKey) % 64 → virtual partition
```

Virtual partition count is independent from relay instance count.

Example:

```text
64 virtual partitions
4 relay instances

A: 0-15
B: 16-31
C: 32-47
D: 48-63
```

On worker loss, partitions rebalance to survivors.

## Ordering correctness questions to solve before implementation

- how are virtual partitions leased?
- how does rebalance avoid concurrent owners?
- what happens when sequence `12` exists but `11` never arrives?
- should gaps block forever, timeout, or become operator-visible?
- can dead event `11` allow event `12` to proceed?
- does replay preserve original sequence semantics?
- how does retention interact with ordering state?
- how are multiple events for the same aggregate in one DB transaction sequenced?

No strict-ordering code should be merged until these questions have a dedicated design document and tests.

