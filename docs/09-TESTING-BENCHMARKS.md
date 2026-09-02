# Testing and benchmarks

## Quality principle

EmitLane is infrastructure. Unit tests are necessary but insufficient. The project must repeatedly demonstrate behavior with real PostgreSQL and real Kafka.

## CI baseline

Every pull request should eventually run:

```text
gofmt check
go vet
staticcheck
go test ./...
go test -race ./...
govulncheck
integration: PostgreSQL
integration: Kafka
```

## Integration environment

Use Testcontainers or an equivalent reproducible setup for:

```text
PostgreSQL
Kafka
EmitLane
Producer fixture
Consumer fixture
```

Do not mock the broker for the tests that claim delivery correctness.

## Required chaos/failure tests

### Scenario A — death after claim, before publish

Inject process death after `pending → inflight` commits.

Expected:

```text
lease expires
another worker reclaims
event is eventually delivered
```

### Scenario B — death after broker ACK, before delivered update

Expected:

```text
possible duplicate
event is not lost
Inbox prevents duplicate local DB effect
```

This is one of the most important tests in the whole project.

### Scenario C — Kafka unavailable for ten minutes

Expected:

- retry policy executes;
- no busy loop;
- database load remains bounded;
- events remain durable;
- metrics expose backlog age;
- delivery resumes when Kafka recovers.

### Scenario D — relay instance death

Run several relays. Kill one owning leases.

Expected:

- other instances recover expired work;
- no permanent stuck records.

### Scenario E — poison event

Force deterministic publish failure.

Expected:

```text
attempts increase
backoff applies
after max attempts → dead
operator can inspect reason
```

### Scenario F — PostgreSQL outage

Expected:

- no unsafe publish of unclaimed work;
- reconnect behavior is bounded;
- process becomes not-ready;
- recovery is automatic when DB returns.

### Scenario G — lost notification

Disable/miss `NOTIFY`.

Expected:

- polling fallback eventually discovers event;
- no correctness dependency on notification.

### Scenario H — many relay instances

Run N workers concurrently.

Expected:

- no silent double claim before lease semantics permit retry;
- no event loss;
- reasonable contention profile.

## Example demo application

`examples/ecommerce` should contain:

```text
Order Service
Payment Service
Email Service
```

Flow:

```text
POST /orders
      │
      ▼
Order DB + outbox
      │
      ▼
order.created
      │
      ▼
Payment Service + inbox
      │
      ▼
payment.completed
      │
      ▼
Email Service
```

A chaos/demo script should be able to:

```text
stop Kafka
create 1,000 orders
show durable backlog
restart Kafka
prove all events eventually arrive
```

## Benchmark command concept

```bash
emitlane benchmark \
  --events 1000000 \
  --payload-size 1024 \
  --workers 8
```

Measure:

- enqueue throughput;
- relay throughput;
- p50/p95/p99 delivery latency;
- CPU;
- PostgreSQL CPU/I/O;
- Kafka publish rate;
- retry overhead;
- DB queries per idle second;
- effect of batch size;
- effect of concurrency;
- effect of multiple relay instances.

## Important benchmark rule

Do **not** market arbitrary throughput numbers until a reproducible benchmark exists with:

- hardware description;
- PostgreSQL version/config;
- Kafka version/config;
- payload size;
- durability settings;
- source code/command;
- warm-up method.

## LISTEN/NOTIFY benchmark question

Measure idle systems with:

1. pure polling;
2. notification wake-up + low-frequency fallback polling.

Desired evidence:

- low idle database load;
- low event-start latency after commit.

