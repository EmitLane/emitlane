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

## v0.2 reproducible harness

The harness lives at `benchmarks/cmd/emitlane-bench` and runs against real
PostgreSQL and, for delivery scenarios, real Kafka. It supports enqueue overhead,
steady state, backlog drain, horizontal scaling, idle overhead, outage recovery,
and the broker-ACK crash window. Every run emits JSON with runtime,
configuration, duration, and throughput metadata. CI only performs a small
functional smoke run; it does not enforce noisy throughput thresholds.

The integration suite additionally verifies v1→v2 data preservation, atomic
pause gating, in-flight completion, presence classification, redaction, keyset
pagination, audited retry, replay provenance/system headers, and atomic rejection
of replay selections above the 1000-event cap.

## v0.3 ordered reliability coverage

The real PostgreSQL/Kafka suite covers transaction commit inversion, durable
future sequences, concurrent duplicate rejection, independent streams,
retry/dead blocking, the ordered ACK crash window, ownership races, multi-Relay
delivery, graceful rebalance, crash takeover and handoff, stale epoch rejection,
gap inspection, explicit stream start, ordered replay, retention, pause/lease
renewal, and actual Kafka partition affinity. A seeded randomized scenario mixes
commit order, transient publish failures, Relay membership changes, and
pause/resume while asserting no committed IDs are lost and no stream regresses.

The benchmark harness adds `ordered-many-streams`, `ordered-hot-stream`, and
`unordered-regression`. Ordered results include throughput, latency percentiles,
virtual-partition distribution, and PostgreSQL transaction count. Compare
unordered regression output with a v0.2.0 run under identical environment
metadata; the harness does not invent a baseline or performance claim.
