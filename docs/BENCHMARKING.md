# Benchmarking

The v0.2 harness uses the real PostgreSQL writer/store and real Kafka publisher.
It does not print invented throughput claims.

Start the example dependencies and migrate the schema, then run:

```bash
export EMITLANE_DATABASE_URL='postgres://emitlane:emitlane@localhost:5432/emitlane?sslmode=disable'
export EMITLANE_KAFKA_BROKERS='localhost:19092'

go run ./benchmarks/cmd/emitlane-bench \
  --scenario backlog-drain --events 10000 --output result.json
```

Supported scenarios are `enqueue-overhead`, `steady-state`, `backlog-drain`,
`horizontal-scaling`, `idle-overhead`, `failure-recovery`, and `ack-crash`.
Use `--relays` for scaling and `--duration` for idle/outage windows.
`failure-recovery` performs two relay crash/restart cycles after claim commit,
waits for lease expiry, then verifies every committed event ID at Kafka.
`ack-crash` injects the documented broker-ACK-before-database-ACK window and
reports duplicates separately from lost IDs.

JSON includes timestamp, Go/OS/architecture, redacted connection metadata,
event and relay counts, duration, throughput, latency where measured, and
scenario-specific recovery information. Record PostgreSQL/Kafka versions,
hardware, durability settings, payload size, warm-up method, and competing load
alongside results before comparing runs.

`.github/workflows/benchmark-smoke.yml` runs a small real dependency smoke test
on relevant pull requests and by manual dispatch. It validates the harness but
does not impose a flaky performance threshold.
