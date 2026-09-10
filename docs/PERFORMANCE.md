# Performance and capacity

EmitLane is at-least-once infrastructure. Performance work must retain durable
Outbox transitions, bounded Kafka publish work, ordered epoch fencing, and
Inbox/offset commit ordering. A faster run that loses an event or advances an
offset before durable processing is a failed run.

## v0.6 methodology

Compare a clean candidate commit with the exact released `v0.5.0` commit on
the same host, PostgreSQL, Kafka, payload size, broker count and durability
settings. Run one warm-up and at least three measured runs. Keep each raw JSON
result and report all runs, not only the best. Differences of a few percent are
noise on a local single-broker setup.

The standard Relay configuration for this comparison is batch size 200,
concurrency 16, a 3 second lease and a 2 second bounded publish timeout. Each
run must report committed, unique delivered, lost, final pending, inflight and
dead counts. The benchmark command must fail when loss or an ordering
regression is observed.

Capacity is bounded by active publishes (`Concurrency`), the per-claim request
limit (`BatchSize`), the PostgreSQL pool, and Kafka broker capacity. Relay
instances should refill a newly-free worker slot without accumulating an
unbounded in-memory backlog. A single ordered stream remains serial by design.

## Released v0.5.0 baseline

`benchmarks/results/v0.5.0-local-baseline.json` is release evidence from tag
`v0.5.0` at `9da5040ba70290d40b15b341e87ebbfd0a221cc6`, not from this branch.
It used Go 1.27.0 on macOS 26.5 arm64 (10 CPUs, 24 GiB), PostgreSQL 16.15 with
`fsync`, `full_page_writes` and `synchronous_commit` enabled, and one Kafka
4.3.1 broker with `acks=all`; payloads were 1024 bytes.

Three 5,000-event backlog-drain runs measured 1,511.98, 1,314.04 and 1,109.94
events/s (mean 1,311.99). The 1/2/4 Relay scaling runs have high local variance
(including one 30-second two-Relay outlier), so they establish a comparison
record rather than a linear-scaling claim. All recorded runs delivered every
committed event uniquely in the outbox and reported zero loss.

## PostgreSQL claim evidence before v0.6 changes

On a 100,000-row due unordered backlog, the released combined pending-or-expired
claim query used a bitmap OR, scanned 100,000 heap rows, externally sorted
them, and took 79.034 ms to claim 16 rows (5,861 shared-buffer hits; 4.7 MiB
temporary disk sort). The captured plan is retained with local evidence. This
is evidence to investigate a pending-first / expired-recovery split; it is not
evidence for an index yet. Any schema change must include before/after plans,
write cost and migration coverage.
