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
committed event uniquely in the outbox and reported zero loss. The v0.5.0
harness predates independent Kafka event-ID auditing, so this baseline is valid
release provenance but does not make a retrospective broker-audit claim.

## PostgreSQL claim evidence before v0.6 changes

On a 100,000-row due unordered backlog, the released combined pending-or-expired
claim query used a bitmap OR, scanned 100,000 heap rows, externally sorted
them, and took 79.034 ms to claim 16 rows (5,861 shared-buffer hits; 4.7 MiB
temporary disk sort). The captured plan is retained with local evidence. This
is evidence to investigate a pending-first / expired-recovery split; it is not
evidence for an index yet. Any schema change must include before/after plans,
write cost and migration coverage.

## Final audited candidate comparison

The retained raw artifact
`benchmarks/results/v0.6-candidate-58ca09d.json` was measured on clean commit
`58ca09ddc7ccc74ca60b27958159d0ee6e712d84`, after a PostgreSQL/Kafka reset on
the same host and durability settings as the v0.5.0 baseline. Its metadata is
compatible according to `emitlane-bench compare`: Go 1.27.0, macOS arm64,
PostgreSQL 16.15, Kafka 4.3.1, one broker, 1024-byte payloads, `acks=all`, and
the standard Relay configuration all match.

The three 5,000-event backlog-drain runs measured 1,248.48, 1,512.22, and
1,161.27 events/s (mean 1,307.32): -0.36% from the v0.5.0 mean of 1,311.99.
Mean p95 improved from 6,089.99 ms to 5,958.59 ms (-2.16%), and p99 from
6,150.10 ms to 6,026.51 ms (-2.01%). This local result is effectively neutral
for throughput and is not a performance improvement claim.

All 15,000 committed IDs were independently read from Kafka up to a captured
broker end-offset boundary: 15,000 unique records, zero loss, and zero
duplicates. Per-run audit duration was 28.08–29.50 ms and was excluded from
the measured throughput duration. Each raw run records the commit, clean-tree
state, seed `20260910`, and `valid_release_evidence=true`.

The same broker-boundary audit was exercised on clean commit
`e52f6187f7af38f1a9bbcfcc697d977ca88f3734` for 200 events across 20 ordered
streams and for a 100 ordered / 100 unordered mixed workload using two Relays.
Both retained raw artifacts (`v0.6-ordered-audit-e52f618.json` and
`v0.6-mixed-audit-e52f618.json`) report every ID once, zero loss and duplicates,
and zero broker sequence regressions. These are correctness audit smokes, not
throughput comparison samples.

One candidate run each at 1/2/4 Relays measured 1,814.36/1,535.34/1,710.82
events/s. The v0.5 control runs were noisy, and the candidate scale matrix has
only one run per point; it demonstrates bounded concurrent operation, not
linear scaling. A 20,000-event, four-Relay backlog drained at 1,593.43
events/s with zero lost/pending/inflight/dead events. The end-of-run snapshot
reported 2 goroutines, about 2.5 MiB heap allocated, and 0 acquired of 10
pool connections. A 2,000-event mixed run (1,000 unordered plus 100 ordered
streams) had zero ordering regressions and no non-delivered final state.

The continuous scheduler initially regressed because it made one small claim
per completion and repeatedly probed an empty ordered path. Completion signals
are now coalesced before a refill and an empty ordered probe is capped until
the next poll or notification. There is still no prefetched event queue: every
claim is bounded by currently free worker slots and immediately dispatched.

`go test -run '^$' -bench '^BenchmarkContinuousRefill$' -benchmem ./relay`
recorded 499,419 ns/op, 432,892 B/op and 4,201 allocs/op for 128 events on this
machine. Allocation pprof is dominated by per-event handling/message/header
construction and the in-memory benchmark double, not an unbounded scheduler
queue; no unsafe micro-optimization was applied.

The earlier exploratory observations remain useful for scheduler diagnosis but
are not used for the final release comparison above.
