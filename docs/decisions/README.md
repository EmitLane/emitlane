# Technical decisions

## Recovery probes and dated polling snapshots

Soak recovery polls for unfinished Outbox work with materialized one-row
status probes, ordered by the existing `(status, created_at DESC, id DESC)`
index. A bare `EXISTS` may choose a sequential scan based on pre-drain
statistics, so the limited ordered subqueries preserve a cheap index path
when only sparse work remains. This is a query shape, not a planner hint or
a latency guarantee: retained dead index entries and MVCC visibility can still
require extra reads. The diagnostic compares both shapes after a rolled-back
synthetic drain without refreshing planner statistics, including one pending,
one inflight, one dead, no active work, and a delivered-only stream gap.
While pending, inflight or dead rows exist, it does not enumerate stream state.
When active rows disappear, a missing-head/future-row gap probe still prevents
premature quiescence, including a gap containing only delivered future rows.
A negative probe is only a candidate: the original exact `queueState` query
must then confirm all five zero counts before recovery can succeed. The final
result uses the same exact query independently, and Kafka, PostgreSQL identity,
counter and full-integrity audits keep their original acceptance and deadlines.

Automatic Relay statistics use an optional dated snapshot capability. The
PostgreSQL Store reuses exact captured queue/stream counts for a 30-second
window per Store, coalescing concurrent refreshes. Both aggregations use one
read-only repeatable-read transaction, so a concurrent producer commit cannot
mix queue counts from one snapshot with stream counts from another. This adds no database state,
trigger or writes to business transactions. Pause, presence and partition
ownership are still read on every poll. Cache hits retain the original capture
time in `emitlane_stats_last_success_timestamp_seconds`; failures preserve the
previous gauges and timestamp. The configured polling interval, query contexts,
leases and audit/recovery timeouts do not change. Long refresh time remains
visible in observation age and is not hidden by restarting a clock.

Explicit `StatsSnapshot`, `StatsSnapshotWithPresence` and Admin/CLI statistics
remain uncached operator reads. `CachedStatsSnapshotWithPresence` exposes both
the observation and its source time for polling callers. None of these
statistics determines delivery completion. The aggregate ordering query probes
future sequences only for missing heads; public list/detail fields such as the
lowest future sequence remain unchanged. Snapshot reuse is per process/Store,
not a cluster-wide cache or a guarantee of synchronized replicas.

Cache contents are disposable. Restart causes a new full observation; it cannot
lose events, change leases or advance a cursor. Read errors are errors, not
observations of an empty queue. The recovery probe and exact reconciliation
use the existing recovery context, so a failing read cannot extend its budget.
The revised paths still require separately authorized server validation.

## Pending unordered claims have a dedicated partial index in schema 5

The pending branch of `Store.Claim` already filters `status = 'pending'` and
`ordering_key IS NULL`, orders by `(available_at, created_at, id)` and applies
`FOR UPDATE SKIP LOCKED` with a batch limit. On the mixed backlog diagnostic,
the general pending index discarded 274,215 ordered rows before returning
eight unordered events. Migration `000005_unordered_claim_index` adds
`outbox_unordered_pending_idx` with the same key order and the exact unordered
pending predicate. The released general pending index remains available for
other queries. The expired-inflight branch is unchanged.

Published migrations 1–4 remain byte-for-byte unchanged. This is schema 5;
startup, doctor and full integrity checks require that version and its new
index. Audit deadlines, zero-violation acceptance and delivery state transitions
are unchanged. Earlier schema-4 qualification is historical evidence, not a
validation of this index or the new candidate's plans.

The existing migrator creates the index inside its advisory-locked transaction;
it does not support `CREATE INDEX CONCURRENTLY`. Installation therefore needs
a maintenance window for the table's writes, with the existing migration
deadline. Failure or cancellation rolls back the index and migration record.
Migration 5 down removes only this index and its history record, preserving
Outbox, Inbox, stream cursors and leases. Stop candidate processes and return
the database to schema 4 before starting v0.9.1 binaries, which reject schema 5.
Do not continue down through the state-removing migrations as a binary rollback.
The exact upgrade and rollback artifacts still require qualification.

The index changes the access path, not claim ownership. A crash before claim
commit rolls back; after commit the event remains leased and recoverable by
expiry. Concurrent claims retain row locks and lease checks. Kafka duplicates
remain possible around ACK/DB failure; no new deletion or exactly-once claim is
introduced. Existing metrics and integrity findings expose unfinished work.

## Inbox identity transitions use a locked primary-key lookup

Renew, MarkProcessed, MarkRetry and MarkDead first materialize and lock the Inbox
row by `(consumer, event_id)`, then check its status and lease token and perform
the transition in the same SQL statement/transaction. The UPDATE joins the same
logical key and checks the token again. No physical `ctid` is persisted or used
as a business identity. A missing or stale lease still returns `ErrLeaseLost`.
MarkProcessed remains inside the caller-owned business transaction.

The previous direct UPDATE also included `status='inflight'` in the target scan.
PostgreSQL could choose `inbox_expired_lease_idx` to find a single identity and
scan many stale entries. A controlled PostgreSQL16 experiment with3million
retained rows and200,000 completed inflight rows observed1,573 shared buffers
for that access path; cleaning the index reduced the cost. This identifies an
access-path risk, not the unique cause of every latency observed in the24h run.
The fixture's disabled autovacuum is an explicit experimental variable, not a
production recommendation.

The materialization boundary keeps the lifecycle predicate out of the target
identity lookup. The partial expiry index remains available for maintenance
queries. Published migrations, indexes, retry/lease timing, durable-write policy,
Kafka acknowledgements and delivery guarantees are unchanged.

Crash/concurrency review: death before the transaction commits rolls back the
transition and business effects; death after commit can still cause Kafka
redelivery. The row lock serializes renewal/takeover/completion, the winning token
fences stale attempts, and released locks do not strand leases. Query errors and
lease loss remain observable. Existing lease expiry and consumer recovery apply.
Tests observe actual lock waits, require valid transitions after same-token renewal,
reject stale transitions after takeover and verify protected-effect rollback.
