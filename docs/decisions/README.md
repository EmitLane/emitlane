# Technical decisions

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
