# Focused v1 validation profiles

Run these opt-in integration profiles only on an isolated test host, serialized
with other heavy jobs. They create their own containers and must not point at
application databases or broker clusters. Preserve the clean candidate commit,
actual configuration, supervisor/test exit status and all output.

## Retained Inbox rows and index churn

```sh
EMITLANE_INBOX_AGING_ROWS=3000000 \
EMITLANE_INBOX_AGING_OUTPUT=/absolute/new-results/inbox-aging.jsonl \
go test -p=1 -tags=integration -run '^TestInboxTransitionsOnAgedTable$' \
  -count=1 -v -timeout=30m ./internal/integration
```

The output parent directory must exist and the file must be new. Supported row
counts are100,000–4,000,000; use the recorded representative size for release
evidence rather than treating a small passing fixture as large-table proof.
The profile starts a dedicated PostgreSQL16 container with durable writes,
512 MiB shared-memory capacity and a3 GiB container limit. It retains the selected
number of processed rows, creates controlled stale entries from completed
inflight rows and captures the actual production Renew/MarkProcessed/MarkRetry/
MarkDead statements through pgx tracing. It executes and rolls back EXPLAIN
ANALYZE with buffers/WAL/settings before and after index cleanup.

The profile rejects a retained-table sequential scan or expiry-index scan for
an identity transition and requires a primary-key access path. It does not use a
flaky elapsed-time threshold or a separately maintained copy of the current SQL.
Autovacuum is disabled only inside this disposable fixture to preserve controlled
index debt. This is not a production tuning recommendation, replay of the old
24-hour database, concurrency proof, or sustained-throughput benchmark.

Run `TestInboxTransitionsRecheckFenceAfterConcurrentUpdate` with race detection
for the separate lock-wait/renewal/takeover and protected-effect rollback checks.

## Replicated Kafka faults

```sh
EMITLANE_KAFKA_CLUSTER_QUALIFICATION=1 \
go test -p=1 -tags=integration \
  -run '^TestKafkaClusterLeaderISRAndNetworkRecovery$' \
  -count=1 -v -timeout=15m ./internal/integration
```

The profile adds three Kafka4.3.1 JVM broker/controller nodes on a fresh private
Docker network, each with a768 MiB limit and128–256 MiB Java heap. Only loopback
client ports are published. The shared integration fixture also starts its
ordinary single broker, which is unused by the replicated test topic. Account
for all containers and the test runner in the aggregate host budget.

The topic has three partitions and replication factor3. The real Writer/Relay
and managed Inbox composition processes mixed ordered/unordered input, verifies
actual leader/ISR changes and exercises:

1. stopping a leader, observing a different leader and processing with two replicas;
2. requiring min.insync.replicas3, stopping a replica, observing an actual
   insufficient-ISR publisher error and preserving the unacknowledged Outbox work;
3. restoring replication, then disconnecting/reconnecting one owned broker from
   only the freshly created test network and processing through recovery.

After draining, a fresh Kafka reader audits all captured end offsets and exact
identities/keys/payloads/ordered continuity. PostgreSQL business, Outbox, Inbox
and protected-effect identities/states are reconciled, and full integrity is
checked. Kafka duplicates remain allowed. Container stops are controlled broker
stops, not host power loss. This bounded scenario does not replace multi-day
endurance, arbitrary partition topologies, TLS/SASL rotation, infrastructure
version matrices, PostgreSQL HA/PITR or an independent application pilot.

A profile is unqualified until its exact candidate run has terminal success and
reviewed evidence. Authored tests and successful compilation are not PASS results.
See [v1 acceptance](V1_RELEASE_CHECKLIST.md).
