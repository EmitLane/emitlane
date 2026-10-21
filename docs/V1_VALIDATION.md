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

## Managed qualification group capacity

The repeated managed-consumer qualification adds a second full worker pool
during each rebalance fault. Its assertion requires an actual nonempty peer
assignment within one minute. Provision at least twice as many topic partitions
as workers in one pool. With eight partitions and eight incumbent plus eight
peer members, cooperative sticky assignment can legally leave every new member
idle while the incumbents retain all eight partitions. A successful group join
does not guarantee new work for a particular member.

`EMITLANE_QUALIFICATION_PARTITIONS` selects8–16 partitions (default8). The
positive-assignment profile rejects insufficient capacity before creating its
fixture. Set it to16 for **both** four-worker and eight-worker capacity trials
to compare the same topic topology. The selected partition count is preserved
in the start record and independently checked by the full Kafka audit. Do not
compare a sixteen-partition result directly with an earlier eight-partition
trial as if only worker concurrency had changed.

`TestQualificationRebalanceCapacityAndOversubscribedHandoff` separately checks
eight plus eight real group members with eight and sixteen partitions. It
requires complete unique broker assignments, verifies the expected idle-member
count, processes protected effects before and after all incumbent members leave,
and requires final committed offsets. The eight-partition case accepts legitimate
idle members; it does not certify the repeated soak's positive-peer assertion.
The sixteen-partition case still requires an actual nonempty peer assignment.
Neither profile increases the one-minute assertion budget. Preserve failures
from the previous fixture as failures, including incomplete terminal audits.

## Standalone Relay upgrade and rollback

Build both the v0.9.1 commit and the candidate with the patched release
compiler. Embed the full source commit through `main.commit` in each CLI.
Then run the opt-in test with absolute binary paths and a new output directory:

```sh
EMITLANE_PREVIOUS_RELAY_BINARY=/absolute/emitlane-v0.9.1 \
EMITLANE_CURRENT_RELAY_BINARY=/absolute/emitlane-candidate \
EMITLANE_ROLLING_OUTPUT=/absolute/new-results/rolling \
go test -p=1 -tags=integration \
  -run '^TestRelayRollingUpgradeAndRollbackFromV091$' \
  -count=1 -v -timeout=10m ./internal/integration
```

The test starts real standalone CLI processes against the disposable durable
PostgreSQL/Kafka fixture. It requires live presence and simultaneous valid
ordered-partition ownership by both versions, then processes mixed ordered and
unordered events through old-only, mixed-version, candidate-only and rollback
to old-only phases. Each phase drains both protected effects and Kafka offsets.
The final independent Kafka and database identity audits and full integrity
report remain mandatory. Child logs and structured evidence are preserved.

This qualifies the selected v0.9.1 source rebuilt with the patched compiler,
not the byte-identical published binary, every historical version, schema
migration rollback, or a real application rollout. There is no schema change
in this candidate. Do not treat this fixture as the independent pilot.

### Multi-relay fixture evidence

The standalone soak harness requests and verifies PostgreSQL `fsync`,
`synchronous_commit`, and `full_page_writes` as `on`, a minimum 512 MiB shared
memory segment, and a loopback PostgreSQL endpoint. It records actual fixture
settings, independent Kafka audit results and the complete integrity report.
The broker pause and stop/restart are distinct faults. Release-profile verdicts
require successful completion of all six fault categories, including graceful
relay restart, SIGKILL takeover, cluster pause and membership change. Fault
records retain ownership snapshots; relay process logs retain signal outcomes.
The independent Kafka scan checks payload, key, ordering headers and partition
stability for every record, including duplicates. These checks strengthen the
test harness only; adding them does not certify a release until the profile has
actually run and its evidence has been reviewed.

### PostgreSQL failure evidence

Qualification retains the existing 10-second PostgreSQL stop grace, two-minute
container-start context and 60-second SQL readiness budget. Restart traces record
actual container state, exit status, restart count and SQL readiness timestamps.
A failed readiness check remains a failed check.

Before fixture cleanup, failure capture saves the available PostgreSQL container
logs and inspected state independently of SQL. A failed logical dump keeps its
`.partial` file and error. The harness then stops only its PostgreSQL fixture,
checks that it stays stopped and copies its actual `PGDATA`. The gzip CRC and all
tar member data are read before the raw archive is published. This is a stopped
byte copy that may require crash recovery; it is not a successful logical dump
or proof of restoration.

`EMITLANE_QUALIFICATION_CAPTURE_CHECK=1` enables the separate short capture
regression. It checks a complete logical dump while PostgreSQL is available,
then captures an unavailable fixture and restores its raw archive into a new
container with no network. Two committed marker records must survive. This
small fixture does not qualify a failed endurance run, production restore/PITR
or Kafka offset reconciliation. Corrupt gzip trailers, missing PostgreSQL control
files, unsafe member paths and incomplete/running container states are rejected.

### ACK crash-window selection

The ACK crash helper stops inside the real Relay hook after Kafka acknowledges
a committed event and before the database marks it delivered. The evidence
records both the initial scheduling probe and the exact event that reached the
ACK window. The parent verifies that selected event belongs to the expected
ledger and remains inflight under the helper's ownership before resuming work.
The claim-crash helper still requires its initial probe.

A transient publish failure can reschedule the initial probe behind other ready
events. Requiring that one probe to reach ACK can exhaust the helper's unchanged
25-second budget while other events are being delivered. Selecting the actual
ACKed ledger event avoids this scheduling assumption without changing Relay
retry policy or any qualification timeout. The separately enabled
`EMITLANE_QUALIFICATION_ACK_CHECK=1` regression injects one probe failure, checks
an actual Kafka ACK and process kill on another event, then verifies recovery
and the expected duplicate. Failed earlier runs remain failed and unaudited.

### Exact Inbox snapshot counts

The periodic snapshot keeps one shared 10-second deadline for Inbox counts,
database statistics, outbox counts and Kafka offsets. Its five Inbox counts run
in one MVCC statement with explicit literal status predicates, making the
released partial indexes eligible. A status condition inside `FILTER` alone
requires fetching statuses from the entire retained consumer history. The
processed subquery reads in `(processed_at, event_id)` order to use the
released retention index when a consumer owns most of the table, avoiding a
parallel scan of the wider heap in the tested PostgreSQL configuration. No
released index, planner setting, SDK operation or business transaction changes.

Counts remain exact, including the full processed history. They are not
estimates or cached progress counters, and counting retained history still has
work proportional to that history. Access-path checks on a short retained-row
fixture do not prove a future endurance deadline under concurrent load.

```sh
EMITLANE_QUALIFICATION_SNAPSHOT_CHECK=1 \
EMITLANE_QUALIFICATION_DURATION=2m \
go test -p=1 -tags=integration \
  -run '^TestQualification(InboxSnapshot.*|SnapshotFailureEvidence)$' \
  -count=1 -v -timeout=3m ./internal/integration
```

The opt-in retained-history regression checks exact consumer isolation and the
actual query plan against 200,000 processed rows for each of two consumers,
then rechecks after one consumer owns the entire retained history,
plus a selective consumer with 1,000 rows. It checks the non-processed partial
indexes and the processed retention index for both large and selective
consumers. These access-path assertions detect a return to the wider heap scan;
they do not establish a universal latency bound.
Separate cases compare all lifecycle counts with the original aggregate and
verify that a blocked query obeys its caller deadline.

A snapshot error remains fatal. Before teardown, `snapshot-failure.json` records
its stage, elapsed time, shared deadline and query shape. Existing failure capture
also records the Inbox snapshot's non-executing `EXPLAIN`, separately from the
recovery audit plan, within the existing failure-capture budget. It does not
retry the failed snapshot. Final identity, payload/key/order, protected-effect,
operator and full-integrity audits and their budgets remain unchanged.
