# Independent v1 application pilot

The pilot is planned. Its result remains open in the
[v1 acceptance record](V1_RELEASE_CHECKLIST.md). Use a separately owned
application that exercises its real business transactions and handlers outside
the synthetic qualification harness.

## 1. Agree on the scope and pass criteria

Run in dedicated staging within the [initial v1 support matrix](V1_CONTRACT.md#initial-support-matrix).
Use the candidate binaries or images with exact SHA and digests, PostgreSQL 16
with durable writes and schema 5, and Kafka 4.3.1 with the production-intended
TLS and authentication mode. Isolate database, topics, groups, secrets and
downstream endpoints from production. Restrict fault controls to pilot-owned
resources.

The application owner and operations owner fill in and agree on these inputs
before a run. Record the criteria with the configuration so an observation can
be compared with a pre-agreed limit.

| Input | Record before starting |
| --- | --- |
| Ownership | Application and repository/version, owner, operator, evidence reviewer and escalation contact. |
| Artifact and environment | Candidate SHA, binary/image digests, app version, PostgreSQL and Kafka versions and topology, TLS/auth mode, partitions, worker and pool sizes, resources and deployment manifests. |
| Business coverage | Transaction path, stable identity resolver and Inbox consumer name, expected local effects, external-effect idempotency, ordered streams and poison-work procedure. |
| Workload | Agreed normal and peak admitted rates, payload and key distribution, retained-row volume and observation period covering a representative retention/vacuum cycle. |
| Pass limits | Allowed p95/p99 latency, queue age, per-partition lag and catch-up time for each load phase. Set RPO and RTO for the separate restore drill and an alert/response budget for each fault. |
| Recovery and compatibility | Kafka retention and Inbox marker horizons, backup tool, schema-4-to-5 maintenance upgrade and index-only rollback to schema 4 before v0.9.1 restart, and the operator allowed to stop or roll back staging. |

Use an independent application ledger to record committed business and Outbox
IDs, expected key/payload digests and expected protected effects. Capture Kafka
group and partition checkpoints before the first phase. Use the same stable ID
for redelivery checks. Application effects that leave PostgreSQL must use
idempotency keys or staging substitutes with observable outcomes.

## 2. Execute the pilot in phases

| Phase | Activity and evidence |
| --- | --- |
| Baseline and rollout | Capture health, offsets and application ledger before changing binaries. Qualify the schema-4-to-5 maintenance installation and verify schema 5 and readiness before resuming the candidate. Compare effects and cursor progress through the transition. Qualify the index-only schema-5-to-4 rollback before restarting v0.9.1 with the selected artifacts. Keep the application deployment steps and their results separate. |
| Normal and peak load | Run the agreed application workload and include the planned retention/vacuum observation. Measure actual committed input, end-to-end latency, Outbox age, and Kafka lag per partition. Capture pool waits, query plans and costs for representative Inbox lifecycle transitions, table/index growth, memory, goroutines and OOM counters. |
| Faults and operator response | In staging, exercise a Relay restart, consumer restart and rebalance, PostgreSQL interruption, and restart of the dedicated Kafka broker. Verify retry and lease recovery, duplicate handling and preservation of ordering. Drive one poison input to the documented blocked/dead state, then perform audited retry and prove progress. Record alert arrival, operator response and catch-up time for each fault. |
| Restore and resumption | Perform the separate [PostgreSQL PITR and Kafka offset drill](V1_PITR_DRILL.md) with the application's backup tooling and representative state. Link its exact cut proof, offset plan/read-back, resumption reconciliation and measured RPO/RTO. |
| Drain and audit | Stop input, let work drain within the agreed budget, capture final Kafka bounds and independently reconcile application, PostgreSQL and Kafka results. Save full integrity output and inspect each warning. |

Record phase start/end times, actual admitted rates and fault windows. Report
normal/peak windows separately from faults and recovery. Compare measured
results with the pre-agreed limits. The qualification harness's simulated
operator polling interval is a test setting. Use measured human or automated
operator response for pilot acceptance.

Stop and fence staging if isolation breaks, a protected effect repeats, an
identity or payload conflicts, a required retained record is missing, or
the agreed stop budget is reached. Preserve the first failure, offsets and
database evidence before any rollback. Treat rollback as a separately
recorded action with its own checks.

## 3. Record acceptance

The application owner and evidence reviewer record PASS, FAIL or BLOCKED for
each phase. Require these outcomes for an overall PASS.

- Every expected committed identity is accounted for, with matching Kafka
  payload and key and the expected local effect. Repeated Kafka deliveries
  are recorded separately. Missing or substituted identities, repeated
  protected effects and ordering violations must be zero.
- At final drain, there is no unresolved Outbox or Inbox work, stale event or
  Inbox lease, or unexplained offset lag. Full integrity has zero violations
  and a complete findings report. Warnings are classified with evidence.
- The measured latency, lag, recovery time, retention behavior and operator
  response meet the agreed limits. The restore drill passes independently.
- The agreed deployment and binary rollback are reproducible using the
  recorded candidate artifacts, configuration and procedures.

Keep one acceptance record with the owners, dates, phase results, baseline
and final snapshots, independent reconciliation, observations against the
limits, restore results, artifact digests and evidence locations. Preserve
failures and untested cases. Close the pilot release item only after the
application owner and evidence reviewer sign off on the actual result.
