# Independent v1 application pilot

Use an application outside the synthetic qualification harness and an isolated
staging environment. The application owner chooses realistic event mix, payloads,
traffic, retention, operator response time and recovery objectives. Do not connect
a fault injector to production or reuse shared application databases for a test.

## Before starting

Record:

- application owner, candidate commit and binary/image digest;
- PostgreSQL/Kafka versions and topology, TLS/SASL settings and resource limits;
- business transaction and protected effect, stable event identity and ordering scope;
- normal/peak admitted event rate, payload distribution and retained-row volume;
- retry/lease/backoff settings, poison policy, alert routing and operator availability;
- maximum acceptable queue age/lag and recovery time, backup/PITR objectives;
- Kafka and Inbox retention horizons covering replay and restoration;
- rollout and rollback procedures, including migration and offset reconciliation.

Use the application's real transaction paths. Write the business row and Outbox
in the same transaction. Commit the managed Inbox transition with the protected
business effect. Protect external side effects separately. Exercise duplicates
with the same event ID; replay-created new IDs are intentionally different work.

## Exercise and observe

Run normal and expected peak traffic long enough to include the application's
retention/vacuum cycle and a representative retained-row volume. Capture actual
admission, queue age/lag by partition, retry/dead/operator activity, database pool
waits, SQL plans/costs, table/index sizes, memory, goroutines and OOM counters.

In staging, verify process restarts, consumer group rebalances, dependency outages
and the deployment's broker/database failover behavior. Repair poison input through
the documented audited operator path. Measure alert delivery and the real operator's
response; the qualification harness's 100 ms simulated operator is not a promise.

Perform a separate isolated recovery drill with the application's backup/PITR
procedure. Reconcile business data, Outbox, Inbox, ordered cursors and Kafka offsets
before resuming. Restore read success alone does not prove correct resumption.

## Acceptance record

Stop new input, recover within the agreed budget and independently reconcile all
committed event IDs, payloads and protected effects. Record Kafka duplicates as
allowed at-least-once delivery; require zero missing/substituted identities,
repeated protected effects and ordering violations. Run full integrity verification
and investigate each warning. Compare measured lag/recovery and resource trends
with the objectives agreed before the pilot.

Keep a signed-off result containing the evidence and any failure or untested case.
An operator must be able to reproduce the candidate/configuration and recovery
steps. No synthetic PASS or checklist entry can substitute for the application
owner's actual pilot outcome.
