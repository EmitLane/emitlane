# Backup and restore boundaries

PostgreSQL business data, Outbox, Inbox, ordered stream cursors and migration
history belong in the same consistent backup. Include `emitlane` and the relevant
application schemas. An Outbox-only export cannot reconstruct the atomic business
transaction; an Inbox-only restore cannot reconstruct the handler's effects.

A database restore and a Kafka restore are separate operations. Kafka records and
consumer group offsets are not part of `pg_dump`. An older database snapshot can
remove processed Inbox markers and business effects while Kafka offsets remain
ahead. Starting the group unchanged can then skip effects missing from the restored
database. Conversely, rewinding Kafka can redeliver previously committed effects.
There is no automatic cross-system point-in-time recovery in EmitLane.

## Deployment and backup preparation

- Apply `emitlane migrate up` as an explicit deployment step before starting a
  binary that requires the current schema. Run `emitlane doctor` afterwards.
- Store database and Kafka credentials in your platform's secret mechanism.
  Restrict the runtime probe/metrics listener to a private network and configure
  the separate admin listener and token only when needed.
- Use `/healthz` for process liveness and `/readyz` for dependency readiness.
  A Kafka outage should not make your orchestrator restart a healthy relay in a
  loop. Durable pause intentionally does not fail either probe.
- Stop with SIGTERM and allow more than `EMITLANE_SHUTDOWN_TIMEOUT` (default 15s)
  before SIGKILL; 30s is a starting grace period. Claims stop and started work
  drains within the configured bound. Forced death still relies on lease expiry.
- Back up the whole application database with your established PostgreSQL
  backup/PITR process. Keep credentials out of command arguments and logs; use
  libpq service/password files or your backup agent's secret integration.
- Record the binary version, schema history, backup timestamp, Kafka topics,
  group/consumer names, broker retention policy and recovery objective.

A logical dump is useful for a drill. It does not replace WAL archiving or a
production recovery policy. Use a compatible PostgreSQL client; validate your
roles, extensions and restore privileges as part of the drill.

## Isolated restore drill

1. Restore into a **new isolated database**, leaving the source intact. Prevent
   restored relays, producers and consumers from contacting production Kafka.
   Inspect credentials and destinations before starting any process.
2. Restore the complete backup in one transaction where your backup format
   supports it. Reapply intended role/ownership privileges through your deployment
   process; test backups use `--no-owner --no-acl` only for disposable databases.
3. Check schema history with the candidate binary. Repeating `migrate up` on a
   current valid restored schema should be a no-op; incompatible history must
   stop the drill rather than be edited to look current.
4. Compare exact retained event IDs, opaque payloads/headers, status, attempts,
   retry timestamps and leases. Compare Inbox IDs and lifecycle metadata together
   with the handler's business effects. Compare ordered cursors and domain
   sequences. Dead records must remain visible; active leases must remain
   recoverable after their stored deadlines.
5. Run bounded read-only integrity checks and save the JSON, warnings and exit
   code. A clean durable-state report cannot establish Kafka retention or offset
   correspondence; examine those independently.
6. Before any real resumption, choose and validate a Kafka offset/retention
   reconciliation plan for each consumer. Compare restored processed markers and
   business effects with retained source records. Retention may have already
   removed the required records. Preserve the original database/backup and broker
   evidence until reconciliation is complete.
7. In an isolated environment, verify pending delivery, duplicate handling,
   retry/dead recovery and ordered stream progress. Record downtime and recovery
   observations. A database-only fixture is not evidence for an entire failover.

## Retention

Relay cleanup deletes only old delivered Outbox rows in bounded batches. Pending,
inflight and dead events must not be pruned as a maintenance shortcut. Losing a
historical delivered row also removes local inspection/replay evidence. Retain
business audit evidence independently when required.

Inbox deduplication lasts only while processed markers are retained. Keep its
horizon at least as long as Kafka redelivery, operator replay and restore windows.
An old producer or backup can reintroduce an identity after its marker is gone.
Kafka retention must cover the oldest retry/dead-blocked consumer record and the
planned recovery window. There is no universal safe TTL across these stores.

## Automated evidence

`TestLogicalBackupRestorePreservesDeliveryState` performs a real `pg_dump` and
`pg_restore` between disposable PostgreSQL 16 databases. It compares retained
Outbox/Inbox state, schema history and constraints, ordered cursors and an atomic
business/Outbox pair; repeating migrations must preserve the restored snapshot.
It never replaces the source database or rewinds Kafka offsets. This small fixture
does not qualify production-size restore timing, PITR, HA failover, external role
configuration or cross-system recovery.
