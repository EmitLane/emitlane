# v1 PostgreSQL point-in-time recovery drill

This procedure is prepared for a future isolated drill. Record the exact
candidate and results in the [v1 acceptance record](V1_RELEASE_CHECKLIST.md).
It exercises PostgreSQL 16 physical recovery and deliberate Kafka consumer
offset reconciliation for the [initial support scope](V1_CONTRACT.md#initial-support-matrix).

## 1. Freeze the drill inputs

- Assign a database operator, a Kafka operator and an application owner. Record
  the candidate SHA and binary or image digest, PostgreSQL and Kafka versions,
  schema version, configuration, backup tool, database size and retained volume.
  Agree on the target UTC time, maximum acceptable data-loss window (RPO) and
  time to verified service recovery (RTO) before running the drill.
- Use a dedicated staging PostgreSQL source, a fresh restore instance, and a
  dedicated staging Kafka cluster. Use the same staging Kafka topics and group
  throughout the exercise to preserve their offset coordinate space. Record
  cluster ID, topic IDs, partition inventory, group-to-Inbox consumer name
  mapping, and exact TLS/authentication mode. Restrict network access and
  credentials to staging, including downstream handler destinations.
- Retain a verified physical base backup of the cluster, with the application
  data, Outbox, Inbox, ordered cursors and migration history together. Finish
  the backup before the target. Verify it with the backup tool, retain the
  complete WAL chain through the target and a later transaction, and record the
  timeline and archive manifest. Include tablespaces and roles in the restore
  inventory. Store the evidence and archive outside the restore data directory.
- Use an independent application ledger to map committed business transactions,
  event IDs and payload/key digests to expected local effects. Capture a
  reconciled starting checkpoint for each Kafka partition while its older
  prefix is still provable. Keep the Kafka records and Inbox markers needed by
  that checkpoint through the entire recovery window.

Prepare the following witnesses using the application's real transaction
paths. In each case save exact IDs and commit evidence in the external ledger.

| Witness | Staged timing and expected state at the target |
| --- | --- |
| Already applied | Source business data and Outbox commit before the target. Kafka delivery, Inbox and its protected effect also commit before the target. Keep the Kafka record for a duplicate replay check. |
| Offset ahead of restored data | Business data and Outbox commit before the target and the record is already in Kafka. Pause the relevant consumer until after the target, then let its effect and Kafka offset commit. At the target the input is valid and the effect is absent. Use the same partition as the already-applied witness where possible. |
| Restored Outbox work | Commit an ordered event before the target and keep it unpublished until staged resumption. At the target its durable state is pending. Record how the event and the stream cursor should advance on resumption. |
| After-target marker | Commit a staging-only database marker after the target. It has no Kafka publication. It must be absent after restore. |

Use the source database clock to choose a UTC cut between verified commits.
Place the witnesses clearly on either side of it. After the after-target
writes and offset advance, stop new input, Relay, consumers and their automatic
restarts. Wait until the Kafka group is inactive. Capture the original group
offsets, per-partition retained start and end, and the full reference state.
Preserve that reference and prevent further writes to it.

## 2. Restore PostgreSQL and verify the cut

Restore the backup with the selected tool into an unused PostgreSQL 16 data
directory. Verify ownership and tablespace paths, read-only WAL archive access,
and isolated PostgreSQL client access. Remove inherited upstream connection
settings from the restore configuration and direct any future archive writes
to a separate staging archive. Keep application processes fenced.

Set the restore command for the verified archive and create `recovery.signal`
in the restored data directory. Use one recovery target. The following are
template values for the isolated instance. Replace the timestamp, timeline and
archive path with values from the drill record.

```conf
hot_standby = on
restore_command = 'cp /drill/wal-archive/%f %p'
recovery_target_time = '2026-11-01 12:00:00+00'
recovery_target_inclusive = on
recovery_target_timeline = '1'
recovery_target_action = 'pause'
```

The restore command must return a failure for missing WAL and use the archive
format produced by the selected backup tool. Record PostgreSQL's log of
reaching the configured target and pausing. From a read-only inspection session,
capture `pg_is_in_recovery()`, `pg_get_wal_replay_pause_state()`,
`pg_last_wal_replay_lsn()` and `pg_last_xact_replay_timestamp()`. The last
replayed transaction can precede the target. Use the stop log, timeline and
both sides of the witness set to establish the cut. See
[PostgreSQL continuous archiving and PITR](https://www.postgresql.org/docs/16/continuous-archiving.html)
and [recovery target settings](https://www.postgresql.org/docs/16/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET).

Before allowing writes, compare schema history, roles and the exact business,
Outbox, Inbox and ordered cursor states with the ledger at the cut. Check the
witness IDs, payload digests, lifecycle states and protected effects. Save
read-only integrity output and account for any expected unfinished work or
leases. A missing WAL segment, wrong timeline, unproven target, inconsistent
witness or schema mismatch stops the drill.

## 3. Calculate a replay position for every partition

Keep the group inactive throughout planning and offset changes. Use the
following values for each group, topic and partition.

- `E` and `H` are the retained start and the captured exclusive end of the
  staging log. `C` is the original committed **next offset**, if present.
- `L` is a retained checkpoint that is at or before `C` when `C` exists.
  The application ledger must prove the restored prefix below `L`. Proof
  must cover any records aged out below `E` that are relevant to recovery.
- `U` is the first visible record at or after `L` whose required protected
  effect is absent at the cut and whose input is proven valid for replay.
  Use `H` if every record through the captured end is already applied.

Scan the readable records in `[L, H)` by direct partition assignment with the
application's fetch visibility and identity resolver. Use a read-only
auditor that leaves group offsets untouched. Enumerate actual records and
their coordinates, identity and payload/key digests. Numeric holes in offsets
do not establish which business records exist. Reconcile each record with the
ledger and restored Inbox **and** effect. A processed row and matching effect
prove it was applied. A valid replay record can have both absent or an
unfinished Inbox row and no effect. Record duplicate IDs at their actual
offsets. Inbox stores first observed coordinates, so its maximum offset
cannot establish a contiguous prefix. Kafka record time cannot establish a
PostgreSQL commit boundary. See [managed consumer identity and offset behavior](CONSUMER_RELIABILITY.md).

Use `R = min(C, U)` when `C` exists and `R = L` when it is absent. An
earlier retained position at or after `L` is allowed if the entire replay
range has been reconciled. Choose such a position for the duplicate witness
to prove it is redelivered without repeating its effect.

For example, suppose `L=120`, the applied witness is at 120, the unapplied
witness is at 121, and `C=122`. Offset 121 is the latest safe resume point
if it is the earliest unresolved record. Use 120 for the duplicate replay
check. Save `E,H,C,L,U,R`, the reason and the evidence references per
partition, with one row for every partition the group can consume.

Stop if a required range has been removed by retention or compaction, if
topic identity or coordinates changed, or if `C`, `L` or `R` is outside
the captured bounds. Stop for missing deduplication evidence, conflicting
payloads or identities, or effects without matching Inbox state. Distinguish
valid Kafka input that was processed after the cut from an Outbox publication
whose source business transaction was rolled back by PITR. The application
owner must reconcile or compensate the latter explicitly before replay.
Keep ambiguous records blocked and preserve evidence. Never jump past them.

## 4. Apply and verify the plan in staging

1. Have the application owner and Kafka operator review the partition plan
   and the PostgreSQL cut. Confirm the staging cluster and topic IDs, inactive
   group membership, and current retained bounds again. Save the original
   offsets, including missing entries.
2. Use the pinned Kafka admin tool to preview the exact offsets for this
   group. For Kafka 4.3.1, use a single-group CSV with one
   `topic,partition,next-offset` row per partition and no header. For the
   preview, supply the staging bootstrap, staging credential file and group
   to `kafka-consumer-groups.sh --reset-offsets --from-file ... --dry-run`.
   Compare every previewed value and partition with the approved plan.
   See [Kafka 4.3 offset reset operations](https://kafka.apache.org/43/operations/basic-kafka-operations/).
3. Apply the reviewed plan only to the inactive staging group. Read back
   the entire map before starting any consumer. Kafka offset changes can
   partially succeed. On an error, keep the group fenced, compare all
   partitions, and resolve differences from the saved plan. Recheck retention
   bounds and reject missing, adjusted or out-of-range offsets. The built-in
   consumer can fall back to the retained start, so explicit read-back is required. See
   [Kafka Admin offset API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/Admin.html).
4. After cut and offset approval, finish recovery of the paused staging
   instance with `pg_wal_replay_resume()`. Verify it has left recovery and
   record the resulting timeline and readiness. Start consumers against the
   restored database with the same logical Inbox consumer names. Keep Relay
   and new input stopped while checking replay through the captured end.
5. Verify that the applied witness was redelivered with one durable effect,
   and the missing witness was rebuilt once from Kafka. Then start the
   staging Relay and exercise the restored Outbox work. Verify cursor progress
   and test a new staging transaction end to end. Recover stale work using
   expiry and documented audited retries. Keep the reference database intact.

## 5. Decide and retain evidence

Require exact reconciliation of expected business data, Outbox and Inbox
identities, Kafka keys/payloads, protected effects and ordered progress.
Require no unexplained missing identities, repeated protected effects or
unresolved blocked work. Check final offsets against the measured end for
each partition and save the full integrity report with warnings reviewed.
Kafka redeliveries are allowed and counted separately.

Save the backup and archive identifiers, cut proof, pre/post snapshots,
partition plan and read-back, witness ledger, timeline and measured RPO and
RTO. Measure RTO from the agreed outage start to verified processing and
reconciliation. Record what post-cut data was intentionally excluded and
what was recovered by replay. Have the operators and application owner
record PASS, FAIL or BLOCKED with the evidence. On failure, fence all staging
writers again and preserve the original database, archive and offset evidence.
Restart from a fresh isolated restore for any approved repeat.
