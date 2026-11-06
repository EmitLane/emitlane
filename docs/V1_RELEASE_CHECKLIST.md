# v1 release acceptance

The qualified base is `d0d19d3662fc78a5318a3ed2732b443391aca725`.
Public v1.0.0 acceptance is pending. Record the final release SHA, configuration and
artifact digests before accepting the release. The
[v1 contract and support matrix](V1_CONTRACT.md) define the initial scope.

## Reviewed evidence

### Managed 72-hour qualification on the base

Run `v1-soak-72h-20261103-051438-d0d19d3`, completed 2026-11-06. The
terminal review (`review.json`) reconciled the clean start/end SHA, zero exit
codes from the test, supervisor and service, and the final harness PASS.
All 2,682 result files were verified against SHA-256 checksums.

- Completed 864 cycles, each with all eight faults, and recovered to zero lag
  in 32.05 s from a load-end lag of 78.
- Independent Kafka and PostgreSQL audits matched 12,668,755 event identities
  and protected effects. No audited events were lost, no protected effects
  were repeated, and ordering checks passed. Kafka recorded 1,312 permitted
  duplicate records.
- Operator audit and counter reconciliation passed. The untruncated full
  integrity report found zero violations, stale event leases or stale Inbox
  leases. Its one warning was `RELAY_PRESENCE_STALE` for a killed helper.
- The fixture ran on Linux/amd64 with durable PostgreSQL 16 and one Kafka
  4.3.1 broker. It used eight topic partitions, four consumers, a pool of
  eight, a 3 GiB Kafka memory limit, and a simulated operator polling every
  100 ms. Memory and storage samples cover this profile, with no new
  hard-memory-limit or OOM events.
- At a configured target of 50/s, the generator measured 48.87 successful
  writes/s across load and 49.78/s in complete producer windows outside faults.
  The measured rates and recovery time apply to this fixture.

### Aged Inbox and short load evidence

- The integrity fix review (`v1-integrity-fix-review.json`) ties the changed
  verifier to differential semantics, race and integration checks, and an
  all-fault rehearsal with full audits. Four full-verifier trials on a logical
  replay of about 12.72 million Inbox rows with update churn took 7.78–14.40 s.
  This measures the full verifier. Lifecycle transition costs and production
  restore remain separate checks.
- Run `v1-checks-queued50-20261106-102323-172c4a5` (`queued-measurement.json`
  and `scheduler-trace-targeted.json`) used a test-only descendant of the base.
  The 15-minute workload used two million synthetic historical events, one
  million churn updates, and a queue of 100. It offered all 45,000 slots with
  zero scheduler misses or overflows. The 810 s of complete windows outside
  faults yielded 40,500 commits at 50/s. Full audits passed. This is a short
  measurement with tracing enabled.
- The earlier `b0d19ac` run's four clean-window misses occurred in the
  generator before enqueue, outside the delivery contract for committed
  events. The traced run recorded zero misses. Linux timer investigation
  remains diagnostic work.

### Historical and inherited evidence

- `4ad7e12` passed the 24-hour managed profile. The earlier 72-hour
  `2c049cc` attempt remains failed because its full Inbox audit timed out.
- The focused aged-Inbox, replicated Kafka and standalone rolling steps
  passed on `299dda4`. The enclosing run
  `v1-checks-20261015-064142-299dda4` failed at Staticcheck. Later source
  maps carry forward the completed steps as historical evidence.
- The standalone multi-Relay release soak and source security scan on
  `41ae6b7` and artifact checks on `4e211a7` apply to those sources and
  fixtures. Preserve their source maps when assessing the final SHA.

## Acceptance items

Record a result, source identity, configuration, evidence location and
limitations for each item.

- [x] Select the qualified base and document the initial contract and support
  matrix. Confirm the final release notes against these boundaries at sign-off.
- [x] Review the 72-hour managed qualification, memory/storage observations
  and final audits for `d0d19d3` and the recorded fixture.
- [x] Record the aged full-Inbox verifier evidence and the short queued
  diagnostic with their separate source and workload scopes.
- [ ] Freeze the final runtime, dependencies, migrations and build inputs.
  Record how the final merge and release commits map to the qualified source.
  Review any delta and qualify the affected guarantees.
- [ ] Pass the required GitHub CI, reachable-vulnerability scan and builds on
  the final candidate. Record the separate multi-Relay release soak and its
  independent Kafka and full integrity audits for the selected release source.
  See [Releasing](RELEASING.md).
- [ ] Verify the initial deployment matrix, including the production Kafka
  transport and authentication mode and the v0.9.1 upgrade and binary rollback
  path. Qualify additional topologies only if they will be listed as supported.
- [ ] Define and verify workload-specific load, queue age, lag and recovery
  objectives on aged data. Record lifecycle transition plans and costs under
  the chosen retention and churn policy. Sustained 50/s remains unverified.
- [ ] Complete a production-representative restore/PITR and Kafka offset
  reconciliation drill. Record the recovery point, recovery time, and
  reconciliation after resumption. See [Backup and restore](BACKUP_RESTORE.md).
- [ ] Complete an [independent application pilot](V1_PILOT.md) and record the
  application owner's sign-off against the agreed operating objectives.
- [ ] Review the final version, changelog, upgrade instructions, support policy
  and Release Please settings. Verify exact-candidate archive contents and
  checksums and Linux/amd64 and Linux/arm64 image builds and startup.
- [ ] Approve publication after the release gates above are closed. After
  publication, verify the tag, release assets, image digests and access.
