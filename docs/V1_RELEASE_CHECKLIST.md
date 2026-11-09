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

The candidate now adds schema migration 5 for the pending-unordered claim
index. The schema-4 72-hour result above is inherited evidence. The focused migration/index regression run below passed on an intermediate
source tree. Final candidate CI, representative deployment acceptance and the
new multi-Relay soak remain open.


### Focused candidate regression evidence — 9 November 2026

These are intermediate dirty-tree results on branch `v1-release-contract`,
base `5cca8df`; they are not a final-SHA release qualification.

- The ordered-head diagnostic and its focused regressions passed in
  `v1-checks-pg-heads-20261109-044015-5cca8df`.
- `v1-checks-unordered-index-20261109-051149-5cca8df`, tree
  `bcce389dc2a01a5caa301a78c6b0b50dea1e658c`, passed 19 top-level tests
  and 32 subtests, including embedded/released migration checks, schema
  4→5→4 rollback and unordered claim. On the synthetic mixed backlog,
  the unordered pending index returned eight rows with no discarded rows.
- `v1-checks-sparse-probe-20261109-094314-5cca8df`, tree
  `f5e4aebf00ee10f0cc80529c1d4736167fffc571`, passed 17 focused tests
  and the query diagnostic: 24 baseline operations, 30 baseline plans and
  ten old/new sparse comparisons with ten additional plans. Both probes
  returned the expected state for pending, inflight, dead, empty and gap
  cases; the simulated drain was rolled back and original totals verified.
- The limited status probes used indexes in all sparse plans. They still
  required 442,366 heap visibility checks after the uncommitted synthetic
  drain when no pending row remained, with client times of 189–320 ms.
  This does not establish uniformly fast polling on aged data or actual
  recovery capacity. Cached statistics hits were approximately 0.75 ms;
  full observation and operator reads remain separate exact queries.

The fixture was PostgreSQL 16.15 with 670,175 events, 442,365 pending,
3,500 streams and durable writes, using the explicitly authorized full
resource profile. Each run finished with test/supervisor exit zero and actual
terminal service success. Full result copies were SHA-256 verified. No new
multi-Relay recovery, sustained input-rate, PITR or independent-pilot PASS is
inferred from these focused runs. A previous failed migration attempt stays
recorded separately; it is not relabelled as a pass.


### First frozen candidate checks — 9 November 2026

Candidate `6bf52ea` was pushed to draft PR #39. Its PR CI run
[42597636552](https://github.com/EmitLane/emitlane/actions/runs/42597636552)
and exact-SHA CI run
[42598601811](https://github.com/EmitLane/emitlane/actions/runs/42598601811)
passed eight jobs but failed `TestLogicalBackupRestorePreservesDeliveryState`.
The test seeded schema 4, then incorrectly treated the new schema-5 migration
as a repeated no-op. The correction seeds the current schema before the
logical backup and keeps its full retained-state equality assertions. Separate
upgrade/rollback tests retain the schema-4-to-5 coverage.

The exact-SHA private packaging run
[42598303321](https://github.com/EmitLane/emitlane/actions/runs/42598303321)
built its snapshot archives and local images but stopped at the source scan:
Go 1.27.2 was affected by
[GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809). Go 1.27.3 contains the
fix. The candidate module minimum and Docker build compiler now require
1.27.3. The ordinary CI scanner used 1.27.3; its success did not cover the
1.27.2 snapshot binaries. No completed packaged-binary or runtime-startup
verification is inferred from the failed packaging job.

The one authorized four-Relay hour remains pinned to `6bf52ea` and its
Go 1.27.2 binary. It must finish and be reviewed under that identity. A later
candidate or changed build compiler needs its own final-SHA qualification;
this source update does not authorize another server soak.


Candidate `12f7438` passed all nine jobs in exact-SHA CI
[42600103887](https://github.com/EmitLane/emitlane/actions/runs/42600103887)
with Go 1.27.3. Its packaging run
[42600190560](https://github.com/EmitLane/emitlane/actions/runs/42600190560)
passed the source vulnerability scan but exposed an error in the new verifier:
it searched for the injected release version in `go version -m` output, which
omits the ldflags string under `-trimpath`. The downloaded artifact's full ZIP
CRC and SHA-256, all six archive checksums, target build metadata, clean full
VCS revision and embedded version bytes were independently checked offline.
The verifier now checks build settings and embedded version bytes separately;
Linux image execution must also return the exact version and commit. The
failed job remains a failure, with its binary scans and runtime checks open.

- [x] Select the qualified base and document the initial contract and support
  matrix. Confirm the final release notes against these boundaries at sign-off.
- [x] Review the 72-hour managed qualification, memory/storage observations
  and final audits for `d0d19d3` and the recorded fixture.
- [x] Record the aged full-Inbox verifier evidence and the short queued
  diagnostic with their separate source and workload scopes.
- [ ] Freeze the final runtime, dependencies, migrations and build inputs.
  Record how the final merge and release commits map to the qualified source.
  Review any delta and qualify the affected guarantees.
- [ ] Validate migration 4→5 and rollback 5→4 on representative retained data,
  including preservation of unfinished Inbox work, event leases and ordered
  cursors. Recheck unordered-claim plans on the mixed backlog. Retain the
  existing timeouts and audit acceptance requirements.
- [ ] Pass the required GitHub CI and release container smoke on the final
  checkout SHA. Record the runs and actual toolchains. See the
  [final candidate plan](V1_FINAL_SHA_CHECKS.md).
- [ ] Complete source and packaged-binary vulnerability scans on the final
  candidate. Record scanner inputs, findings and binary checksums.
- [ ] Pass the standalone multi-Relay release soak on the selected release
  SHA, with independent Kafka and full integrity audits.
- [ ] Verify the initial deployment matrix, including the production Kafka
  transport and authentication mode and the v0.9.1 upgrade and binary rollback
  path. Qualify additional topologies only if they will be listed as supported.
- [ ] Define and verify workload-specific load, queue age, lag and recovery
  objectives on aged data. Record lifecycle transition plans and costs under
  the chosen retention and churn policy. Sustained 50/s remains unverified.
- [ ] Complete a production-representative restore/PITR and Kafka offset
  reconciliation drill. The [drill procedure](V1_PITR_DRILL.md) is ready for
  staging. Record the recovery point, recovery time, partition offset
  read-back and reconciliation after resumption.
- [ ] Complete the planned [independent application pilot](V1_PILOT.md).
  Record the selected application, its pre-agreed operating objectives and
  the application owner's sign-off on the measured results.
- [ ] Verify the final candidate's private GoReleaser snapshot, archive
  contents and checksums and Linux/amd64 and Linux/arm64 image builds and
  smoke results. Record the supported runtime startup result.
- [ ] Review the final version, changelog, upgrade instructions, support policy
  and Release Please settings. The [v1 Release PR draft](V1_RELEASE_PR.md)
  is prepared for review.
- [ ] Approve publication after the release gates above are closed. After
  publication, verify the tag, release assets, image digests and access.
