# v1 release acceptance

This is the acceptance record for a future public v1.0.0, not a declaration that
v1 has shipped or that all checks have passed. Existing v0.9.1 releases remain
pre-1.0. Keep this record tied to the final candidate's source, dependencies,
configuration and built artifacts. Do not change an uncompleted item to pass
because a test exists or a previous version passed it.

## Current evidence

The clean candidate `4ad7e12a09b8428699c5456d53c32b6e6342daf5` completed a 24-hour
managed-consumer qualification on 2026-10-15. The test and supervisor exited zero,
with unchanged clean source, a final PASS, independent full Kafka and PostgreSQL
identity/effect/operator audits and an untruncated full integrity report.

- 3,978,967 committed events and protected database effects; no missing or
  substituted audited identities/effects, repeated protected effects, order
  regressions or stale Inbox leases.
- 3,979,433 Kafka records, including 466 allowed at-least-once duplicates.
- 288 full cycles with all eight fault categories each; 288 actual rebalance
  assignment proofs, maximum 8.352 seconds within the one-minute test budget.
- Load-end lag 6,291; recovery reached zero in 353.752 seconds within three hours.
- Integrity had one expected `RELAY_PRESENCE_STALE` warning for a killed helper.
- Aggregate sampled memory peaked at 6,113,878,016 bytes, mainly file cache;
  swap peaked at 8,847,360 bytes. No new hard-limit/OOM/kill events were observed.

This evidence covers one Linux host, PostgreSQL 16, one Kafka 4.3.1 JVM broker,
eight Kafka partitions and eight ordered streams. Durable PostgreSQL writes
were enabled. The measured configuration used four consumer workers, a workload
pool of eight and a 3 GiB Kafka limit without a CPU ceiling. It also used a
simulated operator that repaired exhausted poison events every 100 ms. These
are measured test conditions, not production defaults or hardware requirements.

The actual admitted rate, including fault probes, was 46.05 events/second at a
configured target of 50. Healthy lag grew and later partly drained during load.
Client MarkProcessed cost rose from 1.20 to 4.68 ms between the first and last
load hour; MarkRetry rose from 4.09 to 6.62 ms. The test is **not** a sustained
50 events/second capacity certificate. Protected effects are transaction-local
PostgreSQL effects; external effects still require idempotency. No end-to-end
exactly-once guarantee is made.

Focused, opt-in [validation profiles](V1_VALIDATION.md) cover retained Inbox
access paths and replicated Kafka faults. Their existence is not a passing result.

## Security toolchain update

The subsequent candidate check on2026-10-15 stopped at vulnerability scanning:
Go1.27.0 had nine reachable standard-library vulnerabilities, with fixes reported
in Go1.27.2. Passing correctness/race tests do not override that failure. The
candidate now requires Go1.27.2, pins Docker build images to that patch and adds
a reachable-vulnerability scan before release publication. A fresh passing scan
and rebuilt artifacts are still required; the old24-hour result is correctness
evidence, not current security qualification.

## Acceptance items

Every item needs a result, exact candidate identity, configuration, evidence
location and limitations. A failure must remain visible; do not automatically
extend or restart a failed endurance run and overwrite its result.

- [ ] Freeze the final runtime, dependency and migration source and review all
  changes since the qualified candidate, including batched ordering lease renewal.
  Record how merge/release metadata commits map to the qualified runtime source.
- [ ] Pass formatting, vet, Staticcheck, unit, race, integration, monitoring,
  vulnerability and build checks on the final candidate. Existing green main CI
  does not qualify a different local candidate.
- [ ] Record the separate multi-relay release soak and its independent Kafka and
  full integrity audits, as required by [Releasing](RELEASING.md).
- [x] Review the 24-hour managed qualification above for its exact source and profile.
- [ ] Complete longer-duration memory/storage observations for the chosen final
  candidate and retention policy. The 24-hour run does not establish 72-hour results.
- [ ] Diagnose and bound Inbox transition costs with millions of retained rows,
  realistic update churn and recorded query plans. Preserve lease fencing and
  transaction rollback behavior in any optimization.
- [ ] Define and verify supported load, latency/lag and recovery objectives on aged
  data. State actual admitted rate and resource conditions. Do not invent a safe
  capacity by scaling down one measured average.
- [ ] Exercise multi-broker leader/ISR and network failures for the intended Kafka
  deployment, and database failover if HA is in the supported deployment contract.
- [ ] Test the explicitly supported infrastructure-version and mixed-version
  upgrade/rollback matrix. List untested combinations as unqualified.
- [ ] Perform production-representative backup/restore/PITR and offset reconciliation
  drills; record the recovery point and time. A small logical restore fixture or
  readable archive is not this drill.
- [ ] Complete an independent application pilot, following [Pilot](V1_PILOT.md).
  A repetition of the synthetic qualification cannot stand in for this evidence.
- [ ] Approve the v1 source/configuration/wire/migration compatibility boundaries,
  supported environments and security-fix/support policy, using the prepared
  [v1 contract](V1_CONTRACT.md). Remove conflicting
  pre-1.0 claims only when those decisions have actually been made.
- [ ] Intentionally prepare the v1 version/changelog and release automation settings.
  The pre-major Release Please configuration does not itself request v1.0.0.
- [ ] Verify exact-candidate release archives/checksums and Linux amd64/arm64 image
  contents and startup. Confirm published tags/digests and access after publication.

The remaining infrastructure, restore, longer-duration and independent-pilot items
continue the v1 qualification boundary recorded in [v0.9](V0_9.md). If the product
support scope is deliberately narrowed, document the decision and resulting
limits before accepting a release; do not silently relabel an untested case as passed.
