# v1 final candidate checks

This is a verification plan. Record actual results in the
[v1 acceptance record](V1_RELEASE_CHECKLIST.md). Start the checks only after
the final SHA and the execution environment have been agreed.

## 1. Freeze the candidate

Use at least Go 1.27.3 for candidate builds and SDK consumers; Go 1.27.2
contains the reachable crypto/x509 issue
[GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809). A passing scan built with
a different patch does not qualify packaged binaries.

Record the full candidate commit `S` and its diff from the qualified base
`d0d19d3662fc78a5318a3ed2732b443391aca725`. Identify the runtime, dependencies,
migrations, test harnesses, workflow definitions and artifact inputs in that
diff. Include the build toolchain, image base digest for each platform, and the
GoReleaser version. Keep the checkout clean and put outputs in a fresh
candidate-owned directory.

The qualification report must identify the SHA actually checked out in each
environment. Resolve the final SHA after any squash merge or Release Please
metadata commit. On a SHA change, review the delta, run the exact-SHA CI,
scans and artifact checks on the new SHA, and obtain the required multi-Relay
evidence on that SHA. Record earlier results with their original scope. The
72-hour managed qualification and the older multi-Relay soak are different
gates.

The candidate's schema-5 index migration is outside the schema-4 72-hour
evidence. Qualify the schema-4-to-5 maintenance upgrade, index installation
within the existing migration deadline, and index-only rollback to schema 4
before starting v0.9.1 artifacts. Do not infer this path from older rolling runs.

Keep `RELEASES_ENABLED` closed during verification. Before any publication
approval, confirm the release flow can promote the approved SHA without
creating a tag or starting publication on an unchecked SHA. An automatic
Release Please merge that can publish before this evidence is reviewed is a
release-flow blocker.

## 2. GitHub Actions

Use [CI](../.github/workflows/ci.yml) in the intended release repository. A
pull-request run can check a synthetic merge commit. For exact-SHA evidence,
use the `main` push run at `S` or dispatch CI against a branch fixed at `S`
once the workflow is available on the default branch. Confirm the checkout
SHA from the run metadata and logs. See
[GitHub workflow event refs](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_dispatch).

Require all nine named jobs to pass with their relevant steps executed:

- `monitoring`, `format`, `vet` and `staticcheck`
- `unit (Go 1.27.x)`, `race` and `integration`
- `vulnerability` and `build`

Check the applicable protected-branch rules and capture the workflow run and
attempt IDs, checkout SHA, workflow source and actual Go versions. CI selects
from `1.27.x`. Record the exact patch used for the release build separately.
Review the [CodeQL](../.github/workflows/codeql.yml) result and alerts when
available, with its own run SHA and scope. A skipped job, a conditional step
that never ran, or a run for another SHA leaves its evidence incomplete.

Use [Release container smoke](../.github/workflows/release-smoke.yml) for
amd64 and arm64 on the same SHA. Dispatch it when its pull-request path filter
does not schedule it. This workflow builds a smoke CLI. The packaged CLI and
images from GoReleaser have their own checks in step 5.

## 3. Vulnerability checks

Use the `govulncheck` version pinned by the candidate workflows. The current
pin is v1.7.0. Record the tool version, actual Go compiler, scan timestamp,
vulnerability database and its freshness, scope, output and exit status.

- Run `govulncheck ./...` from the clean candidate with the compiler selected
  for the release. Include the Go standard library and module dependencies.
- Run `govulncheck -mode=binary` on the executable extracted from each archive
  in step 5 and on each image's `/emitlane` if its bytes differ. Bind each
  result to the scanned binary's SHA-256. Binary findings describe linked
  symbols and need their own review.
- Use the default text scan's exit status for the gate. Preserve any
  machine-readable reports as additional evidence. A scanner or database
  error leaves the gate open. Require zero unresolved reachable source
  findings and zero unresolved findings in the shipped Go binaries. Record
  module/package-only reports with their scope and rationale.

The Go scanner covers the Go program. Record the container base digests and
the scope and outcome of any OS/image scan separately. See
[govulncheck usage and exit codes](https://pkg.go.dev/golang.org/x/vuln@v1.7.0/cmd/govulncheck).

## 4. Standalone multi-Relay release soak

Build `cmd/emitlane-soak` from `S`. Use the standalone `release` profile
with the same parameters as the reviewed historical run.

| Parameter | Planned value |
| --- | --- |
| Load, warmup and recovery budget | 60 min, 15 s and 10 min |
| Relay topology and workload | Four Relays, 3,500 ordered streams, 80% ordered, 512-byte payloads, configured target 250/s |
| Fault schedule | Seed 20261005 and one-minute fault interval |
| Fixture | PostgreSQL 16 with durable writes and at least 512 MiB shared memory, one JVM Kafka 4.3.1 broker pinned by image digest, dedicated loopback endpoints |
| Supervision | One foreground run under a dedicated supervisor. Two-hour outer deadline, including build, recovery and audits. |

Use an approved isolated Linux test environment with a verified resource
budget, owned PostgreSQL/Kafka containers and serialized heavy work. On the
shared test host, use the dedicated test user, rootless Docker and the common
exclusive runner lock. Stop before launch if the approved resource ceilings
cannot accommodate the profile. The historical run's full-host allocation was
scoped to that run.

Record the actual fixture, binary hash, configuration, clean SHA before and
after, logs, terminal service and supervisor status, and host/container
resource samples. Preserve `result.json`, `kafka-audit.json`,
`integrity.json`, `fixture.json` and process and fault logs. Review all six
completed fault categories, including the distinct broker pause and actual
restart, graceful Relay restart, crash takeover, cluster pause/resume and
membership change.

Pass requires zero test and supervisor exit codes, terminal `completed`,
valid release evidence, recovery within budget, and a complete independent
Kafka scan through the captured end of every partition. Verify identity,
payload, key and ordered continuity. Require zero missing events, unexpected
sequence skips, ordering regressions, terminal pending/inflight/dead and
blocked/gap states, runner errors and integrity violations. Review a complete
full integrity report and classify warnings. Count allowed Kafka duplicates
separately and report the observed rate for this fixture. A failure or
incomplete audit stays visible with its first failure evidence.

### PostgreSQL backlog diagnosis

The opt-in `TestPostgreSQLBacklogDiagnostics` is prepared for a separately
authorized short server run. It starts one private, durable PostgreSQL fixture
with the candidate's embedded migrations and indexes.

The synthetic fixture has 670,175 events with 512-byte payloads, 3,500 ordered
streams and four partition owners. It contains 442,365 pending events
(412,357 ordered and 30,008 unordered) and 227,810 delivered events. Each
stream's cursor follows its delivered prefix. Earlier ordered pending rows
precede the unordered pending tail to expose filtering cost on a mixed queue.
These logical counts match the failed run's final counters. Historical WAL,
retry churn and physical index state require separate evidence.

Four serial rounds rotate through the partition owners. Each round measures
six operations: production `ClaimOrdered`, unordered `Claim`, the lightweight
recovery work probe, exact `queueState`, exact Relay statistics and cached Relay
polling. Claim size is eight, event and partition leases
are four seconds, partition renewal is 250 ms, and the ordered minimum lease
is 1.25 seconds. The pgx tracer captures the SQL and parameters actually used.
Exact Relay statistics contribute separate queue and ordering statements.
Cached polling records its source capture time and whether a refresh occurred:
a cold/expired cache contributes those two statements plus the small live-control
query; a cache hit contributes only the live-control query. Preserve all 24
operations and derive the plan count from the captured queries rather than
expecting the previous 20-plan layout. The initial full observation is not a
warm-cache polling result. Record cold refreshes and hits separately.

For every statement, the fresh JSONL output records client time and
`EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON)`, including server
execution/planning time, rows, loops, filtering, buffer hits/reads and spill
information. The measurement connections enable I/O timing. Successful method
claims are rearmed, and EXPLAIN writes are rolled back. Final counts must match
the initial logical fixture. Errors and incomplete termination remain visible.

After the comparable four-round series, a rolled-back synthetic drain compares
the former bare `EXISTS` query with the materialized, ordered status probes.
Preserve all ten sparse operation records and ten sparse plans. Cases retain
one pending, inflight or dead row, no active rows, and a delivered-only gap.
Planner statistics retain the pre-drain distribution; the uncommitted versions
cannot be vacuumed. Record this MVCC limitation explicitly and review heap
visibility fetches as well as sequential scans. A limited index query alone
does not prove constant-cost recovery polling. Verify rollback restores the
original totals. Existing recovery and audit deadlines remain unchanged.

Seeding and preceding measurements populate caches. Record the round order
when interpreting repetitions. This serial diagnostic establishes individual
query costs; production contention and the observer's total CPU share still
need concurrent evidence.

After authorization, use the shared remote-testing resource boundary and
exclusive runner lock. Supply a fresh absolute output file with an existing
parent directory. The diagnostic shares a ten-minute budget for fixture setup
and measurements, using the existing release recovery duration. Existing
soak timeouts, query settings and acceptance criteria retain their definitions.

```sh
GOMAXPROCS=2 \
EMITLANE_SOAK_BACKLOG_RUN_ID=backlog-diagnostic-UNIQUE \
EMITLANE_SOAK_BACKLOG_OUTPUT=/absolute/fresh-results/backlog.jsonl \
go test -mod=readonly -p=2 -parallel=2 -count=1 -timeout=12m \
  -run '^TestPostgreSQLBacklogDiagnostics$' ./cmd/emitlane-soak
```

Preserve source identity, stdout, the complete JSONL and terminal supervisor
status. A complete diagnostic means all measurements were captured. Release
acceptance continues to require the full multi-Relay profile and audits.

Prepared regressions cover cache reuse/expiry, canceled waiters, failed refresh
and timestamp preservation, plus live pause/presence/partition fields and
unchanged public stream details. `TestQueueWorkProbePostgreSQL` is separately
opt-in with `EMITLANE_SOAK_QUEUE_PROBE_RUN_ID`; its private PostgreSQL fixture
checks empty, scheduled, inflight, dead and delivered queues, missing-head gaps,
and query-error handling against the unchanged exact reconciliation. These
focused changes passed on the intermediate trees recorded in the acceptance
record. They still need final-candidate CI and the separate multi-Relay run.

`TestCachedStatsUseOneDatabaseSnapshotAcrossAggregations` holds the statistics
reader after its queue aggregate, commits a new pending stream from another
connection, and requires both cached aggregates to retain the original snapshot.
The explicit uncached read must then observe the newly committed row and gap.

## 5. Package and inspect without publishing

Dispatch [Release container smoke](../.github/workflows/release-smoke.yml)
against the branch fixed at `S`. Its manual-only `release snapshot artifacts`
job builds the private snapshot and runs
[`check-snapshot.py`](../deploy/release/check-snapshot.py), saving all archives,
checksums, binary scans, image metadata and runtime startup evidence as a CI
artifact. The job has read-only repository permissions, performs no registry
login and uses snapshot mode. The publication workflow remains separate.

On a fresh isolated runner at `S`, use GoReleaser v2.18.0, as pinned in
[Release](../.github/workflows/release.yml), with the candidate's
[GoReleaser configuration](../.goreleaser.yaml) and
[release Dockerfile](../Dockerfile.release). Use `goreleaser check` and
`goreleaser release --snapshot --clean` with Buildx, a local Docker daemon,
and no publishing credentials. A snapshot builds local per-platform images
with `dockers_v2` and uses snapshot version metadata. Its evidence covers
the packaging path and local images. Public tag handling and registry
manifest assembly are checked separately. See
[GoReleaser snapshots](https://goreleaser.com/customization/publish/snapshots/)
and [local Docker v2 builds](https://goreleaser.com/customization/package/dockers_v2/).

| Output | Acceptance check |
| --- | --- |
| Release archives | Six archives for Linux, Darwin and Windows, each for amd64 and arm64. Linux and Darwin use `tar.gz`, Windows uses ZIP. Verify names and expected files, extract into fresh directories, verify executable target and source with `go version -m` and match the version metadata to the build. Record platform execution where performed. |
| Checksums | Verify every archive against the generated `checksums.txt`. Preserve the archive and extracted binary SHA-256 for each platform and tie the binary scan to that checksum. |
| Container images | Verify local Linux/amd64 and Linux/arm64 platform, OCI revision/version, non-root user, entrypoint and exact packaged `/emitlane` bytes. Run `version` and `help` offline on both. Record whether arm64 used native execution or emulation. |
| Supported runtime startup | On Linux/amd64, start the built image against an owned, isolated fixture and check `/healthz`, `/readyz` and graceful shutdown. Build and CLI smoke evidence on other platforms stays scoped to those checks. |

Save GoReleaser output and artifact inventory, hashes, build and image metadata,
base image digests, scan results and smoke logs. The release workflow rebuilds
artifacts for the release tag. After separately approved publication, resolve
the tag to the approved SHA, verify release version metadata and recompute
published archive checksums. Check the GHCR index for Linux/amd64 and
Linux/arm64, their platform digests and public access. Verify that the
configured stable tags for this release (`v1.0.0`, `v1.0`, `v1`, and
`latest`) resolve to the expected index. Record the actual release build
inputs. A changed compiler, dependency or base image requires review and
qualification of the affected artifacts. Snapshot hashes are evidence for
the rehearsal artifacts.

## 6. Close the evidence record

For each gate, record PASS, FAIL or BLOCKED with the SHA, exact command or
workflow run, environment, date, artifact or log references and limitations.
Require the reviewer to reconcile these records with the intended final
candidate, the [support matrix](V1_CONTRACT.md#initial-support-matrix) and the
other release gates before approving publication.
