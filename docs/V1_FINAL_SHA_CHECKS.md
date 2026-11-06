# v1 final candidate checks

This is a verification plan. Record actual results in the
[v1 acceptance record](V1_RELEASE_CHECKLIST.md). Start the checks only after
the final SHA and the execution environment have been agreed.

## 1. Freeze the candidate

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
| Fixture | PostgreSQL 16 with durable writes and at least 512 MiB shared memory, one Kafka 4.3.1 broker, dedicated loopback endpoints |
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

## 5. Package and inspect without publishing

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
