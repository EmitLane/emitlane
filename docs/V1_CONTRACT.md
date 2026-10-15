# Contract to accept with v1.0.0

This document prepares the stable-release contract. It takes effect only with an
accepted, qualified v1.0.0 release; it does not relabel current v0.9.1 artifacts
or pending candidate checks as stable. [Acceptance](V1_RELEASE_CHECKLIST.md)
remains incomplete until its evidence has been reviewed.

## Stable application surfaces

Keep the current module path `github.com/emitlane/emitlane` through v1.
Documented application-facing APIs in `outbox`, `inbox`, `consumer`, `broker`,
`broker/kafka`, `relay`, `storage/postgres`, `integrity` and `telemetry` are the
v1 integration surfaces. Use constructors/default helpers, options and keyed
struct literals. Internal packages, test/failure-injection hooks, examples,
benchmark tools and undocumented implementation details are not SDK contracts.
The importable `config` package belongs to the standalone binary's configuration
surface; embedding should use package-specific configuration instead.

Existing storage methods whose signatures expose `internal/admin` types are
binary implementation glue, not an external administration SDK. Use the documented
HTTP/CLI administration surface. An API compatibility test must compile an external
client using the supported SDK composition.

Within v1, minor/patch releases preserve documented source and wire contracts.
Minor versions may add compatible optional features. Deprecated supported APIs
remain available through v1; incompatible API changes require a future v2 module
path. Correctness/security fixes may reject invalid input that was previously
accepted; release notes must identify the changed validation and upgrade impact.
Do not promise compatibility with struct layout, exact prose error messages,
internal goroutine arrangements, undocumented ordering, or arbitrary custom storage
implementations that do not implement a requested optional capability.

## Durable and delivery contracts

- The caller owns the Writer transaction. Enqueue success is not transaction commit.
- PostgreSQL is authoritative. LISTEN/NOTIFY is a wake-up optimization.
- Relay delivery is at least once. An ambiguous or acknowledged publish can be
  redelivered after a crash. Kafka producer idempotence is not a substitute for
  downstream event identity and deduplication.
- Managed business effects and Inbox processed state commit together in one local
  PostgreSQL transaction. External side effects need application idempotency or
  another transactional Outbox. No end-to-end exactly-once guarantee is offered.
- Ordering is opt-in, per documented stream. Gaps/dead expected events block a
  stream and require visible recovery; no global ordering guarantee is made.
- Lease tokens/epochs fence stale transitions. Expiry and durable retry recover
  interrupted work; exhausted poison work remains visible for operator repair.
- Published migration bytes remain immutable. Schema, HTTP API, event schema and
  Go module versions are distinct. Upgrade order and guarded downgrade behavior
  remain documented and tested; binary rollback is not schema downgrade.
- Kafka and Inbox retention must cover supported redelivery/replay/restore windows.
  Deleting deduplication state shortens that guarantee.

Retain the detailed [compatibility](COMPATIBILITY.md), [delivery](DELIVERY_GUARANTEES.md),
[ordering](ORDERED_DELIVERY.md) and [consumer](CONSUMER_RELIABILITY.md) contracts.
Before publication reconcile their pre-1.0 version-policy text with this accepted
v1 policy; do not leave conflicting current guarantees.

## Operational and support boundaries

The release notes must list the infrastructure versions, topology, architecture
and security configuration actually qualified for the candidate. Current
qualification evidence covers PostgreSQL16 and Kafka4.3.1 on Linux/amd64; it does
not qualify every supported upstream release, every operating system with a
compiled binary, HA configuration, credential rotation or managed cloud service.
Unsupported/unqualified combinations must be explicit, not inferred from a
successful constructor or build.

Capacity is deployment-dependent. Publish admitted rates, lag/recovery behavior,
retained-row sizes, resource limits, retry policy and operator response assumptions
with evidence. No global50events/second promise follows from the24-hour test.
Latency/lag and recovery objectives belong to a tested operating profile.

For the initial v1 line, security and correctness fixes target the latest supported
v1 minor release. Older v1 minors may require an upgrade; no LTS/backport window is
promised. Disclose any exceptional backport explicitly. Keep the private security
reporting channel and identify affected versions when reporting a vulnerability.
This policy does not promise an unstaffed response-time or availability SLA.

The publication decision must confirm that these boundaries, upgrade instructions,
release notes and package/image metadata agree for the exact candidate.
