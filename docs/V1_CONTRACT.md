# EmitLane v1.0.0 contract

This contract defines the initial v1 support scope for the candidate based on
`d0d19d3662fc78a5318a3ed2732b443391aca725`. It takes effect with the accepted
v1.0.0 release. Published v0.x artifacts retain their pre-1.0 policy. See the
[acceptance record](V1_RELEASE_CHECKLIST.md) for the evidence and open gates.

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

The wire and operator contract includes documented Kafka metadata, Admin HTTP
`/v1` routes, fields, statuses and error codes, CLI flags and machine-readable
output, documented environment settings, and documented metric names, types,
labels and units. See the [Admin OpenAPI](openapi/admin-v1.yaml) and
[compatibility policy](COMPATIBILITY.md) for the specific boundaries. Clients
must tolerate additive fields and headers. Trace propagation uses the documented
W3C headers. Span names, logs and human-readable error text are diagnostic.

## Durable and delivery contracts

- The caller commits the business write and the Outbox insert in the same
  PostgreSQL transaction. A successful enqueue reports the insert.
- PostgreSQL is authoritative. LISTEN/NOTIFY is a wake-up optimization.
- Relay delivery is at least once. An ambiguous or acknowledged publish can be
  redelivered with the same event ID after a crash. Claims commit before Kafka
  I/O. Relay records delivery after broker acknowledgement.
- Managed business effects and Inbox processed state commit together in one local
  PostgreSQL transaction. The managed consumer commits Kafka offsets after that
  transaction. External side effects need application idempotency or another
  transactional Outbox.
- Ordering is opt-in per `(destination, ordering_key)`, subject to the documented
  publish and handoff bounds. The next sequence waits for durable delivery of
  the current one. Gaps and dead expected events require visible recovery.
- Lease tokens/epochs fence stale transitions. Expiry and durable retry recover
  interrupted work; exhausted poison work remains visible for operator repair.
- The candidate uses schema 5 for the pending-unordered claim index and keeps
  published migrations 1–4 immutable. Schema, HTTP API, event schema and Go
  module versions are distinct. v0.9.1 requires schema 4: rollback stops the
  candidate processes and removes only migration 5 before starting the previous
  binaries. Validate the exact maintenance upgrade and rollback procedure.
  State-removing schema downgrades remain separate guarded operations.
- Kafka and Inbox retention must cover supported redelivery/replay/restore windows.
  Deleting deduplication state shortens that guarantee.

See the detailed [delivery](DELIVERY_GUARANTEES.md),
[ordering](ORDERED_DELIVERY.md) and [consumer](CONSUMER_RELIABILITY.md)
contracts. At-least-once delivery permits Kafka redelivery. Transactional Inbox
deduplication applies to local database effects. Global ordering and end-to-end
exactly-once delivery are outside the contract.

## Initial support matrix

The initial runtime scope is Linux/amd64, a single writable PostgreSQL 16
instance, and one Apache Kafka 4.3.1 KRaft broker. This is the target for
release acceptance. The 72-hour result covers the isolated fixture below.
Production deployment requires evidence for the selected security settings,
recovery procedures and application workload.

| Surface | Initial v1 scope | Evidence and remaining qualification |
| --- | --- | --- |
| Platform and compiler | Linux/amd64 runtime. Go 1.27.2 is the candidate build toolchain and the module's minimum Go version. | The managed 72-hour fixture ran on Linux/amd64 with Go 1.27.2. Other runtime platforms need separate evidence. |
| PostgreSQL | PostgreSQL 16, one writable instance, schema 5, durable writes. | The 72-hour fixture used schema 4 with `postgres:16-alpine`, `fsync`, `synchronous_commit` and `full_page_writes` enabled. Migration 5 and its query plans require candidate qualification. PostgreSQL HA, managed services and other major versions are outside the initial scope. |
| Kafka | Apache Kafka 4.3.1, one KRaft broker. | The 72-hour fixture exercised actual broker restart with eight topic partitions. Multi-broker deployment, other Kafka versions and managed services are outside the initial scope. |
| Kafka transport | Production Kafka connections use TLS with broker identity verification. Qualify the exact listener and authentication mode before listing it in the release notes. | The 72-hour fixture used plaintext on loopback. TLS, mTLS, SASL over TLS and credential rotation need separate evidence for any advertised deployment. See [Kafka connection security](KAFKA_SECURITY.md). |
| Upgrade and rollback | Maintenance upgrade from v0.9.1/schema 4 to v1.0.0/schema 5; rollback removes only the new index migration before returning to v0.9.1/schema 4. | Published migrations 1–4 are unchanged. Earlier schema-4 rolling tests are historical. Qualify the final artifacts, index installation on retained data and exact schema-5-to-4 rollback before accepting the path. |
| Recovery | Operator-led PostgreSQL restore/PITR, with Kafka retention and group offset reconciliation before resumption. | The 72-hour run demonstrated recovery from faults in a live fixture. Production-representative restore and resumption remain open. See [Backup and restore](BACKUP_RESTORE.md). |
| Release artifacts | Verify the release archives and Linux/amd64 and Linux/arm64 image builds and startup. | Exact-release artifact checks are open. An arm64 image smoke test covers image startup. Runtime support requires its own deployment evidence. |

Publish workload and resource conditions with the results, including admitted
rate, queue age and lag, recovery time, retained rows and operator response.
Capacity and recovery objectives apply to the profile that measured them.
The initial contract sets no universal events-per-second or per-slot guarantee.

## Maintenance policy

For the initial v1 line, security and correctness fixes target the latest supported
v1 minor release. Older v1 minors may require an upgrade; no LTS/backport window is
promised. Disclose any exceptional backport explicitly. Keep the private security
reporting channel and identify affected versions when reporting a vulnerability.
This policy does not promise an unstaffed response-time or availability SLA.

The publication decision must confirm that the support matrix, upgrade
instructions, release notes and package/image metadata agree for the final SHA.
