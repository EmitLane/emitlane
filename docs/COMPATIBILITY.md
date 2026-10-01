# Compatibility contract

This document records the application-facing boundaries reviewed for v0.8.
EmitLane remains **pre-1.0**. This is a compatibility baseline, not a promise
that every exported identifier or every pair of releases is interchangeable.

## Version policy

- Pin a released module version and image tag. Review the changelog and
  [upgrade guide](UPGRADING.md) before upgrading.
- Patch releases should preserve documented source and wire contracts. A
  correctness or security fix can reject previously accepted invalid input;
  its release notes must explain the behavior change.
- Minor releases may change pre-1.0 APIs, defaults, configuration, or schemas.
  Such changes need explicit release notes and upgrade instructions. Additive
  changes are preferred where they preserve the delivery guarantees.
- A database schema version, the Admin API `/v1` prefix, event
  `SchemaVersion`, and the Go module version are separate version domains.
  Event schema versions belong to the application, not to EmitLane releases.
- Compatibility fixtures exercise representative SDK usage. They do not
  establish a complete exported-symbol freeze, mixed-version runtime matrix,
  database downgrade guarantee, or compatibility with every Kafka release.

The delivery contract always remains at least once. A Kafka acknowledgement
followed by a crash before the database acknowledgement can produce a
duplicate. A timeout does not prove that the broker rejected a record.

## Go package boundaries

The module path is `github.com/emitlane/emitlane`. The Go version required by
the selected release is declared in its `go.mod`.

- `outbox`: producer SDK (`Event`, `Writer`, `JSON`, options, errors). The
  caller supplies and owns a `pgx/v5` transaction. Successful `Enqueue` means
  the insert completed inside that transaction; the caller must still commit.
- `inbox`: transaction-scoped deduplication and managed lifecycle types.
  `Process` and `ProcessStrict` use a savepoint, while the caller owns the outer
  transaction. Managed `Store` transitions must preserve lease-token fences.
- `consumer`: managed runtime, handler, identity resolver, and broker-neutral
  source interfaces. The runtime owns the handler transaction; handlers must
  neither commit nor roll it back. External effects need their own idempotency.
- `broker`: synchronous publish port and envelope. `broker/kafka` supplies
  the publisher, consumer factory, and shared TLS/SASL configuration. Kafka
  records are not part of the business-handler API.
- `relay`: embedded runner and durable storage ports. `relay.Event` represents
  claimed storage state, not a producer envelope. Optional capability
  interfaces add ordered delivery, presence, and control support; a custom
  basic `Store` is not automatically an ordered store.
- `storage/postgres`: supplied PostgreSQL stores, migrations, and notification
  listener. Pools remain caller-owned. Exported methods whose signatures use
  `internal/admin` types are implementation glue for the binary's operational
  API, not a supported external Go administration SDK. Use the HTTP API or CLI.
- `integrity`: read-only verifier, reports, finding codes, and stream
  inspection. `telemetry`: metrics and W3C trace propagation integration.
- `config` loads the binary's environment configuration; it is importable but
  is coupled to CLI configuration. Prefer package-specific configuration for
  embedded applications. `migrations` embeds SQL; use `postgres.MigrateUp`
  instead of executing embedded files independently.

`internal/*`, `cmd/*`, examples, benchmark tools, and fault-injection hooks
are not general application SDKs. Use keyed struct literals, constructor
options, and default configuration helpers; avoid depending on struct layout,
unexported state, exact error wording, or undocumented defaults.

## Errors and classification

Use `errors.Is` for exported sentinels; errors may be wrapped:

- `outbox.ErrInvalidEvent`, `ErrDuplicateSequence`, `ErrOrderingConflict`,
  `ErrSequenceAlreadyPassed`.
- `inbox.ErrAlreadyProcessed`, `ErrInvalidRequest`, `ErrLifecycleConflict`,
  `ErrLeaseLost`, `ErrNotFound`.
- `broker.ErrPermanent`, also exposed through `broker.IsPermanent`.
- `relay.ErrFenced` and `integrity.ErrStreamNotFound`.
- `postgres.ErrSchemaIncompatible` for an unknown or inconsistent migration
  history. `SchemaVersion` accepts a contiguous older known history; that alone
  does not mean the current runtime can run before pending migrations apply.

`inbox.Permanent(err)` explicitly marks a non-retryable handler failure;
classify it with `inbox.IsPermanent`, rather than a concrete wrapper type.
`Process` returns success for an already processed identity; `ProcessStrict`
returns `ErrAlreadyProcessed`. Lease loss requires rolling back the stale
handler's protected writes, not retrying that transaction's commit.

There is no universal typed configuration, protocol, or shutdown error yet.
Do not parse those error strings as codes. Underlying database/network errors
may be wrapped; security redaction can intentionally replace sensitive error
details. A non-nil operational error and a completed integrity report with
findings are different outcomes. `integrity.ExitCode` maps reports to CLI
status; it does not classify database failures.

## Lifecycle, cancellation, and ownership

Create one relay/runtime per intended lifecycle and call `Run` once. Concurrent
or repeated `Run` on the same object is not a supported lifecycle contract.
Cancel its context to request shutdown, and wait for `Run` before releasing
shared dependencies. The relay does not close its publisher or database pool.
The managed consumer closes the sources it creates; the application closes
the pool after workers have stopped.

Relay cancellation stops new claims and permits already-started work to drain
for `ShutdownTimeout`, then cancels that work. Its normal stop returns `nil`.
The managed consumer cancels handlers and normally returns the context error
when stopped; callers can use `errors.Is(err, context.Canceled)`. Always
propagate contexts into custom publishers, stores, sources, and handlers.

`ShutdownTimeout` is not a guaranteed wall-clock upper bound for all cleanup
and adapter behavior. A handler ignoring cancellation can outlive the managed
runtime's shutdown-timeout return; some error/initialization paths wait for
workers directly. Relay cleanup has additional bounded operations. Go cannot
forcibly terminate an uncooperative callback. Do not close shared dependencies
under a callback that is still running.

Input buffers are borrowed for synchronous calls, not transferred:

- Keep `outbox.Event.Key`, `Payload`, and `Headers` unchanged until `Enqueue`
  returns. The writer serializes headers and passes payload/key to pgx during
  the call; it does not offer a concurrent-mutation guarantee.
- Keep `broker.Message` slices and map unchanged until `Publish` returns.
  Custom publishers must finish their use of borrowed data before returning,
  or make their own copies. Cancellation still permits ambiguous delivery.
- Kafka polling copies key, payload, and header values into `SourceRecord`.
  The runtime passes these buffers to the resolver and handler without an
  additional deep copy. Treat them as read-only for the callback duration;
  copy data needed by asynchronous work. Custom sources must keep returned
  buffers valid throughout processing. `AdapterToken` remains adapter-private.

Kafka constructors snapshot security files and the consumer factory copies
broker/topic lists. Recreate clients/factories to rotate certificates or
passwords; modifying the original configuration does not reload credentials.
See [Kafka security](KAFKA_SECURITY.md).

## Kafka wire metadata

Payload stays opaque bytes; EmitLane does not wrap it in a JSON envelope.
The destination is the Kafka topic. Ordered events use their ordering key as
the Kafka key. Consumers must tolerate additional headers.

Published records carry the lowercase headers `emitlane-event-id`,
`emitlane-event-type`, `emitlane-schema-version`, and `emitlane-attempt`.
Schema version and attempt are decimal strings. Replay adds original-event
and replay-batch IDs; ordered events add ordering-key, sequence, and virtual
ordering-partition metadata. The virtual partition is not a Kafka partition.
Correlation, causation, and W3C `traceparent`/`tracestate` are optional.
`ContentType` is stored in the outbox but is not currently emitted as a
dedicated Kafka header. W3C baggage is not durably persisted by the writer.

The relay owns its named identity, attempt, schema, replay, ordering,
correlation, causation, and trace headers. In v0.8 it removes user-supplied
copies of these reserved names regardless of case or surrounding whitespace,
then writes authoritative durable metadata. The unordered replay provenance
headers `emitlane-original-ordering-key` and `emitlane-original-sequence` are
persisted in the header map by the operator replay path and are preserved; they
are not reconstructed from the new unordered event. Other custom headers are preserved;
the entire `emitlane-*` prefix is not filtered. Applications should nevertheless
use their own namespace to avoid collisions with future metadata names.

**Upgrading from v0.7:** user headers cannot override reserved metadata via case
variants or whitespace. Correlation/causation values must come from the event's
`CorrelationID`/`CausationID` fields; spoofed header values are removed even when
those fields are empty. Do not depend on the old pass-through behavior.

The default consumer identity resolver ignores header-name case and surrounding
whitespace. Conflicting UUID identity headers from other producers block
processing instead of silently choosing one.
The resolver never derives an event identity from the key or payload. Custom
producer formats require an explicit `IdentityResolver` with a stable identity.

The publish map cannot represent duplicate header names and does not promise
header order. The consumer `[]Header` preserves broker header order and
duplicates. Neither a publish attempt number nor a Kafka offset replaces the
stable event ID for business deduplication.

## CLI, Admin API, and observability

Use documented CLI flags and `--json` where offered. Human-readable tables,
logs, help spacing, and prose errors are not parsing interfaces. JSON readers
should tolerate added fields and use documented codes and values. Integrity
checks return 0 for an acceptable completed report, 2 for violations (or
warnings under `--strict`), and 1 for operational errors; do not generalize
those meanings to every CLI command.

The Admin API's externally consumed contract is
[OpenAPI](openapi/admin-v1.yaml), with operational behavior in
[ADMIN_API.md](ADMIN_API.md). Preserve documented routes, field meanings,
HTTP statuses, and error codes when extending `/v1`. Treat cursors as opaque;
error messages are diagnostic text. An API version prefix does not establish
source compatibility for the internal Go implementation.

Metrics names, types, label names, and documented units are integration
surfaces for dashboards and alerts. Additive instruments are allowed; changes
to existing semantics require release notes. Do not treat counter values as
proof of database commits: the enqueue counter counts successful inserts in
caller-owned transactions, which can later roll back. See
[observability](07-OBSERVABILITY-OPERABILITY.md). Span names and attributes
remain pre-1.0; use W3C trace context for propagation rather than log parsing.

## Verification boundary

`internal/compatibility` uses an external test package and imports only public
EmitLane packages. It compiles representative producer, consumer, relay,
PostgreSQL, security, and telemetry composition and runs network-free usage
checks. This catches selected source-contract regressions; it does not replace
integration, migration, restart, or release qualification tests. Before merging
a behavior change, still review crash windows, duplicates, lease fences,
observability, and recovery as required by the project delivery contract.
