# Integrity verification

EmitLane v0.4 verifies that the durable PostgreSQL state used by the outbox,
ordered-delivery, leasing, and runtime-control protocols is internally
consistent. Verification is diagnostic and read-only: it never advances a
stream, retries or deletes an event, changes a lease, or repairs data.

Integrity verification does not prove exactly-once delivery, absence of valid
at-least-once duplicates, Kafka retention, consumer processing, or downstream
side effects. A Kafka acknowledgement followed by a Relay crash before the
PostgreSQL delivered transition can still produce a duplicate.

## Modes

- `summary` performs bounded aggregate and protocol-shape checks. It is the
  default for `emitlane integrity check` and the Admin API.
- `full` verifies every retained active ordered event and durable stream cursor.
  It is selected with `emitlane integrity check --full`.
- `stream` inspects one `(destination, ordering_key)` and reports its cursor,
  retained relevant events, blocking condition, deterministic partition,
  ownership, epoch, lease, and handoff state.

`emitlane doctor` answers whether an installation has the schema, permissions,
and dependencies required to run. Integrity verification instead answers
whether the existing durable runtime state is coherent.

## Snapshot and safety contract

Full and summary checks run in one PostgreSQL `REPEATABLE READ`, `READ ONLY`
transaction with a caller-bounded context and a bounded statement timeout.
They take no row locks and perform no writes. The snapshot prevents normal
Relay transitions between verifier queries from being reported as corruption.

The snapshot remains pinned for the duration of a check. A long-running check
can delay PostgreSQL vacuum cleanup of row versions, so operators should keep
timeouts short, avoid unbounded result collection, and schedule expensive full
checks with regard to database load. Finding details are bounded while summary
counters cover the complete checked scope. Queries never select event payloads
or headers.

## Severity

- `violation` means durable state that should be impossible when EmitLane's
  protocol invariants hold, such as a wrong deterministic partition or an
  active event below a durable stream cursor.
- `warning` means valid but attention-worthy state, such as a sequence gap,
  dead-blocked stream, retry wait, expired event lease, stale Relay presence, or
  overdue handoff.
- `info` is expected state useful during diagnosis.

Warnings do not imply corruption. In particular, expected epoch and ownership
fencing races are not violations.

## Stable finding codes

Finding codes are a fixed automation contract. New codes may be added in later
releases; existing meanings must not be changed silently.

| Code | Severity | Meaning |
| --- | --- | --- |
| `SCHEMA_VERSION_MISMATCH` | violation | Required v3 schema objects or protocol shape are missing or incompatible. |
| `PARTITION_COUNT_INVALID` | violation | The durable virtual-partition set is not exactly the 64 protocol partitions. |
| `PARTITION_ID_INVALID` | violation | A required partition ID is missing or an unexpected ID exists. |
| `PARTITION_EPOCH_INVALID` | violation | A partition epoch is negative. |
| `PARTITION_LEASE_SHAPE_INVALID` | violation | Partition owner, lease, handoff, or publish-window fields form an impossible combination. |
| `PARTITION_OWNER_STALE` | warning | A live partition lease names a Relay whose presence is stopped, stale, absent, or not ordering-capable. |
| `PARTITION_HANDOFF_OVERDUE` | warning | A handoff barrier is materially overdue while work remains blocked. |
| `ORDERING_FIELDS_PARTIAL` | violation | An outbox row contains only part of the required ordering metadata. |
| `ORDERING_PARTITION_MISMATCH` | violation | An ordered event's partition differs from the v0.3 deterministic mapping. |
| `STREAM_PARTITION_MISMATCH` | violation | A stream's partition differs from the v0.3 deterministic mapping. |
| `STREAM_CURSOR_INVALID` | violation | A stream cursor is non-positive or precedes its start sequence. |
| `ACTIVE_SEQUENCE_BEHIND_CURSOR` | violation | A retained pending, inflight, or dead ordered event is below `next_sequence`. |
| `EXPECTED_SEQUENCE_DUPLICATE` | violation | More than one retained active event occupies the expected sequence. |
| `STREAM_GAP` | warning | The expected active sequence is absent while a future active sequence exists. |
| `STREAM_DEAD_BLOCKED` | warning | The expected sequence is dead and blocks later progress. |
| `STREAM_RETRY_WAIT` | warning | The expected pending sequence is scheduled for a future retry. |
| `STALE_EVENT_LEASE` | warning | An inflight event lease has expired and awaits recovery. |
| `RUNTIME_CONTROL_INVALID` | violation | The runtime-control singleton is missing, duplicated, or structurally invalid. |
| `RELAY_PRESENCE_STALE` | warning | A non-stopped Relay presence heartbeat is older than the configured threshold. |

## Retention-aware cursor rules

Delivered-event retention is valid. If a stream has `next_sequence = 100`, the
absence of delivered rows 1 through 99 is not a gap or violation. A retained
delivered row below the cursor is also valid. Verification reasons from the
durable cursor, retained protocol metadata, and retained active
`pending`/`inflight`/`dead` rows. An active ordered row below the cursor is a
violation because Relay could otherwise regress the stream.

For the event at `next_sequence`, normal states are:

- `ready`: pending and currently due;
- `inflight`: leased and not expired;
- `retry_wait`: pending with a future `available_at`;
- `dead_blocked`: dead at the expected sequence;
- `gap`: no active expected event exists while a future active event does.

The verifier observes these states and never skips a sequence.

## Deterministic partition contract

The v0.3 protocol mapping is unchanged:

```text
FNV-1a-64(destination + NUL + ordering_key) % 64
```

Both retained ordered events and durable stream rows are verified against this
mapping. Changing the partition count, hash, separator, or existing vectors is
outside v0.4 and requires a repartitioning design.

## Result and exit contract

A report contains timestamps, mode, result, complete summary counters, bounded
findings, and a `findings_truncated` flag. Findings are emitted in deterministic
order where practical and never include payloads, raw headers, credentials, or
tokens.

CLI exit status is stable:

- `0`: the check completed without invariant violations;
- `1`: the check could not be completed;
- `2`: at least one invariant violation was detected.

Warnings keep the default exit status at zero. With `--strict`, one or more
warnings produce status 2.

## Operator commands

Set `EMITLANE_DATABASE_URL` to a role with `SELECT` on the EmitLane schema. No
write privilege, Kafka connection, or superuser access is required.

```bash
# Fast, bounded production check.
emitlane integrity check

# Machine-readable check suitable for automation.
emitlane integrity check --json --timeout 20s --max-findings 50

# Complete scan of retained active ordering state.
emitlane integrity check --full --json --timeout 2m

# One stream, including its cursor, retained relevant events and partition state.
emitlane integrity stream \
  --destination orders.events --key order:123 --json
```

`--max-findings` bounds detailed findings, not the summary counters. Its allowed
range is 1–1000 and its default is 100. The CLI overall timeout also becomes the
PostgreSQL statement timeout. Prefer summary mode for frequent automation. Run
full mode during incidents, before release, and off peak on large databases.

The authenticated Admin API exposes `GET /v1/integrity` and
`GET /v1/integrity?mode=summary`. It fixes a five-second statement timeout and
returns at most 50 findings. `mode=full` is rejected: an HTTP request must not
start an unexpectedly expensive scan. The integrity endpoint is diagnostic and
is not part of `/healthz` or `/readyz`; warnings and durable corruption must not
cause orchestrators to restart a healthy Relay loop.

## Fencing diagnostics

An epoch or ownership check can legitimately reject stale work during graceful
handoff, crash takeover, or lease expiry. Relay reports this typed `ErrFenced`
outcome at debug level with the event ID, Relay instance, claimed epoch,
partition, and operation (`begin_attempt`, `retry`, `dead`, or `delivered`). It
does not log payload, key, or headers, and it does not count the outcome as an
ordinary SQL failure.

Use these bounded metrics to correlate integrity reports with runtime behavior:

```text
emitlane_ordering_fenced_attempts_total{operation}
emitlane_integrity_checks_total{mode,result}
emitlane_integrity_check_duration_seconds{mode}
```

Expected fencing by itself is not corruption. Repeated fencing with overdue
handoffs, stale owners, or a backlog that does not recover warrants inspection
of Relay membership and partition ownership.

## Incident examples

### A stream stops advancing

Run `integrity stream` for the exact destination and ordering key. A `gap`
warning means the domain producer has not committed the expected sequence;
create that missing event rather than advancing the cursor. `dead_blocked` means
repair and retry the same event identity. `retry_wait` is normally automatic.

### A stale owner or lease appears

Compare the reported owner, epoch, `lease_until`, and `handoff_not_before` with
`emitlane ordering partitions`. An expired event lease is recoverable and is a
warning. Allow another Relay to reclaim it. If recovery does not happen after
the configured lease and handoff windows, inspect Relay presence and database
connectivity.

### A violation is reported

Capture the JSON report and stop manual state mutations. Re-run a targeted
stream check where possible, verify the binary and schema versions, and inspect
recent operator audit records. Violations describe durable state that normal
protocol transitions should not produce. The verifier intentionally performs
no repair; recovery is an explicit operator decision after root-cause analysis.

### Kafka delivery is disputed

Integrity verification cannot establish whether Kafka retained or a consumer
processed a record. Use broker and consumer evidence. A duplicate after broker
ACK and before PostgreSQL acknowledgement is permitted by the at-least-once
contract and should be absorbed by Inbox or downstream idempotency.
