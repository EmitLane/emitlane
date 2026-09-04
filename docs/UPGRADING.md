# Upgrading EmitLane

## v0.1.0 to v0.2.0

Back up PostgreSQL and test the upgrade with production-like row counts. Apply
the additive migration before starting a v0.2 binary:

```bash
emitlane migrate up
emitlane doctor
```

Migration v2 leaves all v1 outbox/inbox rows and status semantics intact. It
adds nullable replay provenance, runtime control, relay presence, audit tables,
and operational indexes. It does not rewrite payloads or IDs. The released
`000001_init` migration is unchanged.

A v0.1 writer and relay can use a database after the additive v2 migration, but
v0.1 relays ignore cluster pause and do not heartbeat. Complete the relay rollout
before using pause as an incident-control guarantee. v0.2 binaries require schema
version 2 and fail readiness/startup against an older schema.

The Admin API remains disabled unless explicitly enabled. Review its bind and
token configuration before rollout. Existing delivery stays at least once;
v0.2 replay creates new event identities and does not change Inbox semantics.

New v0.2 environment variables and defaults are:

```text
EMITLANE_CONTROL_CHECK_INTERVAL=2s
EMITLANE_RELAY_HEARTBEAT_INTERVAL=10s
EMITLANE_RELAY_STALE_AFTER=30s
EMITLANE_ADMIN_ENABLED=false
EMITLANE_ADMIN_ADDR=127.0.0.1:8081
EMITLANE_ADMIN_TOKEN=
EMITLANE_ADMIN_EXPOSE_PAYLOAD=false
```

Set `EMITLANE_INSTANCE_ID` to a stable, unique value per relay process when the
runtime platform does not provide a stable hostname. If omitted, EmitLane
generates a process-local ID at startup. Keep `EMITLANE_RELAY_STALE_AFTER` comfortably
above the heartbeat interval; the defaults allow two missed heartbeats before a
relay is reported stale.

Rollback of only migration v2 removes v2 indexes/tables/columns and returns the
recorded schema version to 1. Do not roll it back after relying on audit or replay
provenance without first exporting the operational data you need.

## v0.2.0 to v0.3.0

Migration v3 is additive. It adds nullable ordering columns, durable stream and
64-partition tables, indexes/constraints, and an `ordering_capable` presence
flag. Existing v0.2 writers and unordered events remain valid.

Use this order:

1. Back up and run `emitlane migrate up` to schema 3.
2. Upgrade every Relay instance to v0.3.
3. Run `emitlane doctor` and `emitlane ordering partitions`; verify all 64
   partitions have valid v0.3 ownership or are in a temporary handoff.
4. Only then deploy producers that write ordered events.

Schema v3 includes a database claim guard that prevents a released v0.2 Relay
from claiming ordered rows. This fails safe by leaving them pending. Do not
enable ordered producers during the mixed-Relay window: ordered rows at the
front of an old Relay's bounded candidate set can reduce unordered throughput.

New timing settings and defaults are:

```text
EMITLANE_ORDERING_REBALANCE_INTERVAL=2s
EMITLANE_ORDERING_LEASE_DURATION=30s
EMITLANE_ORDERING_SAFETY_MARGIN=1s
```

The ordering lease must exceed `EMITLANE_PUBLISH_TIMEOUT` plus the safety
margin, and the rebalance interval must be shorter than the ordering lease.

Migration v3 cannot be rolled back after ordered state exists. Its down
migration returns a clear error while any ordered stream or ordered outbox row
remains; it never silently discards the durable sequence cursor.
