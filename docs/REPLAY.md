# Replay safety

> Replay creates a new event identity and can intentionally execute downstream
> business logic again.

Use retry when a dead event should continue the same logical delivery. Retry
keeps its ID, resets attempts, and moves only `dead → pending`.

Use replay when already delivered or dead historical content must be delivered
as a new event. Replay copies routing, opaque payload bytes, content type, user
headers, schema version, correlation, and causation. It creates a new UUIDv7,
resets the delivery lifecycle, and records `replayed_from_event_id` plus a new
`replay_batch_id`. The source row is unchanged.

Kafka records for a replay include authoritative
`emitlane-original-event-id` and `emitlane-replay-batch-id` headers. EmitLane
overrides conflicting user headers. The historical trace is not reused as if
the replay were the original request.

Inbox sees a replay's new ID as new work. Before execution, determine whether
the consumer may send email, charge money, call external APIs, or otherwise
repeat a business effect. Domain-level idempotency may still be required.

Batch replay is preview-first in the CLI. It requires a reason and a meaningful
selector, defaults to delivered sources, allows dead only explicitly, and is
capped at 1000 events. Source selection, clone inserts, and the audit row commit
in one PostgreSQL transaction. A failure cannot leave a partially created,
unaudited batch.

Replayed events retain normal at-least-once semantics. A broker acknowledgement
followed by a relay crash before the delivered update can duplicate a replay.

Replay also depends on the source row still being retained. Configure delivered
retention according to the replay window your operations require; after cleanup
deletes a delivered source, EmitLane cannot reconstruct its payload for replay.
