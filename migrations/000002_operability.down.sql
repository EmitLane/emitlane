DROP INDEX IF EXISTS emitlane.admin_audit_created_idx;
DROP TABLE IF EXISTS emitlane.admin_audit_log;
DROP TABLE IF EXISTS emitlane.relay_instances;
DROP TABLE IF EXISTS emitlane.runtime_control;

DROP INDEX IF EXISTS emitlane.outbox_replay_batch_idx;
DROP INDEX IF EXISTS emitlane.outbox_destination_type_created_idx;
DROP INDEX IF EXISTS emitlane.outbox_status_created_idx;
DROP INDEX IF EXISTS emitlane.outbox_created_idx;

ALTER TABLE emitlane.outbox_events
    DROP COLUMN IF EXISTS replay_batch_id,
    DROP COLUMN IF EXISTS replayed_from_event_id;

DELETE FROM emitlane.schema_migrations WHERE version = 2;

