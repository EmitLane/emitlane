DROP INDEX IF EXISTS emitlane.outbox_unordered_pending_idx;

DELETE FROM emitlane.schema_migrations WHERE version = 5;
