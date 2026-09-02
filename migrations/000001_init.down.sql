-- Remove only objects created by 000001_init.up.sql. Never use CASCADE here:
-- applications may own other objects in the emitlane schema.
DROP TABLE IF EXISTS emitlane.inbox_events;
DROP TABLE IF EXISTS emitlane.outbox_events;
DROP FUNCTION IF EXISTS emitlane.headers_are_strings(JSONB);
DROP TABLE IF EXISTS emitlane.schema_migrations;

-- Drop the namespace when it is empty. If application-owned objects remain,
-- preserve both those objects and their schema.
DO $$
BEGIN
    DROP SCHEMA IF EXISTS emitlane;
EXCEPTION
    WHEN dependent_objects_still_exist THEN
        NULL;
END
$$;
