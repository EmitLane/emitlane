-- Match the pending unordered claim without walking ordered pending rows.
-- The general pending index remains available for other operational queries.
CREATE INDEX outbox_unordered_pending_idx
ON emitlane.outbox_events (available_at, created_at, id)
WHERE status = 'pending' AND ordering_key IS NULL;
