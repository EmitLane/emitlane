# Delivery semantics

The current contract is maintained in these guides:

- [Delivery guarantees](DELIVERY_GUARANTEES.md): atomic producer writes,
  at-least-once publication, attempts, retries, pause, replay and retention.
- [Failure modes](FAILURE_MODES.md): process-death windows, dependency outages,
  lease recovery, duplicates and operator actions.
- [Ordered delivery](ORDERED_DELIVERY.md): per-stream sequencing, epoch fencing
  and bounded publish/handoff assumptions.
- [Consumer reliability](CONSUMER_RELIABILITY.md): Inbox transactions, lease
  tokens, partition blocking and commit-after-processing offsets.

This entry point is retained for existing links. The guides above contain the
maintained detail; EmitLane does not claim end-to-end exactly-once delivery.
