# Admin API v1

The Admin API is a separate operational listener. It is disabled by default and
does not replace `/healthz`, `/readyz`, or `/metrics`.

## Safe configuration

```bash
EMITLANE_ADMIN_ENABLED=true
EMITLANE_ADMIN_ADDR=127.0.0.1:8081
EMITLANE_ADMIN_TOKEN=
EMITLANE_ADMIN_EXPOSE_PAYLOAD=false
```

An unauthenticated listener is allowed only on an explicit loopback address.
Wildcard and non-loopback addresses require `EMITLANE_ADMIN_TOKEN`; startup
fails otherwise. Remote requests use `Authorization: Bearer <token>`. Tokens
are compared in constant time after hashing and are never logged.

Every response includes a bounded `X-Request-ID`. A valid caller-supplied value
is reused; otherwise EmitLane generates one. Mutation audit rows contain the
same request ID.

## Endpoints

- `GET /v1/stats`: durable queue, relay, ordered stream, and partition counts.
- `GET /v1/events`: redacted, keyset-paginated event list.
- `GET /v1/events/{id}`: redacted event inspection.
- `GET /v1/relays`: active, stale, and stopped relay instances.
- `GET /v1/relays/{id}`: one relay instance.
- `GET /v1/relay`: persisted cluster control state.
- `POST /v1/relay/pause` and `/resume`: idempotent audited control changes.
- `GET /v1/audit`: keyset-paginated mutation audit.
- `POST /v1/events/{id}/retry`: same-ID retry of a dead event.
- `POST /v1/events/{id}/replay`: new-ID replay of delivered/dead content.
- `POST /v1/replays/preview`: advisory batch selection preview.
- `POST /v1/replays`: atomic bounded batch replay.
- `GET /v1/ordering/streams`: cursor-paginated stream states; filters are
  `state`, `destination`, `partition`, and `blocked_only`.
- `GET /v1/ordering/stream?destination=...&ordering_key=...`: one stream with
  expected/future sequence, gap, attempts, and partition metadata.
- `GET /v1/ordering/partitions`: all 64 desired/actual owners, epochs, leases,
  handoff barriers, and computed ownership states.

Event filters are `status`, `destination`, `event_type`, `created_after`,
`created_before`, `replay_batch_id`, `limit`, and `cursor`. Page size defaults
to 50 and cannot exceed 200. Treat cursors as opaque.

## Payload safety

Lists and ordinary inspection omit payload, key, and headers. If
`EMITLANE_ADMIN_EXPOSE_PAYLOAD=true`, an operator can request
`GET /v1/events/{id}?payload=true`; binary values are base64 encoded. This flag
should remain off unless an incident procedure explicitly needs content access.

## Mutations

Pause/resume and retry accept `{"reason":"..."}`. Replay requires a non-empty
reason. Batch replay accepts:

```json
{
  "destination": "orders.events",
  "event_type": "order.created",
  "created_after": "2026-09-02T00:00:00Z",
  "created_before": "2026-09-02T01:00:00Z",
  "statuses": ["delivered"],
  "limit": 1000,
  "reason": "consumer incident"
}
```

The filter must be non-empty. Omitted status means `delivered`; `dead` must be
explicit. Execution rejects a selection larger than its limit and never
silently truncates it. The hard maximum is 1000.

Errors use a stable JSON envelope:

```json
{"error":{"code":"invalid_request","message":"...","request_id":"..."}}
```

Raw SQL errors and secrets are not returned. The complete machine-readable
contract is [OpenAPI](openapi/admin-v1.yaml).

Ordered event inspection adds key, sequence, and virtual partition metadata but
still hides payload. Replay of an ordered source returns `409` unless the body
contains `"ordering_mode":"unordered"`; the resulting clone is deliberately
outside the historical stream.
