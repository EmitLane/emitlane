# Failure modes

Every critical path is designed around process death. Prefer a duplicate over
silent loss.

## Crash before claim commit

The claim transaction rolls back. The event stays `pending`. Another relay can
claim it. The event is not lost.

## Crash after claim, before Kafka publish

The row is `inflight` with an expiring lease. When `lease_until` passes, another
worker claims it (claim selects expired inflight rows directly). The event is
eventually delivered. A claim by itself does not increment the publish-attempt
counter. No manual recovery is required.

## Crash while the broker is processing

The relay may not know whether Kafka accepted the record. Recovery favors a
possible duplicate over loss. After the lease expires the event is published
again if it is not yet `delivered`.

Kafka producer idempotence is disabled so the Relay's publish deadline remains
a real application-side bound. Cancelling an attempt does not recall a request
Kafka already accepted; the retry can therefore duplicate it. With no producer
sequence state carried across attempts, a later record cannot be reported as
the ambiguous earlier sequence while silently disappearing.

## Crash after Kafka ACK, before `delivered`

Kafka may already have the record. The database still shows `inflight` or, after
expiry, claimable work. The next publish is a **duplicate**. Inbox (or downstream
idempotency) must handle it. The event is never silently discarded.

## Kafka outage

Publish fails. The relay schedules retry: `status=pending`, `available_at` in the
future, lease cleared, `last_error` stored (truncated to 4 KiB). Exponential
backoff with full jitter avoids a thundering herd. After `max_attempts` the event
becomes `dead`. Events remain durable in PostgreSQL for the whole outage.
Pause/unpause and actual same-container stop/start are separate regression
scenarios because they exercise different connection and broker recovery paths.

## Permanent broker errors

Invalid topic configuration, authorization failures, and similar errors are
classified as non-retryable. The event is marked `dead` instead of retrying
forever.

## PostgreSQL outage

The relay cannot claim or acknowledge work. It must not publish unclaimed events.
Readiness fails. When PostgreSQL returns, polling resumes. Leases that expired
during the outage are reclaimable.

## Expired lease / worker death

A dead instance does not hold PostgreSQL row locks after commit. Survivors claim
expired inflight rows. Valid leases are not stolen.

## Poison event

Repeated retryable failures increment `attempts` until `max_attempts`, then
`status=dead` with `last_error`. Operators can inspect with `emitlane dead list`
and re-queue with `emitlane dead retry <event-id>`.

## Duplicate consumer delivery

At-least-once delivery plus the ACK/crash window can present the same event
twice. For the same `consumer` + `event_id`, Inbox runs the callback once.
Different consumers process independently.

## Lost or coalesced NOTIFY

Notifications are not the queue. If LISTEN drops, the payload is missing, or
NOTIFY is folded, the poll interval still finds due rows. Notification failure
does not stop delivery.

## Shutdown (SIGTERM / SIGINT)

The process stops claiming new work, shuts down HTTP, and waits a bounded time
for in-flight publishes. It does not mark interrupted work `delivered`. Leftover
inflight rows recover via leases.
