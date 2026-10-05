# Repeated reliability qualification

`TestManagedConsumerQualificationSoak` is an opt-in PostgreSQL/Kafka integration
profile. It runs repeated fault cycles under a continuous mixed ordered/unordered
outbox load. The existing three-minute managed regression remains separate.

## Run

Run on an isolated test host. The test stops/restarts **its own** containers and
kills **its own** helper processes. Serialize heavy jobs and use an aggregate
CPU/memory limit for the runner and Docker containers. No shared databases or
application containers belong in this fixture.

Start with a short trial:

```sh
GOMAXPROCS=2 GOMEMLIMIT=1536MiB \
EMITLANE_QUALIFICATION_DURATION=5m \
EMITLANE_QUALIFICATION_RATE=10 \
EMITLANE_QUALIFICATION_SEED=20261005 \
EMITLANE_QUALIFICATION_OUTPUT=/absolute/new/results-directory \
go test -p=1 -tags=integration -run '^TestManagedConsumerQualificationSoak$' \
  -count=1 -v -timeout=20m ./internal/integration
```

The source must be committed and clean. Use a fresh evidence directory for each
run; an existing `events.jsonl` is rejected. Keep stdout/stderr in a durable log
and launch through a supervisor that survives SSH disconnects, samples aggregate
resources, records source identity and exit status, and stops its process group
and helper children on timeout. Avoid unbounded `go test` output buffers.

For 24h/72h runs set the duration to `24h`/`72h`, and the Go test/supervisor
deadline to `25h`/`73h`. The default injection interval is the smaller of five
minutes and one third of the requested duration. An optional
`EMITLANE_QUALIFICATION_INTERVAL` accepts 30s through half the duration. Rate is
1–50 events/s; the default 10 is intended for a small shared test host. Seed is
a signed integer and reproducibly shuffles each full cycle's fault order. IDs,
broker timing and actual scheduling remain nondeterministic.

## Coverage and assertions

Every completed cycle includes:

- SIGKILL of a consumer with an uncommitted business transaction;
- SIGKILL of a Relay after durable claim and before publishing;
- SIGKILL of a Relay after Kafka ACK and before database acknowledgement;
- consumer membership join/leave while traffic continues;
- actual stop/start of Kafka and PostgreSQL;
- audited pause/resume, including a newly committed row that must stay pending;
- poison handler retry exhaustion, operator repair and audited Inbox retry;
- bounded delivered-row retention with durable proof of prior delivered state.

Consumer joins require an observed partition assignment. Recovery must also
drain group offset lag to zero. Offset commit failure is re-armed every cycle. Transient handler failures are
injected throughout the load. At least two full cycles, two observed offset
commit failures, poison repair and actual deletion are required for PASS.

Half the normal load uses eight ordered streams. Sequence allocation belongs
to the producer transaction, including through ambiguous database commits.
The consumer checks stream progress inside the same transaction as its Inbox
transition and business effect. Failed transactions must not advance progress.

After a bounded recovery period, an independent consumer reads **all snapshot
offsets** in batches of at most 250. Expected IDs come from the producer business
transaction; observed IDs persist in PostgreSQL. No slice/map grows with the
total number of Kafka records. Checks include exact ID, key, payload, seed,
sequence, duplicate count, stream continuity and partition affinity. The final
database audit rejects missing, substituted and extra identities/effects even
when aggregate counts match. A deleted outbox row needs prior delivered proof;
its business transaction, Inbox state and Kafka observation must still agree.

Duplicates are reported and permitted by at-least-once delivery. Protected
PostgreSQL effects must occur once per event through Inbox. This does not claim
end-to-end exactly-once delivery or protection of external side effects.

## Evidence

`events.jsonl` contains provenance/configuration, fault start/end, operator
actions, and per-cycle snapshots of heap/goroutines, pool/database connections,
database size, estimated dead tuples, autovacuum count and queue depth/age.
Kafka end/committed offsets and lag are sampled at the same healthy checkpoints.
Helper logs are stored beside it. A final `PASS` record contains full coverage,
ID counts, duplicates, retention/retry totals and the integrity report.

```sh
tail -f /absolute/results-directory/events.jsonl
```

Missing PASS, an `incomplete` record, a nonzero supervisor/test exit, or a source
change is failed/incomplete evidence. Interruption may prevent any terminal
record; the supervisor's durable status is authoritative for process completion.
Inspect trend data; the profile does not infer a memory leak threshold from a
short trial or treat normal database growth as a leak.

## Remaining qualification

This fixture uses PostgreSQL 16 and a **single** Kafka 4.3.1 broker with the
integration environment's default seven-day retention. Supported duration is
capped at 72h; expected/Inbox/audit ledgers intentionally remain on disk for
identity verification. Multi-broker leader/ISR/network faults, the supported
version matrix, mixed-version upgrades and production-size restore/PITR are
separate gates. A five-minute PASS is preparation evidence, not a passed 24h or
72h stability gate. Do not start long runs before the short trial passes.
