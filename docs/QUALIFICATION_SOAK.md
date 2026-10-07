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

Qualification explicitly runs PostgreSQL with `fsync=on`,
`synchronous_commit=on` and `full_page_writes=on`, verified at startup. Earlier
qualification used the PostgreSQL testcontainers module default `fsync=off`;
its performance results do not establish throughput with durable disk writes.
Other integration fixtures retain their existing settings.

The shared fixture uses `apache/kafka:4.3.1` (JVM), a 256–512 MiB Java heap and
a 1.5 GiB container memory ceiling. The runner and containers still need an
aggregate resource limit. The native image is excluded because its startup
runtime segfaulted on the 46th restart during a qualification run. Do not hide
startup failures with automatic container retries.

To check repeated startup separately, including preservation of all acknowledged
records and successful publishing after every restart:

```sh
GOMAXPROCS=2 GOMEMLIMIT=1536MiB EMITLANE_KAFKA_RESTART_CYCLES=50 \
go test -p=1 -tags=integration -run '^TestKafkaRepeatedRestart$' \
  -count=1 -v -timeout=20m ./internal/integration
```

This regression accepts 2–100 restarts and is skipped unless explicitly enabled.
It does not replace the EmitLane fault/recovery profile or a 24h/72h run.

The source must be committed and clean. Use a fresh evidence directory for each
run; an existing `events.jsonl` is rejected. Keep stdout/stderr in a durable log
and launch through a supervisor that survives SSH disconnects, samples aggregate
resources, records source identity and exit status, and stops its process group
and helper children on timeout. Avoid unbounded `go test` output buffers.

For 24h/72h runs set the duration to `24h`/`72h`, and the Go test/supervisor
deadline to `25h`/`73h` with the default recovery/audit budgets. The default injection interval is the smaller of five
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

## Diagnosing sustained load and recovery

Every qualification now writes `diagnostics.jsonl` at approximately 30-second
intervals. Sampling uses one separate PostgreSQL connection with a five-second
sample deadline; it does not borrow from the workload pool. Its small overhead
and PostgreSQL statement/I/O instrumentation are part of the measured profile.
The workload retains its existing pool default and two Relay/consumer workers.

Evidence includes:

- Kafka committed/end offsets and lag for each partition;
- cumulative consumer outcomes, retry/paused reasons and processing histograms,
  Relay metrics, and interval handler/offset-commit timings;
- pgx query count/error/total/max duration by bounded SQL shape, without bind
  parameters, and cumulative workload pool acquisition/wait/cancellation counts;
- PostgreSQL wait events, table/vacuum statistics, I/O/cache/temp-file counters
  and the twenty statements with greatest cumulative execution time;
- explicit durable-write settings and timestamped partial-sample errors during
  injected outages. Statistics can reset across PostgreSQL restarts.

`runtime.jsonl` retains runtime logs. CPU profiles cover fifteen seconds at
startup and roughly once per hour; terminal heap, goroutine, block and mutex
profiles use standard Go pprof. No debug listener or public port is opened.
Client query timings cover pgx Query/Exec tracing, not untraced batch/COPY calls.
Metric histograms are cumulative and bucketed, not exact percentile samples.

Recovery checks offset progress every five seconds and continues poison repair
between checks. The full expected/Inbox/Outbox join runs when lag reaches zero
or at the recovery deadline, rather than repeatedly during catch-up. The final
independent identity, ordering, business-effect and full integrity audits remain
mandatory. `recovery_begin`, `recovery_progress`, `recovery_end` and the final PASS
retain load-end lag and recovery duration. A successful recovery after a growing
load-time backlog does **not** establish sustained throughput at the offered rate.

`EMITLANE_QUALIFICATION_RECOVERY` accepts 1m–4h (default 2m).
`EMITLANE_QUALIFICATION_AUDIT` accepts 1m–1h (default 10m). For a 24h diagnostic
run at 50/s, use recovery 2h, audit 30m and a 27h Go/supervisor deadline; the
remaining time covers setup, failure preservation and teardown. Budgets are
recorded in evidence. Increasing them does not fix an unstable processing rate.

On a normally handled test failure, after producers/consumers/Relays stop and
before fixture teardown, the test writes a private `failure-database.dump`
(pg_dump custom format), the recovery query's EXPLAIN plan, and a compressed
`failure-kafka.tar.gz` containing the stopped fixture broker's log directory
and KRaft metadata. `fixture.json` identifies only those test containers. A
`.partial` filename or recorded archive error is incomplete evidence. A hard
process kill or a failed/unavailable container can prevent preservation; never
claim a backup exists merely because capture was attempted. Containers are
still cleaned up; no application database or container is touched.

Validate preservation separately before launching a long run:

```sh
EMITLANE_QUALIFICATION_DIAGNOSTICS_CHECK=1 \
go test -p=1 -tags=integration -run '^TestQualificationFailureDumpRestoresLedger$' \
  -count=1 -v -timeout=10m ./internal/integration
```

This opt-in check restores the dump in a separate database inside its disposable
PostgreSQL container, verifies the authoritative ledger, and reads the complete
Kafka tar archive to reject truncation or missing record segments.
