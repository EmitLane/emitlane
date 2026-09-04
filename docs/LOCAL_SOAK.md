# Local soak testing

The local soak runner exercises EmitLane's real writer, Relay, PostgreSQL
storage, ordered virtual-partition ownership, and Kafka publishing. It creates
isolated PostgreSQL 16 and Kafka 4.3.1 containers for every run. It never uses
mocks for correctness results and has no option that can silently point at an
external database or broker.

## Requirements

- macOS or Linux;
- Go as specified by `go.mod`;
- Docker running locally;
- enough disk and memory for PostgreSQL, Kafka, and the selected workload.

## Start a run

Choose one profile:

```sh
make soak-start PROFILE=quick
make soak-start PROFILE=standard
make soak-start PROFILE=release
```

`soak-start` builds the dedicated runner, starts it in a detached background
process, prints its PID and artifact directory, and returns immediately. The
terminal, ChatGPT, and the coding-agent session may all be closed after this
command returns. Only the computer and Docker need to remain running.

Profiles use an 80% ordered / 20% unordered workload mix and seeded fault
injection. Defaults are:

- `quick`: 90 seconds, 2 Relays, 100 ordered streams, about 3,600 committed events;
- `standard`: 20 minutes, 4 Relays, 1,000 streams, target above 100,000 committed events;
- `release`: 60 minutes, 4 Relays, 3,500 streams, final local v0.3 release soak.

Override the running duration when needed:

```sh
make soak-start PROFILE=release DURATION=30m
```

Other Make overrides are `RECOVERY_TIMEOUT`, `SEED`, `RELAYS`, `STREAMS`, and
`RATE`. For example:

```sh
make soak-start PROFILE=quick DURATION=45s SEED=847129411
```

The built runner also accepts flags directly:

```sh
.emitlane/bin/emitlane-soak start --profile release --duration 30m --seed 847129411
```

The recorded seed makes transaction ordering and the fault schedule
approximately reproducible.

## Operate a run

```sh
make soak-status
make soak-logs
make soak-report
make soak-stop
```

`soak-status` reads the latest run's `progress.json`. While work is still being
delivered it reports **not observed yet**, not lost. A stale recorded PID is
shown as a crashed run.

`soak-logs` follows only the current run's log. Pressing Ctrl+C stops log
following and does not signal the soak.

`soak-stop` sends SIGTERM only to the PID recorded for the current run, after
verifying that the process command belongs to that run. The runner marks the
run aborted, writes its result and report, stops its Relay children, and removes
only containers labeled with that run ID. It does not use broad process or
container cleanup commands.

`soak-report` prints the deterministic report after the run has completed,
failed, or been aborted.

## Lifecycle and recovery

Runs move through `initializing`, `warmup`, `running`, `recovering`, `verifying`,
then `completed`, `failed`, or `aborted`. During the running phase, the seeded
schedule injects graceful Relay restarts, crash-like Relay termination and lease
takeover, Kafka outages, cluster pause/resume, and Relay membership changes.

When the configured duration ends, the runner stops producing and injecting
faults. It restores Kafka, resumes EmitLane, restores the requested Relay count,
and waits for the database backlog, ordered streams, and committed-ID verifier
to quiesce. Recovery defaults are 3 minutes for quick, 5 minutes for standard,
and 10 minutes for release. Only then are missing committed events called lost.

At-least-once duplicates are expected and reported but do not fail a run.
Correctness fails when any committed event is lost, an ordered stream regresses
or skips unexpectedly, final queue/blocked/gap state is nonzero, or the runner
records an infrastructure error. Throughput and latency are reported, never
used as arbitrary pass/fail thresholds.

## Artifacts

Each run owns `.emitlane/soak/<run-id>/`, with `.emitlane/soak/current` pointing
to the latest run. The directory contains:

- `config.json` and `metadata.json`;
- `state.json` and periodically refreshed `progress.json`;
- `soak.log` and `pid`;
- `result.json`, `report.md`, `timeline.svg`, and `exit_code` at termination.

`timeline.svg` plots committed and observed events, the temporary delivery
backlog, recovery, and fault markers (`K` Kafka outage, `C` Relay crash, `R`
graceful restart, `P` pause, and `M` membership change). It is generated
locally and embedded in `report.md`; no chart service or browser session is
required.

Detailed diagnostics are bounded to the first 100 failures. Reports do not
contain payloads or unbounded event-ID lists. `.emitlane/` is ignored by Git.

## Release run

Before a v0.3 release, start the full run manually and let it finish:

```sh
make soak-start PROFILE=release
make soak-status
make soak-logs
make soak-report
```

Do not close Docker or suspend the computer while it is running. A release soak
passes only when every committed ID was observed, ordering stayed monotonic,
and the recovered database has no pending, inflight, dead, blocked, or gap
state.
