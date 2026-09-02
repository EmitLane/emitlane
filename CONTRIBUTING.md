# Contributing to EmitLane

Contributions should favor correctness, explicit failure semantics, and
observability over feature count. Relay delivery is at least once. Never describe
EmitLane as providing end-to-end exactly-once delivery.

Report security vulnerabilities privately through
[GitHub Security Advisories](https://github.com/EmitLane/emitlane/security/advisories/new),
not in a public issue.

## Before you start

Read `AGENTS.md` and:

- `docs/02-ARCHITECTURE.md`;
- `docs/03-DATABASE.md`;
- `docs/04-GO-API.md`;
- `docs/05-DELIVERY-SEMANTICS.md`;
- `docs/DELIVERY_GUARANTEES.md`;
- `docs/FAILURE_MODES.md`;
- `docs/10-ROADMAP.md`.

For v0.1 scope, `EMITLANE_V0_1_IMPLEMENTATION.md` has priority over broader
roadmap documents. Propose large or behavior-changing work in an issue first.
If source-of-truth documents disagree, record the decision in
`docs/14-DECISIONS.md` before changing behavior.

## Branches and commits

Branch from `main` and keep the branch short-lived:

```text
feat/<name>
fix/<name>
docs/<name>
refactor/<name>
test/<name>
ci/<name>
chore/<name>
perf/<name>
```

There is no `develop`, `staging`, or `release/*` branch.

Use Conventional Commits for commits and pull request titles. Allowed types are
`feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `ci`, `build`, `chore`,
`revert`.

```text
feat(outbox): add PostgreSQL writer
fix(relay): recover expired leases
docs: clarify delivery semantics
```

## Local verification

Requires Go 1.27.x. Integration tests require Docker (real PostgreSQL and Kafka
via Testcontainers).

```bash
gofmt -w .
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
staticcheck ./...
govulncheck ./...
go build ./cmd/emitlane
```

With Docker running:

```bash
go test -tags=integration -count=1 -timeout=20m ./...
```

`make check` approximates the fast CI jobs. Unit tests must not depend on
locally running infrastructure.

## Pull requests

Open pull requests against `main`. The PR title becomes the squash commit on
`main`, so it must be a valid Conventional Commit.

For every critical state transition, review what happens if the process dies
immediately before and after it. State whether an event can be lost or duplicated,
whether a lease can remain stuck, whether two relays can make the transition
concurrently, how the state is observed, and whether recovery is automatic or
operator-driven.

## Releases

Every commit merged into `main` must pass CI and should remain releasable. Normal
releases are maintainer-driven through Release Please and GoReleaser; ordinary
contributors must not create version tags. See `docs/RELEASING.md`.
