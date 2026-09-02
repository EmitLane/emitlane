# Releasing EmitLane

Release Please prepares versions and changelogs; GoReleaser publishes artifacts.
Automation is the default, and a normal merge to `main` never publishes a release.

## Current readiness gate

Do not enable releases until a maintainer is ready to publish. The product now
has a Go module, `cmd/emitlane`, Dockerfile, integration tests, and failure-injection
coverage. A tag is still a human decision: do not create `v0.1.0` just because
the code exists.

Release automation is guarded by the repository Actions variable
`RELEASES_ENABLED`. It must equal the lowercase string `true`; an absent variable
or any other value keeps Release Please skipped and makes a direct release run
fail closed. Even when enabled, release preflight requires `go.mod`,
`cmd/emitlane/main.go`, and `.goreleaser.yaml`.

Docker images are built by GoReleaser into `ghcr.io/emitlane/emitlane` when a
release runs. The release workflow needs `packages: write` for GHCR. Public
image visibility should be confirmed in package settings after the first publish.

## Normal release flow

1. Merge focused pull requests to `main`. Their squash titles must use
   Conventional Commits such as `feat(relay): add lease recovery` or
   `fix(outbox): preserve retry state`.
2. When releases are enabled, Release Please creates or updates one Release PR
   from the commits on `main`.
3. Review the proposed semantic version and generated `CHANGELOG.md`. The Release
   PR must pass the same required CI checks as every other PR.
4. Merge the Release PR only when the project is ready to ship.
5. On that merge, Release Please creates the `vMAJOR.MINOR.PATCH` tag and GitHub
   Release.
6. The Release Please workflow dispatches the release workflow with the new tag.
   GoReleaser then validates and publishes the release artifacts.
7. Verify the GitHub Release, archives, supported platforms, SHA-256 checksum
   file, and every documented GHCR tag and architecture.

GitHub suppresses most recursive workflow events created with a workflow's
built-in `GITHUB_TOKEN`, and automated pull request runs otherwise wait for
manual approval. `workflow_dispatch` is exempt from that recursion guard, so no
personal access token is needed here: Release Please explicitly dispatches
`ci.yml` on the generated Release PR branch and dispatches the release workflow
after creating a tag. The
publication dispatch remains eligible even if Release Please creates the tag and
then reports a later API failure, so that failure cannot silently strand a tagged
release without an artifact run.
`release.yml` also retains its `v*` tag-push trigger for an authorized manual
recovery tag.

The release workflow rejects a malformed semantic-version tag, a tag not on
`main`, the wrong Go module path, or a missing CLI main package. It must never be
used to turn incomplete source into a nominal release.

If a tag and GitHub Release exist but the artifact workflow was never started,
dispatch it for that exact existing tag instead of creating another tag:

```bash
gh workflow run release.yml --ref v0.1.0 -f tag=v0.1.0
```

If an artifact upload stopped partway through, do not overwrite a completed
release blindly. Inspect the existing assets and workflow logs first. If any
published asset may already have been consumed, fix the problem with a new patch
release. Otherwise, a maintainer may remove only the incomplete assets and rerun
the workflow for the unchanged tag. Recovery is operator-driven and visible in
GitHub Actions; tags are never moved or reused.

## Version and tag policy

Use Semantic Versioning and version tags only:

```text
v0.1.0
v0.2.1
v1.0.0
v0.2.0-rc.1
```

Do not use names such as `release-1`, `prod`, `latest`, `stable`, or date tags for
Go module releases. The manifest starts at `0.0.0`; that is automation state, not
a published release. Without a previous tag, Release Please would otherwise
default the first release to `1.0.0`. The config sets `initial-version` to
`0.1.0` so the first Release PR is `v0.1.0`. Use an alpha, beta, or
release-candidate suffix only when intentionally publishing a prerelease for
early feedback. Do not tag an initialization or CI-only commit.

While the project is on `v0.x`, `bump-minor-pre-major` turns a breaking commit
into the next minor version instead of accidentally declaring `v1.0.0`. That flag
does not choose the first version; `initial-version` does. Before an intentional
stable v1 release, maintainers must deliberately revisit that pre-major setting
and review the chosen stable version. Generated changelog sections retain
user-visible features, fixes, performance, documentation, dependencies, and other
changes; routine CI, test, and chore commits are omitted.

Versions `v0` and `v1` use the current module path:

```text
github.com/emitlane/emitlane
```

Before any `v2.0.0` release, Go's semantic import versioning requires changing the
module path and public imports to:

```text
github.com/emitlane/emitlane/v2
```

That is a future breaking change; do not make it during v0 or v1 release setup.

## Emergency patch and manual fallback

For an urgent patch, use the normal path whenever possible: merge a tested `fix:`
PR, review the resulting Release PR, and merge it. This keeps Release Please's
manifest and changelog synchronized.

Only when release automation has failed and a maintainer has intentionally chosen
manual recovery, tag a commit that is already on protected `main` and has passed
all required checks:

```bash
git checkout main
git pull --ff-only

git tag -a v0.1.1 -m "EmitLane v0.1.1"
git push origin v0.1.1
```

Replace the example with the correct unused semantic version. Confirm the tag's
commit and version before pushing; do not move, delete, or reuse a published tag.
The tag push starts `release.yml` and still must pass its release preflight.

> Do not manually create tags during normal releases when Release Please is
> managing version state, unless intentionally recovering from release automation
> failure.

After a manual fallback, repair the Release Please manifest and changelog through
a reviewed PR before the next automated release. A manual tag alone does not
synchronize that state.

## One-time GitHub repository settings

These controls cannot be committed to the repository and must be configured by an
administrator.

### Actions

Under **Settings → Actions → General → Workflow permissions**:

- select **Read and write permissions**;
- enable **Allow GitHub Actions to create and approve pull requests**.

The workflows still declare their own minimal permissions. No custom repository
secret is required; they use the built-in token. Under **Settings → Secrets and
variables → Actions → Variables**, create `RELEASES_ENABLED` only when the product
is ready and set it to exactly `true`. Remove it or set it to another value to
close the release gate.

For a public repository, CodeQL needs no extra license setting. If the repository
is private when Go source lands, enable GitHub Advanced Security and code scanning
under the repository's security settings so CodeQL can upload results.

### `main` branch ruleset

Under **Settings → Rules → Rulesets**, create an active branch ruleset named
`main`, targeting only `refs/heads/main`, with no bypass actors initially. Configure:

- restrict deletions: on;
- block force pushes: on;
- require a pull request before merging: on;
- required approving reviews: `0` while there is only one maintainer;
- require conversation resolution before merging: on;
- require status checks to pass: on;
- require branches to be up to date before merging: on;
- require linear history: on.

Require these exact GitHub Actions status checks:

```text
format
vet
staticcheck
unit (Go 1.27.x)
race
integration
vulnerability
build
```

Select **GitHub Actions** as the expected source for each required check after it
has run at least once in the repository.

Do not initially require `CodeQL analysis` in the ruleset. It still runs on normal
pull requests, pushes to `main`, and its weekly schedule, but the built-in-token
Release PR path explicitly dispatches `ci.yml`, not CodeQL. Revisit this after a
reliable trusted dispatch path exists.

Leave required reviews, merge queue, signed commits, deployments, and other rules
off unless the maintainer model or threat model changes.

### Merge strategy

Under **Settings → General → Pull Requests**:

- allow squash merging: on;
- default squash commit title: pull request title;
- default squash commit message: pull request body;
- allow merge commits: off;
- allow rebase merging: off initially;
- automatically delete head branches: on.

Because the PR title becomes the commit on `main`, verify that it follows
Conventional Commits before squash merging.
