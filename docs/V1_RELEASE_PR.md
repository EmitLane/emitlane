# v1.0.0 Release PR draft

## Proposed title

`chore(main): release 1.0.0`

## Proposed review summary

This release adopts the [v1 contract](V1_CONTRACT.md) for the documented SDK,
wire and operator interfaces. The module path remains
`github.com/emitlane/emitlane`. Schema migration 5 adds the partial
pending-unordered claim index, preserving published migrations 1–4.
[Release notes](../CHANGELOG.md) cover fenced lease renewal and
Inbox changes, full integrity verification and the Go 1.27.2 minimum.
The [upgrade guide](UPGRADING.md#v091-to-v100) covers maintenance installation,
updating managed consumers and removing only migration 5 before rolling back
to v0.9.1 on schema 4. This path still requires candidate qualification.

The source baseline is `d0d19d3`. The 72-hour managed qualification is
recorded for that SHA and fixture. Final candidate approval is pending in
the [release acceptance record](V1_RELEASE_CHECKLIST.md).

## Prepare the generated Release PR

1. Carry `Release-As: 1.0.0` in the commit body that reaches `main`. If
   the preparation branch is squash-merged, preserve the trailer in the
   squash commit body. The root package has `bump-minor-pre-major` disabled
   for the intentional transition. The existing manifest records 0.9.1.
   Release Please generates the 1.0.0 manifest change in its Release PR.
   See [Release Please version overrides](https://github.com/googleapis/release-please#how-do-i-change-the-version-number).
2. Keep `RELEASES_ENABLED` closed while preparing the Release PR. After
   separate authorization to open it, use the preparation-only
   `release-pr` operation. Review the generated branch, version, pending
   label and manifest diff. Use the title above as the version/title
   expectation while preserving the bot's release metadata.
   See [Release Please CLI](https://github.com/googleapis/release-please/blob/main/docs/cli.md#creatingupdating-release-prs).
3. In the generated PR, move the curated notes from `Unreleased` into
   the generated 1.0.0 entry and remove the consumed draft section. Review
   the compare link and release date. Update the release notes inside the
   bot's PR body as well, preserving its markers. Confirm the documented
   support and upgrade claims against the acceptance record.
4. Verify the release flow can keep publication closed through creation of
   the final merge SHA. After any separately approved merge, keep publication
   closed until that SHA passes the
   [final candidate checks](V1_FINAL_SHA_CHECKS.md) and all remaining
   acceptance items. Keep the approved merge SHA in the release record.

Publication requires a separate approval of the pending release PR and its
exact merge SHA. The combined Release Please workflow runs on a push to
`main`, so changing `RELEASES_ENABLED` alone does not start it. Review the
pending releases and select the authorized publication trigger at sign-off.
Before dispatching artifact publication, verify the new tag's SHA and check
whether the artifact workflow has already started. See [Releasing](RELEASING.md).
