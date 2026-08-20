# GitHub Actions Workflows, AI Guide

CI and release machinery. Read the YAML for mechanics; this covers what is not visible there: which
workflow owns what, and the publishing rules that bite.

## CI

`build.yml` is the entry point (pushes and PRs). It does not run the test suites itself: it holds
`lint` (typecheck), `lint_format` (prettier, eslint, and the DuckDB/pg version-consistency guards),
`runtime_test` (starts the built server bin under both `npx` with the Node shebang and a direct
`bun run` on the bundle), `docker_smoke_test`, and `cross_platform_test`, which calls
`cross-platform-tests.yml`.

`cross-platform-tests.yml` is `workflow_call`-only and is where the tests actually run, across Linux,
macOS, and Windows: the server unit and integration suites, the skills package tests, and the
create-malloy-package tests plus its e2e suite on Linux. It is reached on every PR through
`build.yml`.

The two publish workflows below do re-run their own package's suite, against the exact ref being
published, because a PR run proves nothing about the ref a dispatch actually targets. What they do
not repeat is the server suites and the Linux-only e2e suite.

`app-playwright.yml` runs the browser suite on PRs. `python-sdk.yml` builds and publishes the Python
client. `skills-npm.yml` is not only a publish workflow: its `check_pack` job also runs on PRs, and on pushes
to `main`, that touch `skills/**`, `packages/skills/**`, or the workflow file itself. So a red check
there on a skills-only PR comes from that job, not from `build.yml`. `create-malloy-package-npm.yml`,
by contrast, really is dispatch-only.

Two workflows hold real warehouse credentials, and both should be treated as such when edited:
`connection-integration-tests.yml` (the full connection matrix) and `k6-tests.yml`, which passes
`BQ_PRESTO_TRINO_KEY` and runs on pushes as well as on demand.

## Publishing

There are three npm trains, and they do not share a version.

| Packages | Version | Published by |
|---|---|---|
| `@malloy-publisher/sdk`, `app`, `server` | Lockstep, set by `release.yml` | `npm-sdk.yml`, called from `release.yml` |
| `@malloy-publisher/skills` | Its own line | `skills-npm.yml` |
| `@malloy-publisher/create-malloy-package` | Its own line | `create-malloy-package-npm.yml` |

`release.yml` is `workflow_dispatch`-only and is the single place a release starts. Its `prepare` job
bumps sdk/app/server, commits to a fresh `release/sdk-<version>` branch, and pushes it; `npm-sdk.yml`
and `docker-image.yml` are then called with that ref. `publish-packages` triggers the other two
trains (see below). `gh-release` cuts the tag, and only after npm and Docker have both succeeded.

`gh-release` also owns the release notes. It appends every `## [Unreleased]` section of
`RELEASE_NOTES.md` to the release body (via `scripts/release-notes.mjs`) and then opens a PR stamping
those headings with the shipped version. Both used to be manual post-release steps and were reliably
skipped — 0.0.243 through 0.0.247 shipped with none of their narrative.

Only the first half is fully automatic. **The stamp is a PR because it has to be**: `main` is a
protected branch requiring a pull request, so the direct `git push origin main` this step did until
0.0.250 was rejected with `GH006` on every attempt, retry included. 0.0.249 is the worked example —
all six of its sections reached the release page and not one was stamped. The token cannot be granted
a bypass from inside the workflow: `main` is on **classic** branch protection, which has no
Actions-bypass lever, so that is a repo-admin change to the protection itself.

Two things about that pair are easy to get wrong.

**The two halves must agree on which sections shipped.** `extract` runs against the release branch's
snapshot of the file; `stamp` runs minutes later against whatever `main` has become, and recent
releases take 4–10 minutes. So `extract --titles` writes the exact heading lines it consumed to a
file under `$RUNNER_TEMP`, and `stamp --titles` rewrites only those. Unscoped, a section merged
inside that window was stamped with a version it never shipped in — off that release's page, and off
every later one, because its heading no longer said `[Unreleased]`. The titles file lives in
`$RUNNER_TEMP` and not the repo because the stamp step checks out a fresh branch from `origin/main`,
which would discard an in-tree file first.

**A stamp that does not land is not cheap, and "opened a PR" is not "landed."** The stamp step is
`continue-on-error` — by then the release is public and correct, and a failed docs commit must not
redden it — but the cost is *not* merely a heading that still reads `[Unreleased]` for the next
release to pick up. That heading is exactly what the next release's `extract` matches, so the next
release re-appends **this** release's narrative to its own page, and so does the one after that,
until a human notices. That is why every failure path in the step emits a `::warning` and a
job-summary line naming the fix, rather than relying on `continue-on-error` alone.

Moving to a PR shrinks that failure window without closing it: the consequence is identical whether
the PR was never opened or was opened and left sitting. So the stamp PR is part of releasing, not
paperwork after it — merge it before the next dispatch, and check for a stale one before dispatching.
Two properties of that PR are worth knowing before someone files a bug about it:

- **Its required checks never run.** GitHub does not trigger workflows for events authored by
  `GITHUB_TOKEN`, so the required checks stay `expected` forever and only an admin can merge it. The
  PR body says so, because blank checks on a green repo look like breakage.
- **A re-run of `gh-release` is safe.** The branch is named for the version and force-pushed, and a
  `gh pr create` that fails because the PR already exists falls back to reporting that PR's URL
  rather than failing the step.

The durable fix is an admin one: migrate `main` to a ruleset and give the Actions token a bypass on
the pull-request requirement, or hand the step a GitHub App token so its PR can run checks and
auto-merge. Both trade away something real — the first widens the surface flagged under *Hardening
that is not in place yet*, since `release.yml` has no ref guard, and the second adds a stored
credential to a repo that deliberately has none for npm.

`scripts/release-notes.mjs` has unit coverage in `scripts/release-notes.spec.ts`, run by `build.yml`'s
`lint_format` job on every PR (`bun run test:scripts`), which also smoke-runs `extract` against the
real `RELEASE_NOTES.md` so a heading it cannot read a title from fails the PR that wrote it rather
than the release that would silently drop the narrative. The release path itself is dispatch-only and
cannot be exercised in CI.

The stamp used to push to `main` while `publish-packages` may still have been polling, and that
guard ignored it only because `RELEASE_NOTES.md` is not among the paths it watches. As a PR it cannot
collide at all: the branch it pushes is not `main`, and `main` moves only when someone merges, long
after the run. The path list is no longer what keeps those two apart.

npm publishing uses **GitHub Actions OIDC trusted publishing**, not a stored token. There is no
`NPM_TOKEN` in this repo and one should not be added back. The Docker and PyPI paths do use secrets
(`DOCKERHUB_TOKEN`, `PYPI_TOKEN`); npm does not.

### Rules that bite (not guessable from the YAML)

The first two rules below are npm's behaviour, not ours, and they drive a one-way-door decision, so
they carry sources. Check them before acting on them: npm can change either unilaterally, and
everything in this file that follows from them changes with it.

- **One trusted publisher per package.** npm allows a single entry per package, so a package cannot
  be publishable from two workflows. Sources: npm's
  [trusted publishing docs](https://docs.npmjs.com/trusted-publishers), and
  `malloydata/malloy`'s own `.github/workflows/CONTEXT.md`, which records the concrete check and the
  **409** the registry returns on a second registration for the same package.
- **Matching is on the top-level caller workflow filename**, not a reusable workflow it calls. npm's
  [trusted publishing docs](https://docs.npmjs.com/trusted-publishers) describe this as a mismatch
  hazard, and `malloydata/malloy`'s `CONTEXT.md` states the same rule verbatim ("Matching is on the
  top-level caller workflow filename, not a reusable workflow it calls"), independently arrived at.
  This is why `@malloy-publisher/sdk`, `app`, and
  `server` have entries naming `release.yml` even though the `npm publish` calls live in
  `npm-sdk.yml`.
- **`release.yml` dispatches the two independently-versioned packages, it does not call them.**
  `publish-packages` triggers `skills-npm.yml` and `create-malloy-package-npm.yml` through the
  Actions API, so each runs as its own top-level run and keeps its own npm entry naming its own file.
  Converting either to `workflow_call` would force its npm entry to be re-pointed at `release.yml`
  and would immediately break its manual dispatch. That is a one-way door, because there is only one
  entry per package.
- **Dispatch the children on `main`, never on the release branch.** `prepare` bumps only sdk/app/
  server, so a release branch's copies of these two packages are identical to `main`, and both
  children guard on `github.ref == 'refs/heads/main'`. Dispatched on a release branch their publish
  jobs skip, and a skipped job reports success, so a release would go green having published nothing.
- **Order is forced: skills before create-malloy-package.** The scaffolder pins its skills dependency
  from the monorepo and then asks the registry whether that range resolves, so skills has to be on
  npm first. Dispatching both at once fails the scaffolder.
- **OIDC auto-enables provenance, which requires `repository.url`** in each published `package.json`.
  A package missing it fails to publish with a `422 ... provenance` error. This is the likely failure
  when adding a new package.
- **`main` does not carry the released sdk/app/server version.** Release branches are never merged
  back, so `packages/sdk/package.json` on `main` lags npm. `prepare` walks the patch version forward
  until it finds one whose release branch and tag are both free, which is what keeps that workable.
- **Publish steps need scripts enabled.** `prepack` is what copies `skills/` into the skills package
  and what builds the scaffolder's `dist/`. A publish run with `--ignore-scripts`, or with
  `ignore-scripts=true` in the publisher's own npmrc, ships a package with the contents missing and
  exits 0.

### Adding a published package

Publish the first version by hand: npm will not accept a trusted-publisher entry until the package
exists. Then add the `repository` field, register the trusted publisher naming that package's own
workflow file, and wire it into `release.yml`'s `publish-packages` so a release picks it up.

### Recovering a failed release

The two independently-versioned packages are safe to retry **through `publish-packages`**: that job
asks the registry first and skips a version that is already published, so re-running it republishes
nothing. The skip lives in that job, not in the children — each child's "Verify this version is not
already published" step `exit 1`s on a version npm already holds, so a hand-dispatched child that
already published fails rather than no-opping.

**If `publish-packages` is the only red job, do not re-run the release.** It sits outside
`gh-release`'s `needs`, so it neither blocked nor undid the tag: the sdk/app/server release completed
and only the two dispatches are outstanding. Use "Re-run failed jobs", which re-enters that job alone
against a now-settled `main` and skips whatever already landed. Dispatching `skills-npm.yml` and then
`create-malloy-package-npm.yml` on `main` by hand also works, in that order, but ask npm which of the
two is still missing and dispatch only that one — per the note above, a child whose version is already
published fails its own guard. Re-running `release.yml` instead walks the version forward and burns
three npm versions for nothing.

The "main moved during this release" abort is an expected, retryable failure of this kind. It fails
before dispatching the package it names, but note it can fire on the second package after the first
has already published, so read the job summary for what actually shipped rather than assuming the
registry is untouched. Re-running `publish-packages` is safe either way, because it skips a version
that is already on npm; a hand dispatch is safe only for the package that has not published yet.

It fires only when `main` moved **under the paths that package's published content is built from**, not
on any movement at all: the second package is checked after the first has finished a full
dispatch-and-wait, so a bare sha comparison failed it whenever anything merged inside that window,
including changes that could not affect it. Those paths are not simply "where the package lives".
`create-malloy-package` includes `packages/skills/package.json`, because its publish job reads the
skills version from `main` at dispatch time and bakes it into the published dependency range, even
though no skills file enters its tarball. Every uncertain answer still aborts: an unanswered compare
API, and a diff at that API's 300-file cap, where the list may be truncated.

**Two skips are how a green release ships nothing, and both name themselves in the job summary.**
Read that summary rather than the run's green tick if you expected a publish.

1. **The version was not bumped.** `publish-packages` decides purely on the version in `main`'s
   `package.json`, so if a change lands in `skills/` or `packages/create-malloy-package/` without a
   version bump, the release skips that package and stays green. Nothing else in CI requires the bump.
2. **The release was a prerelease.** Any hyphen in the release version skips both packages, because
   their own versions carry no hyphen and their publish workflows would tag them `latest`.
   `gh-release` skips prereleases for the same reason. Publish these from an ordinary release, or
   dispatch their workflows on `main` directly.

sdk/app/server are not re-runnable at the same version today. `prepare` walks the version forward
whenever a release branch or tag exists, so a release that fails after `npm-sdk.yml` has published
cannot be resumed and the retry burns three new versions. npm versions are immutable, so there is no
repair beyond moving forward.

### Hardening that is not in place yet

There are **three** trusted-publisher entries, not two: `release.yml` (for sdk, app, and server),
`skills-npm.yml`, and `create-malloy-package-npm.yml`. Any hardening has to cover all three.

The `github.ref == 'refs/heads/main'` guards on the two child publish jobs are a guardrail, not a
boundary: npm matches repo plus filename and never a ref, so anyone who can dispatch can push a
branch whose copy of the workflow drops the line. **`release.yml` carries no ref guard at all**, and
its `prepare` job branches from whatever ref it was dispatched on and passes that branch to
`npm-sdk.yml`, so a branch dispatch publishes that branch's sdk/app/server under the real names. That
is the largest of the three surfaces, so it matters most there.

The control that survives all of it is binding each trusted-publisher entry to a GitHub Environment
whose deployment branches are restricted to `main`, which npm verifies from the OIDC token. If that
is added, land the `environment:` key in the workflow **before** setting the environment name on npm,
never after, or every publish fails on a claim mismatch that looks nothing like its cause. Configure
it with branch restrictions only and no required reviewers, since a pending approval would stall the
release's wait for the package to appear on npm.
