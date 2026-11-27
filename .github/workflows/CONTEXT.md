<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# GitHub Actions Workflows, AI Guide

CI and release machinery. Read the YAML for mechanics; this covers what is not visible there: which
workflow owns what, and the publishing rules that bite.

## CI

`build.yml` is the entry point (pushes and PRs). It does not run the test suites itself: it holds
`lint` (typecheck), `lint_format` (prettier, eslint, and the DuckDB/pg version-consistency guards),
`runtime_test` (starts the built server bin under both `npx` with the Node shebang and a direct
`bun run` on the bundle), `docker_smoke_test`, and `cross_platform_test`, which calls
`cross-platform-tests.yml`.

`cross-platform-tests.yml` is `workflow_call`-only and is where the tests actually run: the SDK and
Console suites (plus the SDK's two time-zone runs), the server unit, component and integration
suites, the skills, examples, scripts, CLI and create-malloy-package tests, and the Linux-only CLI
integration and scaffolder e2e suites. Its callers pick one of two shapes with its inputs:

- **`linux: true`** (`build.yml`, every PR and every push to `main`): the whole suite on Linux, split
  into parallel jobs: `Linux: server component (1/3..3/3)`, `Linux: server unit (1/2, 2/2)`,
  `Linux: server integration`, and `Linux: SDK, Console, skills, examples, scripts, CLI, scaffolder`. On the PR that introduced the split each ran 6-9 minutes, about 2 of them setup, and a PR waited about 12 minutes in all, against about 29 for the single Linux leg before.
  A gate job, `Platform: (ubuntu-latest)`, needs all of them and fails unless every one passed. It
  is the **only** required check for the suite (`Cross Platform Tests / Platform: (ubuntu-latest)`,
  the name the single Linux leg had before the split), so regrouping or resharding never touches
  branch protection. Do not make the individual groups required.
- **`macos_windows: true`**: the whole suite in one job per OS, `Platform: (macos-latest)` and
  `Platform: (windows-latest)`, as every leg ran before the split, with the Windows Defender, `D:`
  profile and time-zone handling it needs. Not run on PRs by default: each leg took 28-32 minutes
  median, up to 66. `cross-platform-nightly.yml` runs it nightly (05:37 UTC) against `main`, on a PR
  that carries the `cross-platform` label (when it is added and on every push while it stays), and on
  `workflow_dispatch` against any ref. `release.yml` runs it too, before `prepare` (below). None of
  these is a required check, which is what lets an unlabelled PR merge without them.

The setup every test job shares (Bun, a frozen install, API types, the build that also bakes the
DuckDB extensions) is the local composite action `.github/actions/setup-workspace`. The SDK time-zone
runs and the example tests are `scripts/ci-sdk-tests-in-zone.sh` and `scripts/ci-example-tests.sh`,
shared by the Linux job and the macOS/Windows legs; each script's header says why it is shaped the
way it is.

**The server shards are balanced by time.** `bun test --shard=N/M --timings=…` places files by the
per-file durations in `packages/server/tests/shard-timings.json`; a file it does not list (a new
spec) still runs, placed without a weight. Each shard uploads what it measured as a
`shard-timings-<suite>-<N>` artifact. When the shards drift apart, download those artifacts from a
green run and merge them into the file:

```bash
gh run download <run-id> -p 'shard-timings-*' -D /tmp/timings
jq -s '{version: 1, files: (map(.files) | add | to_entries | sort_by(.key) | from_entries)}' \
  /tmp/timings/*/shard-timings.json > packages/server/tests/shard-timings.json
```

Raise a shard count (the matrix and the `/M` in the same job) when a group's slowest shard passes
about ten minutes of tests; the build in front of it is about two more.

The Postgres-gated server specs (`PUBLISHER_TEST_POSTGRES_URL` and the rest) skip in these jobs and
run against real services in `connection-integration-tests.yml`.

A nightly failure is reported the way every scheduled run is today, by GitHub's own notification of
the failed run. Slack reporting for the nightly workflows is tracked separately and will be a step of
its own in each of them.

The two npm publish workflows below do re-run their own package's suite, against the exact ref being
published, because a PR run proves nothing about the ref a dispatch actually targets. What they do
not repeat is the server suites and the Linux-only e2e suite.

`app-playwright.yml` runs the browser suite on every PR, whatever its base (a PR stacked on another
PR's branch included), against a server with package versioning on (playwright.config.ts starts it
with `PUBLISHER_PACKAGE_VERSIONING=on`). The model viewer's specs get a pass of their own, against a server started
with `PUBLISHER_MODEL_PAGE=viewer` (`EXPECT_MODEL_PAGE=viewer`),
because the suite's server shows the default model page, the Explorer.

The suite runs in three shards, `Playwright shard (1/3..3/3)`. Each shard is on its own runner with
its own Publisher, and runs on one worker inside the shard, because specs share server state. The
spec files each shard runs are listed in `packages/app/tests/playwright/shards.json`, balanced by
measured per-file durations. Shard 2 carries the least and also runs the model viewer pass.
`shards.mjs check` runs first in every shard. It fails the shard when a spec is in no group, is in
two, or a group names a file that is gone, so **a new spec must be added to `shards.json`**.
A gate job, `Playwright (packages/app)`, needs every shard and fails unless all of them passed. It is the
**only** required check for the suite, under the name the single job had before the split, so
regrouping never touches branch protection. Do not make the shards required, and do not give one
that name. Chromium's headless shell is cached under `PLAYWRIGHT_BROWSERS_PATH`, keyed on the
`@playwright/test` version. Credible's image has Chromium's system packages, so apt runs there only
if the browser is missing a library. GitHub's image does not have them all, so it takes `--with-deps`.

**Not every publish workflow is purely a publish workflow, so a red check on a PR that touches one
package can come from its own file rather than from `build.yml`.** Know which job:

- `skills-npm.yml`'s `check_pack` runs on PRs and on pushes to `main` that touch `skills/**`,
  `packages/skills/**`, or the workflow file. It only builds and tests the package; skills carries
  no PR-time version check any more (see *Skills and create-malloy-package version at release
  time* below).
- `create-malloy-package-npm.yml` is genuinely dispatch-only: `workflow_dispatch` is its only
  trigger. Its `pull_request` trigger and `check_version` job are both gone.

`hammer-nightly.yml` runs the hammer scenario suite (`hammer/README.md`) against a real server,
a throwaway Postgres in the runner's Docker and a local DuckLake, nightly and on
`workflow_dispatch`. It is not a pull-request check and nothing depends on it. Two legs, one with
`PUBLISHER_MATERIALIZE=true` and one with `false`, each upload their logs as an artifact.

`nightly.yml` holds the nightly checks of issue #1897 other than the hammer, one job each, on a
schedule (06:41 UTC) and `workflow_dispatch`. Its `upgrade` job runs `hammer/upgrade/run.ts`: the
latest released server builds a fixture package's materializations, then `main`'s build must serve
them unchanged from the same server root and data. Its `mixed-version` job runs
`hammer/mixed-version/run.ts`, the rolling-deploy direction: `main`'s build writes a manifest and the
latest released server, bound to it, must serve every stored table from it (its in-process
counterpart, a frozen copy of the release's manifest loader, runs on every pull request in
`test:component`). Neither it nor `hammer-nightly.yml` holds a secret or is a pull-request check.
Each job writes its job summary from a JSON report (`hammer-report.json`, `upgrade-report.json`,
`mixed-version-report.json`) that is uploaded with the logs (the hammer's on every run, the others'
when they fail); post a nightly result from that report, never from raw logs.

`main-thread-regression.yml` is the main-thread regression check
(`packages/server/tests/perf/main-thread/README.md`): a storm-lite A/B of two server builds on
one runner, added after Publisher 0.13.1 pinned the worker's main thread
(https://github.com/ms2data/service/issues/7795). On a PR that touches `packages/server/**`
or `bun.lock` it compares the PR's head with its merge base; nightly, and as `release.yml`'s
gate, it compares `main` with the anchor release in `anchor.json`. It holds no secret but stays on GitHub-hosted
`ubuntu-latest`, where its absolute budgets were calibrated, and it is **not a required check yet**: its flake rate on PRs decides
that. It path-filters, which is only safe because it is not required (see below).

Two workflows hold real warehouse credentials, and both should be treated as such when edited:
`connection-integration-tests.yml` (the full connection matrix) and `k6-tests.yml`, which passes
`BQ_PRESTO_TRINO_KEY` and runs on pushes as well as on demand.

Every workflow a pull request triggers shares one `concurrency:` rule: runs for a PR are grouped by
its number and a new push cancels the one in flight, while every other event (push to `main`,
`workflow_run`, schedule, dispatch) is grouped by its own run id, so it is never cancelled or
replaced and every merged commit is tested. `push:` triggers are `main` only, so a PR branch runs
once, through `pull_request`. Path filters are not used on `build.yml` or `app-playwright.yml`:
their jobs are required status checks on `main`, and a filtered-out workflow leaves a required check
pending forever. A docs-only skip needs an always-run gate job first.

### Credible's donated runners

Credible runs self-hosted Ubuntu 24.04 x64 runners for the `malloydata` organization (4 vCPU / 16 GB,
Docker, one job at a time per machine, machines reused between jobs), which do not count against
GitHub's 20-concurrent-job limit. A job
opts in with:

```yaml
runs-on: ${{ vars.CREDIBLE_RUNNERS == 'true' && 'credible-ubuntu-24.04' || 'ubuntu-latest' }}
timeout-minutes: 30
```

The organization variable `CREDIBLE_RUNNERS` is the kill switch: set it to `false` and every such job
goes back to GitHub's runners with no PR. On them: `build.yml`'s jobs, every Linux job of
`cross-platform-tests.yml` (seven test jobs and the gate per PR), `app-playwright.yml`, the Trivy jobs in `security-scan.yml` and
`image-scan.yml` (amd64 build only), `release-sync.yml`, `skills-npm.yml`'s tarball check (not its publish job), and
`hammer-nightly.yml`. `docker_smoke_test` reads a registry layer cache there and writes it only from
`main`, through the `layer-cache-writer` identity bound to `main`'s OIDC subject.

**Keep off them:** any job that uses a repository or organization secret, holds `id-token: write`
for anything but the layer cache, publishes, or runs on `pull_request_target` (the runners refuse
those); and macOS, Windows, and arm64 jobs. Publishing jobs, which stay on GitHub's runners, restore no
Actions cache (setup-bun's `no-cache: true`): any `main` job, here or on GitHub's runners, can write
`main`'s cache entries. Every job moved there needs a `timeout-minutes`. `credible-ubuntu-24.04` is spot
(a reclaimed job fails "The operation was canceled." and a rerun covers it);
`credible-ubuntu-24.04-ondemand` is the label for a job that must not be interrupted. Package
installs go through pull-through caches whose credentials last an hour from job start. Jobs there
restore no Bun install cache: its steps run only where `runner.environment == 'github-hosted'`, since on
Credible's runners that cache downloads through the farm's NAT while `bun install` reads the npm mirror
inside GCP.

**Every `bun install` runs through `scripts/ci-install-with-retry.sh`**, in the composite action and
in each workflow that installs on its own. The npm mirror intermittently answers a tarball GET with
401 or 403 well inside that hour (one was seen 35 seconds after the job-started hook set it up), and
with about seven installing jobs per PR, one such answer failed a PR or `main` often. The script
re-runs the same command, unchanged, up to three attempts (15 s, then 45 s apart), against the same
registry: it adds no fallback registry and touches no runner configuration, so a retried install
resolves exactly what the first would have, which is why the publishing jobs use it too. A retry that
rescues a job leaves a `Package install retried` warning annotation on the run, so a mirror that is
getting worse stays visible instead of being absorbed; report a run of those to the runners' owners.
A new install step should use the script too.

## Publishing

There are three npm trains. sdk/app/server/cli share one version; the other two each have their own.
Nothing publishes to PyPI (see *PyPI: currently not published*).

| Packages | Registry | Version | Published by | Missing bump caught by |
|---|---|---|---|---|
| `@malloy-publisher/sdk`, `app`, `server`, `cli` | npm | Lockstep, set by `release.yml` | `npm-sdk.yml`, called from `release.yml` | n/a — the release sets it |
| `@malloy-publisher/skills` | npm | Decided at release time by `scripts/independent-version.mjs`; `package.json` carries a fixed `0.0.0-dev` placeholder | `skills-npm.yml` | n/a — nothing to forget, the decision reads npm itself |
| `@malloy-publisher/create-malloy-package` | npm | Decided at release time the same way; `package.json` also carries `0.0.0-dev` | `create-malloy-package-npm.yml` | n/a, same reason |

### The versioning policy

**`0.MINOR.PATCH` for every published package, while pre-1.0. MINOR is a breaking
change; PATCH is everything else.**

**In force on the `sdk`/`app`/`server` line since `0.2.0`** (2026-08-28), which
followed `0.0.250` directly, skipping `0.1.x` because `skills` occupies it and two
trains sharing a minor invites reading one for the other. The move needed
`-f version=0.2.0`, because a minor bump computed from a `0.0.x` floor lands on
`0.1.0`; on the `0.x` line an ordinary `bump=minor` gives the policy's number
(`0.11.0` was a `bump=minor` off `0.10.7`). `skills` has always been on `0.1.x` and
so already follows the policy.

`create-malloy-package` does not: it is still on `0.0.x`, so `^` pins it exactly,
the same trap the policy exists to close. Nothing moves it on its own, because the
release-time version decision is one patch above the highest published version,
never the shape of the number; moving it is a hand dispatch with `-f version=` (see
*Skills and create-malloy-package version at release time*). Not urgent: the
scaffolder has no dependents.

`bun.lock` is a fourth reader of these numbers and is deliberately left alone. It
records an older version for the lockstep workspace packages (`0.9.16` when 0.11.0
shipped), so once a release PR merges
`main` disagrees with the manifests it locked. That is benign — the entries
are workspace links, not registry resolutions, and `bun install --frozen-lockfile`
does not compare them — so `prepare` does not touch the lockfile, which keeps the
release out of the business of regenerating it. If a future bun makes that a hard
error, the fix is to rewrite those `bun.lock` entries beside `PKGS` in `prepare`,
not to make `prepare` run an install.

The point of the policy is that a consumer can write a range. Verified with
`semver.validRange`:

| Range | Resolves to |
|---|---|
| `^0.0.250` | `>=0.0.250 <0.0.251` — exactly one version |
| `~0.0.250` | `>=0.0.250 <0.1.0` |
| `^0.2.0` | `>=0.2.0 <0.3.0` |

So on `0.0.x` there is no way to say "patches yes, breaking no": `^` pins a single
version, and `~` spans a range whose upper half does not exist. On `0.MINOR.PATCH`
a minor means breaking, so `^0.2.0` is a genuinely compatible range. (Note `~` and
`^` are *identical* on a `0.x` line — both stop at the next minor — so `^` is a
convention here, not a wider range.)

**There is no `major` on the dispatch, deliberately.** Publisher is preview, so a
major is a decision to be made with an explicit `-f version=` rather than a
dropdown choice. One registry fact worth knowing before anyone does: on
`@malloy-publisher/sdk`, **`1.0.1` is published** (an old off-line publish that
`latest` long since moved off) while `1.0.0` is free; `app` and `server` have no
`1.x` at all. So `1.0.1` is burned on one of the three, and a 1.x line would have
to start at `1.0.0` and skip `1.0.1`, or start at `1.0.2`. `prepare`'s floor
comment covers the related trap: `npm view … version` reads the `latest` dist-tag,
not the highest version, and "correcting" it to the maximum would make the next
release `1.0.2` and take the whole line with it.

`release.yml`'s `bump` input chooses `patch` (default) or `minor` off the same npm
floor; `-f version=` still wins outright. The level is applied once and the walk
past an already-taken number is by patch, so `bump=minor` colliding with an
existing `0.3.0` gives `0.3.1` rather than abandoning the `0.3.x` line. Note a
minor bump is computed from npm's current `latest`, so off a `0.0.x` floor it
lands on `0.1.0` — which is why the move to `0.2.0` was made with an explicit
version.

Prereleases keep the hyphen convention: `npm-sdk.yml` routes a hyphenated version
to the `next` dist-tag and `docker-image.yml` withholds `latest`, so a prerelease
never takes over either.

**The `next` dist-tag was retired on 2026-08-26, on npm and on Docker Hub.** It
had pointed at `0.0.198-dev6` since 2026-05-21 while 51 stable versions shipped
past it, because a dist-tag only moves when a publish uses it and no prerelease
had been cut since. So `@malloy-publisher/server@next` silently installed a
three-month-old build, and `ms2data/malloy-publisher:next` served the matching
image. Nothing in this repo, in the Credible service, or in the published docs
installed from either. Three consequences, and the first one decides what
automation is possible here at all:

- **npm OIDC cannot move a dist-tag.** Trusted publishing authenticates
  `npm publish` and `npm stage publish` only; `npm dist-tag add|rm` needs a
  granular token, which this repo deliberately does not have (see the OIDC note
  below). So CI can move `next` only by publishing a real prerelease under it.
  Re-pointing `next` at an existing stable release is not available without
  reintroducing `NPM_TOKEN`, which is not a trade worth making for a tag nothing
  reads. The same follows for any other dist-tag surgery: it is a human org
  member acting by hand, never a workflow.
- **The routing was left in place, so this is self-healing.** `npm-sdk.yml`
  still sends any hyphenated version to `next`, and `docker-image.yml` still
  tags `:next` on a prerelease. The first prerelease after this recreates both
  tags, correctly, with no change here.
- **`docs/deployment.md` now states there is no pre-release channel today.**
  That sentence is true only while the above holds, so a release that cuts a
  prerelease has to update it in the same PR.

### Skills and create-malloy-package version at release time

There is no PR-time bump to forget for these two: nothing is committed ahead of time, so there is
nothing for a check to compare against. `packages/skills/package.json` and
`packages/create-malloy-package/package.json` both carry a fixed `0.0.0-dev` placeholder version in
the repo; what actually publishes is decided by `scripts/independent-version.mjs`, called from
`scripts/publish-packages.sh` (a release) or from the child workflow's own "Resolve version to
publish" step (a hand dispatch), and written into the manifest at publish time by
`scripts/set-version.mjs`, in the runner's working tree only.

The two packages are decided differently:

- **skills** publishes when its published content changed since npm `latest`'s `gitHead`: a
  `git diff --quiet <gitHead> HEAD` over `skills/`, `packages/skills/`, `bun.lock` and the root
  `package.json`, excluding `skills/README.md` and `packages/skills/src/*.spec.ts` (neither reaches
  the tarball). Changed publishes one patch above the highest published plain version; unchanged
  skips. `gitHead` is what `npm publish` stamped onto the currently-published version's manifest,
  which is why `release.yml`'s checkout now uses `fetch-depth: 0` — an arbitrary past commit a
  shallow clone would not have.
- **create-malloy-package** publishes on every non-prerelease release, because it bakes the
  server's npm `latest` into every workspace it scaffolds, so a release changes what it ships even
  when nothing in its own directory did. The only skip is a re-run: if npm `latest`'s
  `publisherServer` field already equals this release's version, a previous attempt of the same
  release already got it out.

The publish version is one patch above the HIGHEST published plain `x.y.z` version
(`npm view <pkg> versions --json`), not `latest` plus one: `latest` can be moved backwards by hand
(`npm dist-tag add <pkg>@<older> latest`) after a bad release while the newer version stays
published, and computing off `latest` alone would recompute that already-published version and
abort — under `set -e`, before the scaffolder is ever dispatched, so a server release could ship
with no scaffolder pinning it. `latest` and its `gitHead` stay the content-diff baseline above,
since that's what users actually get; only the version arithmetic moved to the versions list.

A hand dispatch of either child workflow (`gh workflow run skills-npm.yml` /
`create-malloy-package-npm.yml`, no `version` input) publishes one patch above the highest
published version, the same "free and above the ceiling" guard the old PR check used to enforce,
now run once at dispatch time instead of on every PR. **A minor or major bump is a hand dispatch
with `-f version=`** — nothing computes one automatically. After a hand-dispatched skills minor,
hand-dispatch the scaffolder too (it depends on skills' version), or just wait for the next
release, which republishes the scaffolder anyway.

The old PR-time bump check's `bun.lock` gap is closed by this move, not merely inherited: `bun.lock`
and the root `package.json` are in the skills diff's watched paths directly (they change what
`dist/` becomes, since it's emitted by `tsc` and bun resolves that from the lockfile), where before
they were outside `skills-npm.yml`'s `paths:` trigger and a lockfile-only PR never reached the
check at all. A lockfile-only change now counts as "changed" and triggers a publish on its own.

`release.yml` is `workflow_dispatch`-only and is the single place a release starts. Its `prepare` job
bumps sdk/app/server/cli, stamps the release notes, commits both to a fresh `release/v<version>`
branch cut from `main`, and pushes it; `npm-sdk.yml` and `docker-image.yml` are then called with that
ref. `publish-packages` triggers the two other npm trains (see below).
`gh-release` cuts the tag, and
only after npm and Docker have both succeeded.

**`main-thread-check` and `cross-platform-tests` run first.** `prepare` needs both, so a release
whose `main` fails the main-thread regression check, or fails the whole suite on macOS or Windows
(`cross-platform-tests.yml` with `macos_windows: true`, against the commit being released), stops
before any branch is cut or anything is published, and a re-dispatch after a fix starts clean. The
main-thread check adds about ten minutes; the macOS and Windows legs run alongside it and add 30 to
60, which is the price of PRs no longer running them. They run rather than look up last night's
result because `main` usually moves between the nightly and a dispatch, so a green nightly on the
release commit is rarely there to find. They restore no Actions cache (`restore_cache: false`), the
rule for anything in front of a publish, and have no skip input: a release that cannot pass them
waits for the fix. A release that must
ship while the check itself is known to be wrong is dispatched with
`skip_main_thread_check=true`: the job still runs and passes with a warning, rather than being
skipped, because a skipped job would also skip every job downstream of `prepare`.

**The release branch is also the post-release PR.** `prepare` rewrites every `## [Unreleased]`
heading in `RELEASE_NOTES.md` as `## [<version>]` (`scripts/release-notes.mjs stamp`) in the same
commit that sets the four `packages/{sdk,app,server,cli}/package.json` versions
(`scripts/set-version.mjs`). `gh-release` then puts the sections headed `[<version>]` on the release
page (`release-notes.mjs extract <version>`) and prints a compare link. A person opens
`release/v<version>` into `main` as a PR, titled `chore(release): <version>`, and merges it; that
one PR is the whole post-release step. It does **not** touch `packages/skills/package.json` or
`packages/create-malloy-package/package.json`: those two carry a fixed `0.0.0-dev` placeholder and
their published version is decided at release time from npm's own state (see *Skills and
create-malloy-package version at release time* above). Both halves used to be manual post-release
steps and were reliably skipped: 0.0.243 through 0.0.247 shipped with none of their narrative.

**The release page has a length limit, and the job fits the body to it.** GitHub refuses a release
body over 125,000 characters with a 422, and `gh-release` runs after npm and Docker have
published, so 0.13.0 (75 sections, about 136k characters) failed there and its page was made by hand.
`gh-release` now fetches the generated PR list itself (`gh api …/releases/generate-notes`, the list
`--generate-notes` would append) and has `release-notes.mjs fit` compose the whole body under a
120,000-character budget. It gives up detail in order until the body fits: the narrative's full text
becomes an index of its section titles linking `RELEASE_NOTES.md` at the tag (the form 0.13.0's page
was made in by hand), then the PR list is truncated keeping its **Full Changelog** compare link, then
only that link is left, then the index itself is shortened. The last step is a plain cut, so length
cannot fail the job. Each fallback logs a notice, and the index fallback adds a line to the job
summary. If the list cannot be fetched, the compare link stands in for it; if `fit` itself fails, the
header goes up alone with `--generate-notes`, under the usual warning.

Why this shape, and not the ones it replaced:

- **One tree.** The stamp, the version and the narrative on the release page all come from the
  commit npm and Docker published. An earlier design extracted from the release branch and stamped a
  fresh branch cut from `main` minutes later, which needed a `--titles` artifact to keep the two
  agreeing about which sections shipped. A section merged to `main` while a release runs is simply
  not on the release branch, stays `[Unreleased]`, and ships next time.
- **A release that fails stamps nothing that matters.** The stamped branch reaches `main` only by a PR
  someone opens after the release succeeded. A failed release leaves its branch unmerged, and the
  next dispatch walks past it (`version_taken`) and cuts a new one from `main`, which still reads
  `[Unreleased]`.
- **Not a push to `main`.** `main` is protected and requires a pull request; until 0.0.250 the stamp
  step pushed there and every attempt was rejected with `GH006`.
- **Not `gh pr create` from the workflow.** Opening a PR with `GITHUB_TOKEN` needs the off-by-default
  "Allow GitHub Actions to create and approve pull requests" setting, which only an admin can read.
  Worse, a bot-authored PR triggers no workflows, so `main`'s required checks sit at `expected` and
  only an admin can merge it. A PR opened by a person (or by an agent on that person's credentials,
  which is what the `publisher-release` skill does) runs its checks and any maintainer can merge it.

**`release-sync.yml` fails while a release's PR is unmerged.** It compares npm's `latest` for
`@malloy-publisher/sdk` with the version `packages/sdk/package.json` declares, on every PR, every
push to `main`, and right after each `release.yml` run (`workflow_run`, against `main`'s head, so
`main` reads red as soon as there is a release PR to merge). It is its own workflow rather than a
`build.yml` job because it is not about the build: it changes state when npm does, not when the
code does. `main`'s version moves only when a release PR merges, so npm ahead
of it means a release published and its PR has not merged. The next release would then stamp that
release's still-`[Unreleased]` sections a second time under the new number. The fix is to merge
`release/v<npm latest>` into `main`, which is also the only thing that clears the check; the
release PR itself passes because its tree declares the new version. It reads npm rather than the
git tags because a release whose npm publish succeeded and whose `gh-release` failed has no tag
but did ship.

**Expect it red on unrelated PRs.** Because it runs on every PR, contributors see it fail from the
moment a release publishes until that release's PR merges, on changes that have nothing to do with
the release. That is deliberate, and it is meant to be a **required status check** on `main`, so
in that window nothing else merges and the release PR is the next thing to land. It gates merges,
not dispatches: `release.yml` does not consult it.

**A prerelease is never stamped.** `prepare` skips the stamp for any version with a `-` or `+`, so
`release-notes.mjs extract <prerelease>` finds nothing (it accepts the version, and there is no
`[<prerelease>]` heading to match), and `main`'s `[Unreleased]` sections stay unstamped and ship
with the next ordinary release. A prerelease gets no GitHub release page in any case.

**Merging it back usually needs a conflict resolved.** `main` requires branches to be up to date, and
a new `[Unreleased]` section merged during or after the release is normally inserted directly above
the first stamped heading. Git treats those adjacent lines as one hunk, so `RELEASE_NOTES.md`
conflicts. Keep both sides: `main`'s new `[Unreleased]` sections as they are, and the release branch's
`[<version>]` headings. The version lines do not conflict, since only release PRs change them.
**Do not push to the release branch until the release has finished**, because `npm-sdk.yml` and
`docker-image.yml` check out the branch by name, not by commit.

**`delete_branch_on_merge` is on, so merging the PR deletes `release/v<version>`.** The tag keeps
the commit, `version_taken` checks the tag as well as the branch, and the release page's
"Release branch" line is the only thing left pointing at a name that no longer exists.

`scripts/release-notes.mjs` and `scripts/set-version.mjs` both have unit coverage —
`release-notes.spec.ts` and `set-version.spec.ts` — run by `build.yml`'s `lint_format` job on every PR
(`bun run test:scripts`). The notes spec covers each of `fit`'s fallbacks, and that the body never
exceeds its limit however small. The notes spec also smoke-runs `extract` against the real
`RELEASE_NOTES.md`, so a heading it cannot read a title from fails the PR that wrote it rather than the
release that would silently drop the narrative; the version spec does the equivalent, asserting the
replace still finds the `"version"` field in all four real manifests at their real paths, so a
rename or a reformat that moved it out of reach fails a PR rather than a release. The release path
itself is dispatch-only and cannot be exercised in CI, so the shell in the changed steps was verified
out of band instead: each `run:` body extracted from the YAML and executed against a scratch repo with
`npm`, `npx` and `curl` stubbed, which is how the version-resolution steps, the server-pin
substitution, the `--host` check, `prepare`'s stamp and `release-sync.yml`'s comparison were each covered. That harness is a one-off and
is **not** in this repo — it is worth rebuilding rather than trusting, because a later edit to any of
those bodies has nothing automated behind it.

Merging the release PR cannot collide with `publish-packages`: it changes only
`packages/{sdk,app,server,cli}/package.json` and `RELEASE_NOTES.md`, neither of which
`publish_indep_pkg`'s "main moved" guard watches for skills or create-malloy-package. The reason to
wait for the run to finish before touching the PR is the release branch itself (above), not this
guard.

npm publishing uses **GitHub Actions OIDC trusted publishing**, not a stored token, as PyPI's did
(see *PyPI: currently not published*). There is no `NPM_TOKEN` or `PYPI_TOKEN` in this repo and
neither should be added back. Only the
Docker path uses a secret (`DOCKERHUB_TOKEN`).

### Docker image signing

`docker-image.yml`'s `merge` job signs the published manifest list with cosign, keyless, after
`imagetools create` pushes it, then verifies the signature before the job ends. Things that are not
obvious from the YAML:

- **The digest comes from `imagetools create --metadata-file`, not from a tag.** A signature binds to
  one digest; resolving `:latest` or the version tag afterwards would sign whatever the tag points at
  by then.
- **The certificate names `docker-image.yml`, not `release.yml`.** Fulcio's subject for GitHub
  Actions is `job_workflow_ref`, the reusable workflow, so the identity a consumer verifies is
  `https://github.com/malloydata/publisher/.github/workflows/docker-image.yml@refs/heads/main`.
  Renaming or moving `docker-image.yml` changes that identity and breaks every consumer pinned to
  it; treat the filename as a public contract.
- **`id-token: write` has to be granted twice.** The `merge` job requests it and `release.yml`'s
  `publish-docker` job must grant it, since a called workflow cannot hold more than its caller
  grants. Dropping it from the caller fails `publish-docker` before any job starts.
- **One signature covers the attestations.** The manifest list references each platform image and
  its BuildKit SBOM and provenance attestation manifests by digest, so signing the list binds all of
  them. The attestations themselves are not separately signed.
- **A signing failure fails the release after the tags are already pushed.** The image is public
  but unsigned, and `gh-release` does not run. Re-run the failed jobs: `imagetools create` re-pushes
  the same list from the same platform digests and the signing steps run again. A Sigstore outage
  (Fulcio or Rekor) shows up here the same way.
- **The verify retries "no signatures found", and only that.** Docker Hub can take a moment to
  serve a signature it just accepted (0.15.0 and 0.16.0 both failed verify two seconds after the
  push, and passed on a re-run), so the verify step retries that one error with backoff, 6 attempts
  over about 2.5 minutes, logging each retry. Any other error, an identity or issuer mismatch
  included, fails at once, and the step never re-signs: a second `cosign sign` adds a second
  signature rather than repairing the first.
- **The `@refs/heads/main` in the identity is what makes a branch dispatch distinguishable.**
  `release.yml` has no ref guard (see *Hardening that is not in place yet*), so a release dispatched
  from a branch is signed with that branch's ref. The in-workflow verify accepts any ref and prints
  the identity it found; consumers pinning `@refs/heads/main` reject the branch-built image.

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
  This is why `@malloy-publisher/sdk`, `app`, `server` and
  `cli` have entries naming `release.yml` even though the `npm publish` calls live in
  `npm-sdk.yml`.
- **`release.yml` dispatches the two independently-versioned npm packages, it does not call them.**
  `publish-packages` triggers `skills-npm.yml` and `create-malloy-package-npm.yml` through the
  Actions API, so each runs as its own top-level run and keeps its own npm entry naming its own
  file. Converting either to `workflow_call` would force its npm entry to be re-pointed at
  `release.yml` and would immediately break its manual dispatch. That is a one-way door, because
  there is only one entry per package.
- **PyPI's matching rule is the opposite of npm's.** PyPI matches the file that defines the
  uploading job, and refuses a reusable workflow outright, so a future PyPI upload cannot be
  dispatched from `publish-packages`. See *PyPI: currently not published*.
- **`timeout-minutes` on `publish-packages` tracks the package count.** It is two poll budgets plus
  margin now. Keep it above N x `POLL_BUDGET_SECONDS`, or the LAST package timing out gets killed by
  the job cap before it can print which package failed — the one diagnostic that says whether a
  publish happened.
- **Dispatch the children on `main`, never on the release branch.** `prepare` bumps only sdk/app/
  server, so a release branch's copies of these two packages are identical to `main`, and both
  children guard on `github.ref == 'refs/heads/main'`. Dispatched on a release branch their publish
  jobs skip, and a skipped job reports success, so a release would go green having published nothing.
- **Order is forced: skills before create-malloy-package.** The scaffolder pins its skills dependency
  from the version skills just published (or skills' npm `latest`, when skills was skipped) and then
  asks the registry whether that range resolves, so skills has to be decided, and confirmed on npm if
  it published, before the scaffolder is dispatched. Dispatching both at once fails the scaffolder.
- **OIDC auto-enables provenance, which requires `repository.url`** in each published `package.json`.
  A package missing it fails to publish with a `422 ... provenance` error. This is the likely failure
  when adding a new package.
- **`main` carries the released sdk/app/server/cli version only once that release's PR merges.** Before
  release branches were merged back, `main` sat 41 patches behind npm. `release-sync.yml`
  reports when it lags, but nothing stops a dispatch, so `prepare`'s floor is
  still `max(npm latest, main declared)` and fails closed when the registry will not answer. **Do
  not "simplify" that to reading the file** — an unmerged release PR leaves the file behind npm, and
  the max is correct either way. `prepare` then walks forward from the floor until it finds a
  version whose release branch and tag are both free.
- **Publish steps need scripts enabled.** `prepack` is what copies `skills/` into the skills package
  and what builds the scaffolder's `dist/`. A publish run with `--ignore-scripts`, or with
  `ignore-scripts=true` in the publisher's own npmrc, ships a package with the contents missing and
  exits 0. The derived `SERVER_VERSION` now rides on this too: the substitution edits `src/scaffold.ts`,
  and only `prepack` turns that into the `dist/index.js` npm actually ships (`dist/` is gitignored, so
  there is no stale copy to fall back to). Nothing asserts the substituted value reached `dist/` —
  scripts-disabled would ship no `dist/` at all rather than an old pin, which is why this is a footnote
  and not a check.

### PyPI: currently not published

Nothing in this repository publishes to PyPI. The generated Python client (`packages/python-client`),
its build workflow (`python-sdk.yml`) and `release.yml`'s `build-python-sdk` and `publish-python-sdk`
jobs were removed in 0.17.0. The versions of `malloy-publisher-sdk` already on PyPI (0.10.5 through
0.16.x) stay there as published: none was yanked, and no deprecation release was made. The name is
kept for a future client, so the setup on PyPI's side is left in place and recorded here, to be
switched back on rather than rebuilt.

**The trusted publisher (OIDC, no token).** Project `malloy-publisher-sdk`, owner `malloydata`,
repository `publisher`, workflow **`release.yml`**, environment **`pypi`**. There is no
`PYPI_TOKEN`, and one should not be added. The `pypi` environment exists, with its deployment
branches restricted to `main` by a custom policy (created 2026-10-29). To check it:

```bash
# Expect one policy, `main`:
gh api repos/malloydata/publisher/environments/pypi/deployment-branch-policies \
  --jq '.branch_policies[].name'
```

**What a new client has to match, because none of it is guessable:**

- **PyPI matches the file that defines the uploading job, not the top-level workflow.** Warehouse
  looks the publisher up by the token's `job_workflow_ref` claim. That is the opposite of npm's
  rule (see *Rules that bite* above), so the upload has to be a job written in `release.yml` itself,
  or the PyPI entry has to be changed to name the file that holds it.
- **The upload cannot live in a reusable workflow.** PyPI does not support trusted publishing from
  one, so the shape is two jobs: a build job (it may call a reusable workflow) that uploads `dist/`
  as an artifact and holds no `id-token`, and a plain publish job in `release.yml` with
  `environment: pypi` and `permissions: id-token: write` and nothing else, which downloads the
  artifact and hands it to `pypa/gh-action-pypi-publish` pinned by SHA. Keeping the build out of the
  job that can mint a token is the security model, not tidiness.
- **The environment is the boundary.** `release.yml` has no ref guard (see *Hardening that is not
  in place yet*), and PyPI does not check the ref, but it refuses a token whose `environment` claim
  is not `pypi`. Keep `environment: pypi` on the publish job and the environment restricted to
  `main`. If the environment is ever deleted, recreate it, restricted, **before** anything on PyPI
  names it: GitHub creates a missing environment on first use with no restrictions.
- **Check the wheel's project name before uploading.** A token minted for the entry uploads only to
  `malloy-publisher-sdk`; anything else fails with "Non-user identities cannot create new projects".
- **Upload after `publish-npm` and `publish-docker`, outside `gh-release`'s `needs`.** A PyPI
  filename can never be reused, so an upload that ran before a failed npm or Docker publish would
  leave a client version for a server that never shipped; and a failed upload must not block the
  tag, for the reason `publish-packages` is outside it.
- **Skip a version already on PyPI**, so "Re-run failed jobs" after an upload that landed is a no-op.

The removed jobs, with their guards, are in the history of `release.yml` and of
`.github/workflows/python-sdk.yml` (`git log -- .github/workflows/python-sdk.yml`).

### The CLI rides the lockstep train

**`@malloy-publisher/cli` (the `malloy-pub` binary) is versioned and published with sdk/app/server.**
It was `@malloydata/publisher-cli` at `0.0.1`, never published; decision D6 renamed it into the
`@malloy-publisher` scope and put `packages/cli/package.json` in `prepare`'s `PKGS`, so a release sets
it to the same number as the other three. `npm-sdk.yml` builds and publishes it last, after the
server. Its `build` is a single bundle (dependencies inlined) whose `prebuild` regenerates the API
client from `api-doc.yaml`, so the client in the tarball always matches the server released beside
it, and the tarball needs Node only, no Bun. `prepublishOnly` refuses to publish without
`dist/index.js`, since `files` is `dist` alone and a missing build would otherwise ship an empty
package and exit 0.

It is the last step of the same job, so a CLI publish that fails fails `publish-npm`, and with it
`gh-release` and `publish-packages`, after sdk, app and server are already on npm. Re-running the job
cannot help (sdk's publish would fail on a taken version) and re-running the release burns a version.
Publish the missing CLI by hand from the release branch with an npm login that can publish to the
scope (`cd packages/cli && bun run build && npm publish --access public`; OIDC works only inside
Actions), then create the tag and release page by hand from that branch (`gh release create
v<version> --target release/v<version>`, with the notes `release-notes.mjs extract <version>` prints),
and dispatch `skills-npm.yml` then `create-malloy-package-npm.yml` on `main`.

Its trusted-publisher entry names **`release.yml`**, like sdk/app/server, for the reason under *Rules
that bite*. npm will not take an entry until the package exists, so the first version is published by hand
and the entry registered **before** the change that wires it into `npm-sdk.yml` merges; otherwise the
first release fails exactly as above (see *Adding a published package*).

For completeness: `api-doc.yaml`'s `info.version: v0` is the REST API version behind the `/api/v0`
path prefix, not a package version. It is unrelated to any of this and should not be bumped with a
release.

### Adding a published package

Publish the first version by hand: npm will not accept a trusted-publisher entry until the package
exists. Then add the `repository` field, register the trusted publisher naming that package's own
workflow file, and wire it into `release.yml`'s `publish-packages` so a release picks it up.

### Recovering a failed release

The two independently-versioned npm packages are safe to retry **through `publish-packages`**: that job
asks the registry first and skips a version that is already published, so re-running it republishes
nothing. The skip lives in that job, not in the children — each child's "Verify this version is not
already published" step `exit 1`s on a version npm already holds, so a hand-dispatched child that
already published fails rather than no-opping.

**If `publish-packages` is the only red job, do not re-run the release.** It sits outside
`gh-release`'s `needs`, so it neither blocked nor undid the tag: the sdk/app/server/cli release completed
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

One thing the derived `SERVER_VERSION` changed about that hand dispatch: it is now **quiet** where it
used to be loud. Dispatching `create-malloy-package-npm.yml` resolves the pin from whatever the server's
`latest` is at that moment, so a dispatch fired before `publish-npm` has landed the new server
publishes a scaffolder pinned a release behind, and passes. The old hand-maintained pin failed the run
instead. A release waits for this itself (see below), but a hand dispatch does not: confirm the server
version is on npm (`npm view @malloy-publisher/server dist-tags.latest`) before dispatching the
scaffolder by hand.

It fires only when `main` moved **under the paths that package's published content is built from**, not
on any movement at all: the second package is checked after the first has finished a full
dispatch-and-wait, so a bare sha comparison failed it whenever anything merged inside that window,
including changes that could not affect it. `create-malloy-package` no longer watches
`packages/skills/package.json`: its publish job now takes the skills version to depend on as a
dispatch input (`skills_version`, set from what skills just published, or its npm `latest` when
skills was skipped) rather than reading that file from `main`, so a skills publish with no
scaffolder-relevant change no longer retriggers this guard. Every uncertain answer still aborts: an
unanswered compare API, and a diff at that API's 300-file cap, where the list may be truncated.

**Two kinds of skip are how a green release ships nothing (or ships less than expected), and both
name themselves in the job summary.** Read that summary rather than the run's green tick if you
expected a publish.

1. **Nothing was decided to publish.** `independent-version.mjs` can return `skip` for a real reason,
   not a bug: skills skips when nothing in its watched paths changed since npm `latest`'s `gitHead`,
   and the scaffolder skips only on a re-run where its published `publisherServer` already equals
   this release. Either skip is expected and reported by name in the summary; it is not the same
   failure mode a forgotten bump used to be, because there is no bump to forget any more.
2. **The release was a prerelease.** Any hyphen in the release version skips both packages, because
   their own versions carry no hyphen and their publish workflows would tag them `latest`.
   `gh-release` skips prereleases for the same reason. Publish these from an ordinary release, or
   dispatch their workflows on `main` directly.

Those two are skips. The scaffolder used to have a third failure that was not a skip but a hard red,
costing a full poll budget to discover: it refused to publish unless the `SERVER_VERSION` it pins in
`src/scaffold.ts` equalled `@malloy-publisher/server`'s npm `latest`, and nothing set that pin, so a
release that forgot it failed 25 minutes in, *after* `skills` had already published.

**That pin is now derived at publish time**, the same way the `workspace:*` skills dependency is:
`create-malloy-package-npm.yml` reads npm's current server `latest`, substitutes it into the runner's
working tree only, asserts the substitution took, and then verifies it. The value committed to the
repo is a dev default — what a `bun run` from a clone boots — and **setting it in a pre-release PR is
now wrong rather than merely unnecessary**, because the substitution overwrites it.

Three things about that pair are load-bearing.

*The verification step is not a tautology.* The resolve step writes; the verify step reads the line
back with the same expression the old hand-policed guard used and re-asks the registry. So it still
catches everything it did before, plus a file that grew a second `SERVER_VERSION` line and a `latest`
that moved between the two steps. The protection it exists for is unchanged: a scaffolder published
ahead of its pinned server generates workspaces whose `npm start` cannot resolve at all, under a
version that can never be replaced.

*`sed` exits 0 when it matches nothing,* so `set -e` does not catch a substitution that did not apply
— the trap the skills-dep step in the same file already names. The resolve step therefore asserts the
exact line it wrote. Without that it exits 0 announcing `SERVER_VERSION <unreadable> -> <latest>`.

*The `--host` check had to become automatic.* The old guard's error message told a human to "confirm
the new server still honours `--host`", and deriving the pin removes that prompt. Nothing else covers
the thing that matters here, which is the **published** server this scaffolder is about to pin. The
scaffolder's e2e suite does boot a server with `--host 127.0.0.1` and assert no LAN address answers
(`packages/create-malloy-package/tests/e2e/scaffold.e2e.spec.ts:350-395`), run on every PR in
`cross-platform-tests.yml`'s Linux client job — but against the **repo's own build**, which is the one place the flag
could not regress unnoticed anyway. The server itself has no unit test asserting the flag is parsed
(`server.ts:112`), only the usage text at `:157`. The flag is not arbitrary — the server accepts an unknown flag without a word and falls back
to binding `0.0.0.0`, so a release that renames or drops it does not fail a generated workspace, it
silently makes it serve an unauthenticated REST API and MCP endpoint on every interface while its own
`package.json`, `AGENTS.md` and `README` all still say `127.0.0.1`. The job now runs the resolved
version's `--help`, which prints usage and `process.exit(0)`s before binding a port, and requires
`--host` in the option column **with an argument placeholder and a description after it**. Anchored
that tightly, not on a bare substring, because two near misses both pass a loose match: a server that
replaced the flag and merely named it in a wrapped description line ("`--host` is deprecated; use
`--bind`"), and one that kept the name but dropped its argument, which would leave a generated
workspace's address behind as a stray token.

All of this only works because `publish-packages` waits for `publish-npm`, which it did not until
0.0.250. Before that this job raced the server publish, and *no* pin could satisfy the old guard: the
outgoing version failed against an npm still a release behind, the previous one failed once npm caught
up. 0.0.250 is the worked example — the guard ran at 15:07:55 against a `latest` that did not become
0.0.250 until 15:09:13. **Narrowing that `needs:` back to `prepare` alone now breaks the derivation
silently** rather than loudly: the substituted value would simply be a release behind, and every
generated workspace would pin the previous server.

That `needs:` is not enough on its own, because npm can take minutes to show a version after
`npm publish` returns. In 0.7.0, `publish-npm` finished at 19:09 and `latest` read 0.7.0 only at
19:16, so the scaffolder read 0.6.0. So `publish-packages.sh` also waits, up to 15 minutes, for the
server's `latest` to read the version the release shipped before it dispatches the scaffolder
(`wait_for_server_latest`). If `latest` never gets there, the scaffolder is not dispatched, `set -e`
ends the job, and the job says to re-run it once `latest` reads the new version.

That wait proves only the release runner's view of npm. The registry is cached per CDN edge
(`cache-control: public, max-age=300`), so the scaffolder's runners can read the previous `latest` for
a few more minutes. So the release also dispatches the scaffolder with `server_version` set to the
version it shipped. Both scaffolder jobs wait for their own `latest` to read it, and the pin step
refuses anything else. A hand dispatch leaves the input empty and pins `latest` as before.

The `--host` check runs `npx @malloy-publisher/server@<pin>` from an empty temporary directory, not
the checkout. In the checkout, npx uses the workspace's own unbuilt `packages/server` whenever the pin
equals the version that file declares, and fails with `malloy-publisher: not found`. That failed the
0.5.1 and 0.7.0 scaffolder publishes. In 0.7.0 it was also the only thing that stopped a scaffolder
pinned to the previous server from shipping.

sdk/app/server/cli are not re-runnable at the same version today. `prepare` walks the version forward
whenever a release branch or tag exists, so a release that fails after `npm-sdk.yml` has published
cannot be resumed and the retry burns four new versions. npm versions are immutable, so there is no
repair beyond moving forward.

### Hardening that is not in place yet

There are **three** npm trusted-publisher workflows, not two: `release.yml` (for sdk, app, server and cli),
`skills-npm.yml`, and `create-malloy-package-npm.yml`. Any hardening has to cover all three.
PyPI's one entry (`release.yml`, for `malloy-publisher-sdk`) is unused while nothing publishes
there, and stays bound to the `pypi` environment, whose deployment branches are restricted to `main`
(see *PyPI: currently not published*).

The `github.ref == 'refs/heads/main'` guards on the two child publish jobs are a guardrail, not a
boundary: npm matches repo plus filename and never a ref, so anyone who can dispatch can push a
branch whose copy of the workflow drops the line. **`release.yml` carries no ref guard at all**, and
its `prepare` job branches from whatever ref it was dispatched on and passes that branch to
`npm-sdk.yml`, so a branch dispatch publishes that branch's sdk/app/server/cli under the real names. That
is the largest of the three surfaces, so it matters most there.

The control that survives all of it is binding each trusted-publisher entry to a GitHub Environment
whose deployment branches are restricted to `main`, which npm verifies from the OIDC token. If that
is added, land the `environment:` key in the workflow **before** setting the environment name on npm,
never after, or every publish fails on a claim mismatch that looks nothing like its cause. Configure
it with branch restrictions only and no required reviewers, since a pending approval would stall the
release's wait for the package to appear on npm.
