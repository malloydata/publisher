<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Main-thread regression check

A storm-lite A/B that fails when a change makes the server's main thread do much
more work under a fleet-like load, or makes `/status` slow. It exists because
Publisher 0.13.1 pinned the worker's main thread: `/status` p90 went from about
430 ms to 9 s in Credible's dev fleet and main-thread CPU per load doubled, and
nothing in this repo measured either
([ms2data/service#7795](https://github.com/ms2data/service/issues/7795)). The
design follows Credible's worker-storm release check (ms2data/service#7640),
shrunk to fit a CI runner.

## Run it locally

```bash
bun install --frozen-lockfile
cd packages/server
bun run perf:main-thread --baseline git:v0.12.6 --candidate ../..   # this checkout vs a release
bun run perf:main-thread --baseline git:origin/main --candidate git:HEAD --rounds 3
```

A build spec is `git:<ref>` (checked out as a worktree under `<out>/builds`,
installed and built there and reused by a later run with the same `--out`; `git worktree prune` forgets it once `<out>` is deleted) or a
path to a checkout, `packages/server` or a `dist` directory, built with
`build:server-only:bundle` if it has no `dist/server.mjs`. Results land in
`<out>` (default `$TMPDIR/mtr-<time>`): `summary.md`,
`verdict.json`, each run's JSON and each server's log as `mtr_<side>-<round>_server.{log,err}`.
Exit status: 0 PASS, 1 FAIL, 2 invalid run or harness error.

It needs about 6 GB of free memory and four idle cores. On a loaded laptop the
relative checks still hold (both sides see the same load) but the absolute
budgets will not, so read those from CI. It binds ports 4941 to 4943 by default
(`--port` moves all three). Other options: `--rounds` (default 2), `--ops` (160),
`--concurrency` (8), `--envs` (4), `--seed`, `--cpu-prof`, and
`--limits '{"statusP90BudgetMs": 600}'` to override a limit while calibrating.

To explore one build without a verdict, run the storm on its own:

```bash
bun tests/perf/main-thread/fixtures.ts /tmp/mtr-fx
bun tests/perf/main-thread/storm.ts --dist dist --fixtures /tmp/mtr-fx/fixtures.json --out /tmp/mtr.json
```

## What one storm does

`storm.ts` starts one built server under `bun`, as the image runs it, with
package versioning on, `PUBLISHER_MATERIALIZE=true` (dev's setting, where the
bind path regressed), four load workers and the memory governor ticking every
2 s. It preloads `probe.ts` on the server's main thread (below).

The package set (`fixtures.ts`) is `storefront` and `governed-analytics` from
`examples/`, two generated packages of fleet-like size (8 and 24 model files),
`mat` (a generated 10-model package with four persisted rollups), `mat-mini`
(one model, two persisted rollups) and `gov-arrays` (access governed by a given
over array columns and a repeated record). Each is written as two versions.

1. It creates four environments and places every version in each (56 versions).
   Each materializing version is published once so its build plan names the
   sources a build manifest binds, and two manifests are written for it.
2. It drives 160 operations, eight at a time, from a seeded script that is the
   same for both sides: 40% publishes (a materializing version sends its
   `manifestLocation`, so the bind path runs), 20% version unloads, 10% reloads,
   10% manifest binds that change the binding, 15% re-binds of the identical
   manifest, 5% environment patches. Two pollers read
   `/status?includeLoading=true` every 200 ms throughout, as the control plane's
   heartbeat does.
3. Two quiet checks follow, each in its own probe window: three identical
   re-binds of a bound `mat-mini` version, and twenty `/status` polls after
   unloading three versions.

`run.ts` preflights every package against both builds and keeps the A/B to the
ones both load, so an example an old baseline cannot read narrows the set
instead of skewing it. It then runs the storm on each side in alternating order
(baseline, candidate, candidate, baseline, ...), so a runner that slows down
mid-job slows both sides alike.

### The probe

`probe.ts` is test-only: a `bun --preload` module the bundle never imports,
inert unless `MTR_PROBE_PORT` is set, which only the storm sets. It adds no API,
setting or metric to the server. From the main thread it measures:

- **main-thread CPU**, `process.threadCpuUsage()`, so the load workers' CPU is
  not counted;
- **the longest event-loop block**, from a 10 ms timer's lateness (and
  `monitorEventLoopDelay`, reported for reference);
- **load dispatches**, by counting `load-package` messages posted to the load
  worker pool;
- **main-thread Malloy compiles**, by wrapping `getModel` on the materializer
  `Runtime.loadModel` returns in the `@malloydata/malloy` the bundle resolves,
  each attributed to the bundle function that asked for it.

It hooks only the runtime and the external Malloy package, so it works unchanged
against any build, a release that predates it included.

## The verdict

Each side's figure is the median over its rounds. The two counts fail if any
round is over. On `ubuntu-latest` (`verdict.ts` has the ranges): 0.12.6's
`/status` p90 is 370 to 660 ms, which is why the budget is 1000 ms rather than
the 500 ms a laptop suggests; 0.13.1's is 1.5 to 2 s.

| check | kind | limit | catches |
|---|---|---|---|
| main-thread CPU per op vs baseline | relative | 1.5x | more synchronous work per load, bind or poll (0.13.1's `formatArrayQueryJoins`, its governor tick, its served-copy compiles) |
| `/status` p90 vs baseline | relative | 2x, once the candidate's p90 is over 150 ms | a heartbeat that does more per poll or waits behind more main-thread work |
| `/status` p90 under the storm | absolute | 1000 ms | the same, when the baseline is as bad |
| longest event-loop block | absolute | 3000 ms | a gross stall: one synchronous section of seconds (0.12.5's storm went silent for 47 s). It does not separate 0.13.1 from 0.12.6, whose longest blocks overlap at 1 to 2 s on a CI runner |
| served-copy compiles on an identical re-bind | absolute | 0 | re-binding the manifest a version already holds recompiling its served copy on the main thread (0.13.1 cause 1) |
| loads started by `/status` | absolute | 0 | a heartbeat that starts a package load |

A run that did not do the work is **invalid** (exit 2), not a pass: an operation
that never answered, a probe that saw no load dispatch, a failed re-bind, or the
two sides publishing more than 10% differently.

### Which baseline, and why

- **Pull request: the merge base.** Everything but the PR's own change is the
  same on both sides, so a ratio over the limit is the PR.
- **Nightly and release: the anchor release** in `anchor.json` (0.12.6 today),
  not the release before. Comparing each release with the previous one lets a
  regression arrive in steps of 1.4x that never fail. Against a fixed anchor
  they add up and fail once their product passes the limit. The anchor moves
  forward only after a release has run in the fleet without main-thread trouble,
  in a PR of its own; moving it is the deliberate act of accepting today's cost.
- **Absolute budgets** hold on every run, so a main that is already bad, as main
  was when this landed, cannot pass by being compared with itself.

## Reading a failure

1. Open the job summary: the check table, then every round of both sides. A
   relative failure with every round of the candidate above every round of the
   baseline is real; one that rests on a single round is worth a re-run.
2. **Served-copy compiles on an identical re-bind**: the detail line names the
   bundle function that asked Malloy for each compile. Re-binding an identical
   manifest should leave the served copy as it is.
3. **CPU per op, `/status` or a long block**: profile both sides and diff them.

   ```bash
   bun run perf:main-thread --baseline git:<base> --candidate ../.. --rounds 1 --cpu-prof --out /tmp/mtr
   bun tests/perf/main-thread/prof.ts /tmp/mtr/prof-baseline-1/*.cpuprofile /tmp/mtr/prof-candidate-1/*.cpuprofile
   ```

   `--cpu-prof` profiles the main thread only. `prof.ts` lists the functions
   whose inclusive time grew most, then each side's top self time. For 0.13.1
   against 0.12.6 the growth is `judgeLoadGates` -> `formatArrayQueryJoins` ->
   `walk`, `listVersionEntries` and `getPackageMetadata` under `/status`, and the
   served copy's `compile`. Profiles are written when the server exits, which the
   storm does gracefully. The profile includes setup and shutdown, so compare
   functions, not totals.
4. The CI artifact has every run's JSON and both servers' logs.

## Keeping it true

- The fixtures may only use syntax every compared build accepts; the preflight
  drops a package one side refuses and says so in the log.
- The server settings in `storm.ts` (`SERVER_ENV`) mirror a worker with
  materialization on. Change them when the fleet's worker settings change.
- The limits were calibrated on GitHub-hosted `ubuntu-latest` (4 vCPU, 16 GB), which is why the
  job is pinned there, against
  0.12.6, 0.13.1 and the three fixes for ms2data/service#7795 merged together;
  the PR that added this check has the numbers. Recalibrate before tightening a
  limit, and before making the check required.
