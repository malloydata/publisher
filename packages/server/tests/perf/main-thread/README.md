<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Main-thread regression check

A storm-lite A/B that fails when a change makes the server's main thread do much
more work under a fleet-like load, or makes `/status` slow. It covers the two
fleet freezes nothing in this repo measured:

- **[ms2data/service#7641](https://github.com/ms2data/service/issues/7641)**
  (0.12.1 to 0.12.5, the 2026-11-09 prod freeze): every load compiled a
  fingerprint per `#(index)` dimension against a model whose `QueryModel` was
  rebuilt each time, on the main thread.
- **[ms2data/service#7795](https://github.com/ms2data/service/issues/7795)**
  (0.13.1): a served-copy recompile on every manifest bind, a whole-ModelDef
  walk per load, the governor's `memoryUsage`, and `/status` building full
  metadata. `/status` p90 went from about 430 ms to 9 s in Credible's dev fleet.

The design follows Credible's worker-storm release check (ms2data/service#7640),
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
(one model, two persisted rollups), `gen-index` (30 sources over six model
files, re-exported by `index.malloy`, with 120 `#(index)` dimensions, four per
source, tagged as Credible's worker storm tags them) and `gov-arrays` (access
governed by a given over array columns and a repeated record). Each is written
as two versions.

1. It creates four environments and places every version in each (64 versions).
   Each materializing version is published once so its build plan names the
   sources a build manifest binds, and two manifests are written for it.
2. It drives 160 operations, eight at a time, from a seeded script that is the
   same for both sides: 35% publishes (a materializing version sends its
   `manifestLocation`, so the bind path runs), 20% version unloads, 10% reloads,
   10% manifest binds that change the binding, 10% re-binds of the identical
   manifest, 7% `GET …/entities` (what an indexer reads), 5% MCP `get_context`
   (it has no REST form; the storm calls the MCP port), 3% environment patches.
   Two pollers read `/status?includeLoading=true` every 200 ms throughout, as
   the control plane's heartbeat does. The script is drawn before the storm, so
   an unload or reload may name a version that is not loaded at that moment
   (a 404); choosing only loaded ones would make the script depend on timing,
   and then the two sides would not run the same one.
3. Quiet checks, each in its own probe window, after waiting for two seconds
   with no compile, QueryModel build or load (a run that never gets there is
   invalid). Two run before the storm, because the storm starts background work
   (a `get_context` index build reading `#(index)` values) that translates
   queries on the main thread for minutes afterwards:
   - one ad-hoc query against `mat-mini` (a positive control: it must count at
     least one compile), then three identical re-binds of the same version;
   - the first load of a `gen-index` version, then three `/entities` reads.

   One runs after it: twenty `/status` polls after unloading three versions.

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
- **main-thread Malloy translations**, by wrapping the static `Malloy.compile`
  in the `@malloydata/malloy` the bundle resolves (every path that translates
  Malloy text goes through it), each attributed to the bundle function that
  asked for it;
- **main-thread `QueryModel` builds**, by wrapping
  `QueryModelImpl.loadModelFromDef`, with their time and callers.

It hooks only the runtime and the external Malloy package, so it works unchanged
against any build, a release that predates it included. Each snapshot reports
which hooks took; a run with a hook that did not, or whose positive controls
counted nothing (the ad-hoc query's compile, any `QueryModel` build over the
storm), is invalid rather than a pass.

## The verdict

Each side's figure is the median over its rounds. The two counts fail if any
round is over. On `ubuntu-latest` (`verdict.ts` has the ranges): 0.12.6's
`/status` p90 under this storm is 0.8 to 1.3 s, which is why the budget is
1600 ms rather than the 500 ms a laptop suggests; 0.13.1's is 2 to 2.7 s, and
`main` with the #7795 fixes 0.17 to 0.24 s.

| check | kind | limit | catches |
|---|---|---|---|
| main-thread CPU per op vs baseline | relative | 1.5x | more synchronous work per load, bind or poll (0.13.1's `formatArrayQueryJoins`, its governor tick, its served-copy compiles) |
| `/status` p90 vs baseline | relative | 2x, once the candidate's p90 is over 150 ms | a heartbeat that does more per poll or waits behind more main-thread work |
| `/status` p90 under the storm | absolute | 1600 ms | the same, when the baseline is as bad |
| longest event-loop block | absolute | 3000 ms | a gross stall: one synchronous section of seconds (0.12.5's storm went silent for 47 s). It does not separate 0.13.1 from 0.12.6, whose longest blocks overlap at 1 to 2 s on a CI runner |
| main-thread Malloy compiles on an identical re-bind | absolute | 0 | re-binding the manifest a version already holds and translating Malloy on the main thread for it: 0.13.1's served-copy recompile, and 0.12.0 to 0.12.5's `compileErrorOf` |
| loads started by `/status` | absolute | 0 | a heartbeat that starts a package load |
| main-thread QueryModel builds on an indexed load | absolute | 20 | work per `#(index)` dimension on the main thread: 0.12.1 to 0.12.5 build 256 for `gen-index`'s 120 dimensions (`buildDimIndexFingerprintLookup`), every other build 4 |

The relative CPU check does not catch #7641 at this fixture's size: its
fingerprints cost about 50 ms per load here, against about 1 s per load on the
prod packages in #7641. The QueryModel count does, at any size, and it costs
nothing to run.

A run is **invalid** (exit 2), not a pass or a fail, when:
- an operation never answered;
- the probe saw no load dispatch, a hook did not take, or a positive control
  counted nothing;
- a re-bind or the indexed load failed;
- an op type succeeded more than 10% (or 3) more often on one side. An op one
  side never serves, like `/entities` before 0.12.1, is noted and not compared;
- the runner is not the calibrated kind (`--expect-cpus 4` in CI);
- a quiet check never got quiet;
- the baseline itself is far over budget (`/status` p90 over three times its
  budget, or a block over 3000 ms): an overloaded runner or a bad baseline.

A harness error (a failed install, a server that never booted, a fixture one
side cannot load) exits 2 too, with an `::error` titled *Main-thread harness
error (not a performance verdict)* and a summary saying to re-run the failed
jobs.

### Which baseline, and why

- **Pull request: the merge base.** Everything but the PR's own change is the
  same on both sides, so a ratio over the limit is the PR.
- **Nightly and release: the anchor release** in `anchor.json` (0.12.6 today,
  pinned by commit; the workflow refuses a tag that no longer names it),
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
- The limits were calibrated on GitHub-hosted `ubuntu-latest` (4 vCPU, 16 GB),
  which is why the job is pinned there, against 0.12.0, 0.12.1, 0.12.5, 0.12.6,
  0.13.1 and `main` with the ms2data/service#7795 fixes; the PR that added this
  check has the numbers. Recalibrate before tightening a
  limit, and before making the check required.
