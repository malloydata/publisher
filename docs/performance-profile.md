<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Performance profile: where load and query time goes

> What this is: an attribution of the [baseline](performance.md#baseline) to code, and a ranked list
> of what to change. Nothing here changes Publisher; every number comes from the production build of
> Publisher 0.9.7 on the machine the baseline ran on (Apple M5 Pro, 18 cores, 48 GiB, Bun 1.4.2), with
> the generated `gen` package at the medium and large presets and `storefront`. Profiled on
> 2026-10-11, on a machine that was not idle (other sessions were open), so read small percentages as
> approximate.

## Summary

1. **Half of every package load runs on the main thread, and it blocks it.** A medium load is ~950
   ms; the worker accounts for 436 ms of it. The other ~515 ms run on the main thread after the worker
   returns: the persist build plan **recompiles every model file from scratch** (330 ms), and
   render-tag validation prepares every annotated view (167 ms). Neither yields, so the event loop
   stalls for up to 135 ms (medium) and 1.1 s (large) per step. That is the 12–99× slowdown every
   other request sees during a load.
2. **One DuckDB connection per package serializes its queries.** At every concurrency above 1, the
   package's connection has a query running 100 % of the time, the in-flight count equals the VU
   count, and the time inside DuckDB equals the HTTP latency. Both plateaus in the baseline (gen ~68
   req/s, storefront ~530 req/s) are that one connection; the main thread is 17–33 % busy at those
   points. DuckDB alone, on the same data, nearly doubles its throughput with 4 connections.
3. **Compiles redo work they already did.** Each model file gets a fresh Malloy `Runtime`, so every
   import is re-parsed for every file that imports it, in the worker and again in the build plan; a
   shared compile cache halves the worker's compile. Every query against a model rebuilds that
   model's whole `QueryModel` (0.6 ms per query on the medium package, 2.3 ms on the large one), and
   every Malloy document rebuilds the dialect function table (0.14 ms).
4. **Per-query main-thread work is small but avoidable.** ~0.85 ms per storefront request and ~2.8 ms
   per gen request. A fifth of storefront's is re-deriving the model's report facts on every query.

The [ranked list](#ranked-opportunities) is at the end.

## Method

All tooling lives in [`packages/server/k6-tests/perf/profile/`](../packages/server/k6-tests/perf/profile/)
and touches nothing in `src/`. It instruments the production build (`dist/`) for the length of a
profiling session and is removed by `patch-dist.ts restore` or by the next `bun run build`.

| Tool                                                                        | What it measures                                                                                                                                                                                                                                                                                                                                                                                            |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`hook.mjs`](../packages/server/k6-tests/perf/profile/hook.mjs)             | Inert unless `PERF_PROFILE_CTL` is set. CPU profiles of the main thread and of each package-load worker through `node:inspector`'s `Profiler` domain, which works on both under Bun 1.4.2; event-loop delay from a 1 ms `setInterval` drift probe; with `PERF_PROFILE_TRACE=compile`, every `Malloy.compile` on the main thread grouped by its caller; with `duckdb`, time in DuckDB and queries in flight. |
| [`patch-dist.ts`](../packages/server/k6-tests/perf/profile/patch-dist.ts)   | Prepends the hook to `dist/package_load_worker.mjs` (a `--preload` does not reach worker threads) and wraps twelve load phases in `dist/server.mjs` with timers.                                                                                                                                                                                                                                            |
| [`ctl.ts`](../packages/server/k6-tests/perf/profile/ctl.ts)                 | Serves the packages with the hook on :4200/:4240, opens a window around N sequential loads or around a k6 `queries.ts` run, and joins phases with event-loop stalls.                                                                                                                                                                                                                                        |
| [`analyze.ts`](../packages/server/k6-tests/perf/profile/analyze.ts)         | Self and total time per package, source file and function from a `.cpuprofile`, mapping bundle lines back to `src/` modules; `--callers` for a function's callers.                                                                                                                                                                                                                                          |
| [`experiments.ts`](../packages/server/k6-tests/perf/profile/experiments.ts) | Standalone measurements that size a fix before it exists: compile cache, shared `QueryModel`, DuckDB connections, dialect namespace.                                                                                                                                                                                                                                                                        |

Why these and not others: `bun --cpu-prof` works but profiles the main thread only, over the whole
process lifetime. Bun's `performance.eventLoopUtilization()` returns zeros, and the server's own
`publisher_event_loop_lag_*` gauges sample at 20 ms and reset on every scrape, too coarse to see a
100 ms stall land on a phase. Bun keeps `async` frames in `Error().stack`, which is what lets the
compile trace name the Publisher function behind each compile even though the sampling profiler's
stacks stop at the first `await`.

Reproduce, from `packages/server/k6-tests` after `bun run build` at the repo root:

```bash
bun perf/profile/patch-dist.ts apply
D=/tmp/pubprof                                    # any scratch directory
bun perf/profile/ctl.ts serve $D PERF_PROFILE_TRACE=compile,duckdb &   # the commands below wait for it

# Area 1 and 3: loads (window names are printed; profiles land in $D/out)
bun perf/profile/ctl.ts loads $D register genm 10          # phases + event-loop delay
bun perf/profile/ctl.ts loads $D register genm 6 --prof    # + main and worker CPU profiles
bun perf/profile/ctl.ts loads $D register genl 4
bun perf/profile/ctl.ts loads $D reload gen 10
bun perf/profile/ctl.ts phases $D register-genm-10-noprof
bun perf/profile/analyze.ts $D/out/register-genm-6-prof.main.cpuprofile 40
bun perf/profile/analyze.ts $D/out/register-genm-6-prof.worker1.cpuprofile 40

# Area 2 and 4: queries (k6 queries.ts, window over the middle of the run)
bun perf/profile/ctl.ts queries $D gen 1 20
bun perf/profile/ctl.ts queries $D gen 32 20 --prof
bun perf/profile/ctl.ts queries $D storefront 32 20 --prof

kill %1; bun perf/profile/patch-dist.ts restore

# Standalone experiments
bun perf/profile/experiments.ts compile-cache $D/pkgs/genm
bun perf/profile/experiments.ts query-model $D/pkgs/genl index.malloy "run: sales_3_1 -> by_month_status" 100
bun perf/profile/experiments.ts duckdb-conns $D/pkgs/genm/data
bun perf/profile/experiments.ts namespace

# Worker-pool sizing: the ordinary harness
bun run perf:loads --targets gen --preset medium --duration 30 --warmup-rounds 1 \
  --server-env PACKAGE_LOAD_WORKERS=2
```

Profiling costs something: with the CPU profiler on, a medium register took ~1040 ms instead of
~950 ms. Phase timings and event-loop numbers below come from windows without the profiler.
Function names below are the `src/` functions; line numbers are in the current tree.

## 1. Main-thread blocking during package loads

### Event-loop delay

| Window (sequential, 1 load at a time) | p50     | p99     | p99.9  | max         | stalls > 100 ms | loop blocked |
| ------------------------------------- | ------- | ------- | ------ | ----------- | --------------: | -----------: |
| idle, 5 s                             | 0.14 ms | 0.22 ms | 5.3 ms | 7.8 ms      |               0 |          1 % |
| 10 medium registers                   | 0.13 ms | 14.4 ms | 118 ms | **135 ms**  |              11 |         49 % |
| 10 medium reloads                     | 0.13 ms | 13.8 ms | 114 ms | **130 ms**  |              10 |         49 % |
| 4 large registers                     | 0.13 ms | 23.7 ms | 49 ms  | **1142 ms** |               8 |         57 % |

"Loop blocked" is the sum of the 1 ms timer's lateness above 2 ms over the window. Each medium load
leaves the loop unable to run for about half its duration, in two or three ~100–130 ms stalls. A
request that arrives during a stall waits for it: in the mixed scenario, report reads whose own work
is ~1 ms reach a p95 of 98–143 ms under load, and cheap queries 167–169 ms.

### Where the time goes

Phase timers around each load, averaged per load (`ctl.ts phases`; medium n=10, large n=4):

| Phase (thread)                                                           | medium avg |   large avg | event-loop stalls > 20 ms inside it       |
| ------------------------------------------------------------------------ | ---------: | ----------: | ----------------------------------------- |
| whole `Package.create`                                                   |     953 ms |     5365 ms |                                           |
| worker: manifest, compile, extract (off-thread)                          |     436 ms |     2198 ms | large: 4 stalls, 193 ms (result hand-off) |
| **build plan** `computePackageBuildPlan` (main)                          | **330 ms** | **1599 ms** | medium 12 / 1242 ms; large 112 / 5498 ms  |
| **render-tag validation** `Model.validateRenderTags`, all models (main)  | **167 ms** | **1500 ms** | medium 9 / 782 ms; large ≥ 11 / 2422 ms   |
| `readDatabases` (main, overlaps the worker)                              |      15 ms |       17 ms |                                           |
| report discovery + lint `discoverReports("load")` (main)                 |       8 ms |       21 ms |                                           |
| package copy (register only)                                             |       5 ms |        9 ms |                                           |
| `_updatePackageLocked` (register: records the location, relints reports) |       4 ms |        5 ms |                                           |
| `Model.fromSerialized`, all models                                       |     1.1 ms |      1.8 ms |                                           |
| manifest bind, serve rebind, `notifyPackageLoaded` (embedding hooks)     |     < 1 ms |      < 1 ms |                                           |

So of a medium load, 46 % is the worker and ~52 % is main-thread work that follows it; for large,
41 % and 58 %. Package copy, deserialization, report discovery, lint, query-boundary application,
logging and the retrieval hooks are each under 2 %. (This workload configures no embedding
provider, so `get_context` index builds were not exercised; `notifyPackageLoaded` returned in under
a millisecond.)

**The build plan** (`src/service/build_plan.ts:881`, `compilePackageBuildPlan`, called from
`Package.loadViaWorker`, `src/service/package.ts:1318`) loops over every `.malloy` file and compiles
it with a fresh runtime (`Model.getModelRuntime` → `runtime.loadModel(...).getModel()`), the same
compile the worker just finished. The compile trace counts 12 compiles per medium load (every file)
taking 332 ms of wall time, and 28 per large load. For a model without `##! experimental.persistence`
the loop then `continue`s (`build_plan.ts`, the `modelTag.has("experimental", "persistence")`
check), so for `gen` and `storefront`, which declare no persist sources, all of it is discarded. The
in-code comment above the call already notes the duplicate compile.

**Render-tag validation** (`src/service/model.ts:6072`, called per model from
`Package.hydrateWorkerModels`, `src/service/package.ts:983`) prepares every annotated view and named
query compile-only and runs the renderer's validator on the result: 268 prepares per medium load and
1,164 per large one. Each prepare `await`s, but nothing in it does I/O, so the whole loop for one
model runs as one macrotask: the large `index.malloy`, which re-exports all 144 sources, is a single
1.13 s stall. Per prepare, the dominant cost is rebuilding the model's `QueryModel` (see area 4):
in the large-package main-thread profile `QueryModelImpl` construction is 27.8 % of samples, about
940 ms per load (6 points of it are `randomUUID`, one per field node).

**The load metric stops early.** `malloy_package_load_duration` is recorded
(`src/service/package.ts:1263`) before `discoverReports` and the build plan run, which is why the
baseline's server-side load time (611 ms medium, 3.9 s large) is well under the HTTP time (952 ms,
5.6 s).

### Main-thread CPU profile of a medium load

With the profiler on, a medium load costs ~530 ms of main-thread CPU against ~435 ms in the worker
(10 sequential registers). Inclusive (total) and self time from a 6-register run (`analyze.ts`):

| Function (file)                                                                        |  total |  self |
| -------------------------------------------------------------------------------------- | -----: | ----: |
| `Malloy.compile` (`@malloydata/malloy` `api/foundation/compile.js:118`)                | 75.2 % |       |
| — ANTLR parse `runMalloyParser` (`lang/run-malloy-parser.js:53`)                       | 46.8 % |       |
| — `GlobalNameSpace` → `getDialectFunctions` (`lang/ast/types/global-name-space.js:17`) |  9.0 % | 2.0 % |
| `getPreparedResult` (`api/foundation/core.js:1504`), render-tag prepares               | 10.5 % |       |
| — `QueryModelImpl` construction (`model/query_model_impl.js:26`)                       |  7.7 % |       |
| `validateRenderTags` (`src/service/model.ts`), sync part                               |  8.5 % |       |
| `collectGivenRefs` (`src/service/given.ts:311`), via report facts                      |  3.9 % | 1.5 % |
| `randomUUID` (native, from `QueryNode`)                                                |        | 1.8 % |

Nearly all of the main thread's load work is Malloy compiling and preparing, and nearly all of that
is the build plan's recompile and the render-tag prepares.

## 2. Single-package query serialization

`ctl.ts queries` with `PERF_PROFILE_TRACE=duckdb`, 20 s runs, window over the middle 14 s:

| target     | VUs | total req/s | server CPU (cores) | DuckDB busy | DuckDB in flight (avg / max) | time in DuckDB, avg | HTTP avg (cheap) | loop blocked |
| ---------- | --: | ----------: | -----------------: | ----------: | ---------------------------: | ------------------: | ---------------: | -----------: |
| gen        |   1 |        59.7 |                8.2 |        87 % |                      0.9 / 1 |             15.2 ms |           4.8 ms |          9 % |
| gen        |   4 |        70.4 |                9.8 |       100 % |                      3.8 / 4 |             57.5 ms |          48.8 ms |         10 % |
| gen        |  32 |        68.2 |                9.9 |       100 % |                    31.3 / 32 |              469 ms |           513 ms |         17 % |
| storefront |   1 |       332.0 |                2.3 |        67 % |                      0.7 / 1 |              2.1 ms |           1.2 ms |          1 % |
| storefront |  32 |       528.3 |                3.7 |       100 % |                    28.8 / 32 |               57 ms |            58 ms |         33 % |

"Time in DuckDB" wraps `DuckDBConnection.runDuckDBQuery` / `runSQLStream` and so includes waiting
for the connection. Read across a row: at 4 VUs and above, the package's connection always has a
query, every VU's request is inside DuckDB, and the time inside DuckDB is the HTTP latency. The
throughput ceiling is one query at a time: gen's ~14.5 ms of DuckDB service per query caps it at
~69 queries/s, which is the plateau (68–70 req/s); storefront's ~2 ms caps it at ~500/s, which is its
plateau (~530 req/s including report reads, which skip DuckDB). Neither is a Publisher lock: the
query gate (`PUBLISHER_MAX_CONCURRENT_QUERIES=32`) admits all 32, and the main thread is 9–33 % busy.

The cause is in how the package's sandbox is built: `Package.buildPackageMalloyConfig`
(`src/service/package.ts:3938`) gives each package one `:memory:` DuckDB, and
`@malloydata/db-duckdb`'s `DuckDBConnection` holds a single `this.connection` that every query runs
on. DuckDB runs one statement per connection at a time. It parallelizes inside a query, which is
why one gen query at a time already keeps ~8 cores busy and why the server never uses more than ~10
of 18.

DuckDB on its own (`experiments.ts duckdb-conns`, the gen data and three representative queries):

| threads per instance | connections | clients |     q/s | p50     | cores |
| -------------------- | ----------: | ------: | ------: | ------- | ----: |
| default (all)        |           1 |       1 |     211 | 4.9 ms  |   7.9 |
| default              |           1 |       4 |     211 | 18.2 ms |   8.1 |
| default              |           1 |      16 |     211 | 77.5 ms |   8.2 |
| default              |           4 |       4 | **391** | 8.8 ms  |  16.1 |
| default              |          16 |      16 |     372 | 30.0 ms |  16.5 |
| 4                    |          16 |      16 |     379 | 31.2 ms |  15.8 |
| 1                    |          16 |      16 | **413** | 30.6 ms |  15.3 |

One connection gives the same 211 q/s at 1, 4 and 16 clients with latency growing linearly, which is
the gen baseline's shape exactly. Four connections nearly double it (the rest is CPU: 16 of 18 cores).
On storefront's small tables (`duckdb-conns <storefront>/data sf`, two simple aggregates) the same
shape holds and the headroom is larger, because a small query uses few cores: one connection gives
~3,000 q/s at 1, 4 and 16 clients, 4 connections ~8,100 and 16 connections ~15,000 (23,500 with
`threads=4`). Publisher's storefront queries spend ~2 ms each inside the connection wrapper, far more
than these bare queries, so treat the multiple as an upper bound, not a forecast.

Two smaller costs ride along. Each package is its own DuckDB instance with `threads` and
`memory_limit` sized to the whole machine, so N busy packages oversubscribe cores N times over (the
server already warns about the memory side at boot). And on the main thread, a gen query costs ~2.8
ms of CPU (area 4), so with the connection limit removed the main thread would cap gen at roughly
350 req/s.

## 3. Package load cost

Per KB of Malloy text, a load splits into three roughly linear parts: the worker (~3.8 ms/KB at
medium and large), the build plan's recompile (~2.8 ms/KB) and render-tag validation (1.5 ms/KB at
medium, 2.5 ms/KB at large — it grows with views × model size, because each prepare rebuilds a
`QueryModel` over the whole model).

### Inside the worker

Worker CPU profiles (`analyze.ts` on `*.worker1.cpuprofile`; medium: 457 ms of CPU per load, large:
2,230 ms):

| Function                                                                      | medium total | large total |
| ----------------------------------------------------------------------------- | -----------: | ----------: |
| `Malloy.compile`                                                              |       77.9 % |      75.3 % |
| — ANTLR parse (`runMalloyParser`)                                             |       56.2 % |      56.8 % |
| `postMessage` (structured clone of the result to the main thread)             |        8.2 % |      10.8 % |
| `modelInfoOf` → Malloy `modelDefToModelInfo` (`src/service/model_info.ts:15`) |        7.4 % |       8.0 % |
| `collectRetiredRouteMarkers` (`src/service/gate_classification.ts`)           |        5.4 % |       5.0 % |
| `collectSourceInfos` (`src/service/source_extraction.ts`)                     |        4.1 % |             |

Parsing is more than half the worker. Much of it is parsing the same text again:
`compileMalloyModel` (`src/package_load/package_load_worker.ts:830`) builds a fresh `Runtime`, with
no cache, for every model file (`buildRuntimeForModel`, line 803), and `loadPackage` compiles all
files at once with `Promise.all` (line 1282). In the medium package `base.malloy` is translated once
for every file that imports it, directly or through `index.malloy` (12 times), and each `model_NN`
two to three times. Malloy already has a URL-keyed `CacheManager`; it only needs a reader that
returns an invalidation key, and one cache per load:

| Compile every file of the package (`experiments.ts compile-cache`) | storefront (6 files) | medium (12) | large (28) |
| ------------------------------------------------------------------ | -------------------: | ----------: | ---------: |
| fresh `Runtime` per file (what the worker and the build plan do)   |                52 ms |      341 ms |    1734 ms |
| one shared `CacheManager`, files one at a time                     |                23 ms |  **142 ms** | **865 ms** |
| one shared `CacheManager`, `Promise.all`                           |                36 ms |      264 ms |    1003 ms |

A shared cache cuts compile by 50–58 %, but only if files compile in dependency order: under
`Promise.all` the files race and miss each other's cache entries.

### More workers

`PACKAGE_LOAD_WORKERS` only adds parallelism between loads: one load is still one worker job plus
the main-thread tail. The harness at `--preset medium --duration 30` (same session, so comparable to
each other, not to the baseline):

| `PACKAGE_LOAD_WORKERS` | register c1 avg | register c4 req/s | register c4 avg / p95 | reload c4 req/s | reload c4 avg / p95 |
| ---------------------: | --------------: | ----------------: | --------------------: | --------------: | ------------------: |
|                      1 |          998 ms |              1.39 |        2757 / 3613 ms |            1.41 |      2812 / 3547 ms |
|                      2 |          972 ms |              1.55 |        2461 / 2681 ms |            1.56 |      2542 / 2783 ms |
|                      4 |          966 ms |              1.58 |        2526 / 2752 ms |            1.62 |      2464 / 2553 ms |

A single load does not get faster, and four concurrent loads gain only 11–15 % throughput and ~25 %
on p95. The ceiling is the main-thread tail: with ~515 ms of every medium load serialized on the main
thread, no number of workers can exceed ~1.9 loads/s, and the workers' compiles also compete with
the main thread for the cores DuckDB and k6 are using. More workers become worth it after #1, #2 and
#8 below move that tail.

## 4. Main-thread CPU per query

Main-thread CPU profiles under 32 VUs (`ctl.ts queries … --prof`). storefront: 6.2 s of main-thread
CPU over a 14 s window, ~0.85 ms per request, the thread busy ~33 % of the time. gen: ~2.8 ms per
query, busy ~21 %.

| Function (file), total time                                                                                      | storefront |        gen |
| ---------------------------------------------------------------------------------------------------------------- | ---------: | ---------: |
| `getQuery` (`src/controller/query.controller.ts`)                                                                |     32.5 % |            |
| `Malloy.compile` of the query text                                                                               |     30.1 % |     22.7 % |
| — `GlobalNameSpace` → `getDialectFunctions`, rebuilt for every document                                          |     17.6 % |     10.3 % |
| `Model.carriesReportTag` → `getReportModelFacts` → `readReportModelFacts` (`src/service/report_manifest.ts:613`) | **20.5 %** |          — |
| — `collectGivenRefs` (`src/service/given.ts:311`)                                                                |     13.4 % |            |
| `getPreparedResult` (SQL generation)                                                                             |      8.4 % |     28.1 % |
| — `QueryModelImpl` construction, rebuilt for every query                                                         |      8.3 % | **21.1 %** |
| render-tag validation of each result (`@malloydata/render-validator`, renderer `setResult`)                      |     10.9 % |            |
| `collectCallerJoins` / `assertEveryCallerJoinPlaced` (`src/service/caller_joins.ts:203`)                         |            |      9.2 % |
| row conversion (`rowDataToDate`, luxon)                                                                          |      3.4 % |      8.9 % |
| express, body parsing, logging, metrics                                                                          |      < 3 % |      < 2 % |

Three of these are the same work done again on every request:

- **`carriesReportTag`** (`src/service/model.ts:5830`) is called on every query
  (`query.controller.ts:165`, and the MCP `execute_query` tool) to filter render logs, and re-derives
  the model's report facts from the whole `ModelDef` each time. The model is immutable once
  hydrated.
- **The `QueryModel`**: Malloy reuses the base model's `QueryModel` for a query that does not modify
  the model (`compile.js:207`, `existingQueryModel`) only if the base model already has one, and the
  model Publisher hydrates from the worker's `ModelDef` never builds one, so every query builds a
  `QueryModel` over every source and field in the file and drops it. Building it once
  (`experiments.ts query-model`):

  | model                     | QueryModel per query | shared QueryModel |
  | ------------------------- | -------------------: | ----------------: |
  | storefront                |             0.267 ms |          0.208 ms |
  | gen medium `index.malloy` |             0.580 ms |          0.269 ms |
  | gen large `index.malloy`  |             2.339 ms |      **0.386 ms** |

- **The dialect namespace**: Malloy builds a `GlobalNameSpace` for every document it translates, and
  each one expands every dialect's function blueprints: 0.146 ms each (`experiments.ts namespace`).

The report reads' 80 ms p95 at 32 VUs is not their own work (0.2 ms at 1 VU). It is waiting for the
event loop: 32 query completions each bring ~0.85 ms of main-thread work, and the loop's p99 delay
is 14 ms in that window.

## Ranked opportunities

Ranked by expected effect on the baseline per unit of effort. "Verify" names the harness command and
the numbers that should move.

### Quick wins

| #   | Change                                                                                                                                                                                                                                                                            | Evidence                                                                                                                                                                              | Expected effect                                                                                                                                                                                                                                                                   | Effort | Risk                                                                                                                                                    | Verify                                                                                                 |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -----: | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| 1   | **Skip the build-plan recompile for models that cannot contribute.** Have the worker report, per model, whether it carries `##! experimental.persistence` or any `#@ preaggregate` (it already has the compiled `ModelDef`), and compile only those in `compilePackageBuildPlan`. | Build plan = 330 ms of 953 ms (medium), 1.6 s of 5.4 s (large); 12/112 stalls per window; the result is discarded for every model without the flag.                                   | Loads without persistence: medium register/reload −35 % (950 → ~620 ms), large −29 % (5.6 → ~4.0 s); c4 throughput up by the same share; about half of the main-thread stall time during loads gone, so mixed under-load p95 roughly halves. Packages that do persist: unchanged. |      S | Low, if the gate comes from the compiled model rather than a text scan. No contract change.                                                             | `perf:loads --targets gen` (all presets): register/reload avg and p95; `perf:mixed`: p95 ratios.       |
| 2   | **Build the hydrated model's `QueryModel` once** (e.g. touch `queryModel` on the materializer's base model after `Model.fromSerialized`), so every query and every render-tag prepare reuses it.                                                                                  | `QueryModelImpl` construction: 21 % of gen query main-thread CPU, 8 % of storefront's, ~940 ms per large load. Standalone: 0.58 → 0.27 ms (medium), 2.34 → 0.39 ms (large) per query. | Large load −0.9 s (with #1: ~5.6 → ~3 s); medium −~50 ms. gen per-query main-thread CPU −20 %; storefront −8 %. Does not move gen throughput until #6 lands (DuckDB-bound).                                                                                                       |      S | Low. Memory: each model keeps one `QueryModel` alive (it is built per query today and dropped). Queries that do modify the model still build their own. | `perf:loads --preset large`; `perf:queries`: storefront c32 p95; profile gen c32 for `QueryModelImpl`. |
| 3   | **Yield inside long main-thread loops**: `await setImmediate` between render-tag targets, between build-plan files and between hydrated models.                                                                                                                                   | Stalls of 100–135 ms (medium) and 1.1 s (large) are one model's loop run as one macrotask.                                                                                            | No change to load time; the longest stall drops to the largest single compile or prepare (tens of ms). Mixed: report_get/report_list under-load p95 from ~100–140 ms toward ~10–30 ms, query p95 ratios well below 10×.                                                           |      S | Low. Interleaves requests with a load in progress, which is already allowed (they run against the previous model).                                      | `perf:mixed`: ratio table; `ctl.ts loads … ` event-loop max.                                           |
| 4   | **Memoize `Model.carriesReportTag()` / `getReportModelFacts()`** per model instance.                                                                                                                                                                                              | 20.5 % of storefront main-thread CPU per query; pure function of an immutable `ModelDef`.                                                                                             | storefront main-thread CPU per request −20 % (~0.17 ms); report read p95 at 32 VUs down with it. No throughput change while DuckDB-bound.                                                                                                                                         |      S | Very low; a reload builds new `Model` objects.                                                                                                          | `perf:queries --targets storefront`: report p95 at c32.                                                |
| 5   | **Record `malloy_package_load_duration` after the main-thread tail**, or add a histogram for it (build plan already has one; render-tag validation has none).                                                                                                                     | Metric reads 611 ms for a 952 ms load (medium), 3.9 s for 5.6 s (large).                                                                                                              | Measurement only.                                                                                                                                                                                                                                                                 |      S | **Changes a metric's meaning**: dashboards and alerts on it would jump. Prefer adding a phase histogram.                                                | Compare server histograms with client times in `report.md`.                                            |

### Structural changes

| #   | Change                                                                                                                                                                                                                                                                         | Evidence                                                                                                                                                                             | Expected effect                                                                                                                                                                                                 |                     Effort | Risk                                                                                                                                                                                                                                                            | Verify                                                                    |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------: | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| 6   | **More than one DuckDB connection per package** (a small pool on the package's instance, e.g. 4), or one shared instance with a pool and a `threads` budget.                                                                                                                   | Area 2: one connection is busy 100 %, in-flight = VUs, DuckDB time = HTTP latency; standalone 211 → 391 q/s with 4 connections.                                                      | gen: ~68 → ~120–130 req/s at ≥ 4 VUs and p95 at 32 VUs roughly halved (CPU-bound beyond that on 18 cores). storefront: past ~530 req/s until the main thread binds (~0.85 ms/request, more with #2/#4/#7 done). |                        M–L | Medium. Session state is per connection: `SET`s, `applyExtensionSessionSettings`, ATTACHed databases, temp tables, persist/materialization writes. Likely needs `@malloydata/db-duckdb` support or a pooling wrapper in Publisher. Memory grows per connection. | `perf:queries` both targets: total req/s and p95 at 4–32 VUs; server CPU. |
| 7   | **One compile cache per package load, compiled in import order**, in the worker (and in whatever compile #1 leaves in the build plan).                                                                                                                                         | Parse is 56 % of the worker; `base.malloy` translated 12× per medium load; standalone compile −58 % (medium), −50 % (large) with a shared `CacheManager`, worse under `Promise.all`. | Worker −~45 %: medium ~436 → ~240 ms, large ~2.2 → ~1.2 s per load. With #1–#2: medium load ~950 → ~450 ms, large ~5.6 → ~2 s.                                                                                  |                          M | Medium. Malloy returns a cached `ModelDef` without its `problems` (a TODO in `compile.js`), so warnings from an imported file must still be reported once; invalidation keys must reflect `replacement` text for `/compile` previews.                           | `perf:loads` all presets; worker compile histogram.                       |
| 8   | **Move render-tag validation off the main thread** (into the worker, if `@malloydata/render-validator` loads there without the DOM-mutating renderer, or into a second worker), or validate once per distinct view instead of per file (`index.malloy` re-exports every view). | 167 ms (medium) and 1.5 s (large) per load on the main thread; the large `index.malloy` alone is a 1.1 s stall.                                                                      | With #2, the remaining ~10–15 % of main-thread load CPU leaves the main thread.                                                                                                                                 |                          M | Medium: the reason it is on the main thread is the renderer's DOM globals; needs proof the validator alone is safe in the worker.                                                                                                                               | `ctl.ts loads` phases; `perf:mixed`.                                      |
| 9   | **Upstream (Malloy): build the dialect function table once** and reuse it across `GlobalNameSpace` instances (invalidate on dialect registration).                                                                                                                             | 0.146 ms per document; 17.6 % of storefront per-query main-thread CPU, ~10 % of gen's and of load compiles.                                                                          | storefront main-thread per request −~0.15 ms (−17 %); loads −5–10 %.                                                                                                                                            | S upstream + a Malloy bump | Low.                                                                                                                                                                                                                                                            | `perf:queries`; profile shows no `getDialectFunctions`.                   |
| 10  | **Cache per-result render-tag validation at query time** by query shape (model + query text or result schema), and look at caching `collectCallerJoins` per source.                                                                                                            | Render validation 10.9 % of storefront per-query main thread; caller joins 9.2 % of gen's.                                                                                           | −10–20 % main-thread CPU per query.                                                                                                                                                                             |                          M | Medium: the cache key must include everything the validator reads (tags, schema).                                                                                                                                                                               | `perf:queries` c32 main-thread profile.                                   |
| 11  | **Size `PACKAGE_LOAD_WORKERS` above 1** once the main-thread tail shrinks.                                                                                                                                                                                                     | Area 3: today 1 → 4 workers gives +11–15 % throughput at 4 concurrent medium loads and nothing for a single load, because ~515 ms per load is main-thread work.                      | After #1/#2/#8, concurrent-load throughput should scale with workers up to the core count; single loads unchanged.                                                                                              |                 S (config) | Low; each worker holds its own isolate (memory).                                                                                                                                                                                                                | `perf:loads --server-env PACKAGE_LOAD_WORKERS=N`.                         |

Together, #1–#4 are small, independent and do not change behavior or a public contract; on this
machine they should take a medium load from ~950 ms to ~570 ms, a large one from ~5.6 s to ~3 s, and
bring the mixed scenario's under-load ratios down by an order of magnitude. Query throughput will not
move until #6, because both packages' plateaus are their DuckDB connection.

## What was not measured

- **Embedding and `get_context` index builds.** No embedding provider was configured, so the
  retrieval hooks did no work (`notifyPackageLoaded` < 1 ms). A deployment with one configured needs
  its own profile.
- **The main thread's cost of receiving the worker's result.** Structured-clone deserialization is
  native and does not appear in the JS profile. It shows only as stalls inside the worker phase: none
  over 20 ms for medium, ~48 ms per large load.
- **DuckDB's own time vs. queueing on the connection.** The wrapper sees both together; the split
  comes from the standalone experiment, not from inside Publisher.
- **Per-thread CPU by name.** `ps -M` lists ~80 busy threads but cannot say which are DuckDB's; the
  core counts are whole-process.
- **What the ~2 ms per storefront query inside the DuckDB wrapper is.** The bare queries in the
  storefront experiment take ~0.3 ms; Publisher's generated SQL is heavier and the wrapper also runs
  `setup()` and row conversion, but the split was not measured. It sets storefront's ceiling, so it
  is the next thing to profile after #6.
- **Cold first loads and first queries**, by design of the harness (warm-up excluded).
