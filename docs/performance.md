<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Performance: package loads and queries

> What this is: a load-test harness that measures **throughput and latency (average and p95, plus
> p50/p99/max)** of Publisher's two expensive operations — **loading a package** and **running a
> query** — each in isolation and both at once. It lives in
> [`packages/server/k6-tests/perf/`](../packages/server/k6-tests/perf/) and drives a dedicated
> Publisher with [k6](https://k6.io).

One command boots a fresh Publisher from this checkout's production build, generates the workload,
runs the scenario at each concurrency level, scrapes the server's `/metrics` around every run, stops
the server, deletes its temp directory and writes a results directory with a `report.md`.

## Run it

Prerequisites: `bun install && bun run build` at the repo root (the harness runs
`packages/server/dist/server.mjs` and builds nothing itself), and `k6` on your `PATH` (tested with k6 v2.0).
The data generator uses `@duckdb/node-api`, which the root install already provides.

From `packages/server/k6-tests`:

```bash
bun run perf:loads      # package load latency/throughput (register+delete, reload)
bun run perf:queries    # query latency/throughput per query class
bun run perf:mixed      # queries at a fixed rate while packages load
bun run perf:all        # all three, one server boot
```

Every script takes the runner's options after it, for example a quick check:

```bash
bun run perf:all --duration 10 --load-concurrency 1,2 --query-concurrency 1,4
```

**The baseline run** — the command whose numbers belong in [Baseline](#baseline). Run it on an
otherwise idle machine (no other test suites, builds or servers), on AC power:

```bash
bun run perf:baseline                                     # all scenarios, medium package, 60 s per run
# package-load cost vs. package size, one preset at a time:
bun run perf:loads --targets gen --preset small  --duration 60 --warmup-rounds 3
bun run perf:loads --targets gen --preset medium --duration 60 --warmup-rounds 3
bun run perf:loads --targets gen --preset large  --duration 120 --warmup-rounds 2
```

`perf:baseline` is `bun perf/run.ts all --preset medium --duration 60 --warmup-rounds 3`: about
25 minutes. Results land in `packages/server/k6-tests/results/perf-<timestamp>/` (git-ignored);
`report.md` there is the summary, and `bun run perf:report <dir>` regenerates it from the raw files.

### Options

| Option                          | Default             | Meaning                                                                                    |
| ------------------------------- | ------------------- | ------------------------------------------------------------------------------------------ |
| `<scenario>`                    | (required)          | `loads`, `queries`, `mixed`, a comma list of them, or `all`.                               |
| `--targets storefront,gen`      | `storefront,gen`    | Which packages each scenario measures (see [Workloads](#workloads)).                       |
| `--preset small\|medium\|large` | `small`             | Size of the generated package.                                                             |
| `--rows N` / `--seed S`         | `1000000` / `42`    | Fact-table rows and data seed for the generated package.                                   |
| `--duration S`                  | `20`                | Seconds per measured run; `mixed` uses S for each of its two windows.                      |
| `--warmup-rounds N`             | `1`                 | Warm-up before each measured run (see each scenario).                                      |
| `--load-ops register,reload`    | both                | Which load operations `loads` measures.                                                    |
| `--load-concurrency 1,2,4`      | `1,2,4`             | Concurrency levels for `loads`.                                                            |
| `--query-concurrency 1,4,16,32` | `1,4,16,32`         | Concurrency levels for `queries`.                                                          |
| `--rate R`                      | `10`                | `mixed`: query arrival rate, requests/s.                                                   |
| `--loader-vus N`                | `1`                 | `mixed`: VUs in each loader scenario.                                                      |
| `--weights a,b,c,d,e`           | `40,35,20,2.5,2.5`  | Query mix: cheap, medium, heavy, report list, report get.                                  |
| `--max-error-rate R`            | `0.05`              | Sanity threshold on `http_req_failed`; crossing it makes k6 exit 99 and the run say so.    |
| `--server-env KEY=VALUE`        | none                | Extra server environment, repeatable — e.g. `PACKAGE_LOAD_WORKERS=4`.                      |
| `--port P` / `--mcp-port P`     | `4200` / `4240`     | The dedicated server's ports. 4000/4040 and 4100/4140 are refused: other servers own them. |
| `--out DIR`                     | `results/perf-<ts>` | Results directory.                                                                         |
| `--keep-temp`                   | off                 | Keep the temp server root and generated package for inspection.                            |

### What the runner does

1. Generates the package into a fresh temp directory, and copies `examples/storefront` beside it
   (nothing in the repo is touched).
2. Writes a config with one environment, `perf`, serving both, and starts the server with
   `NODE_ENV=production DISABLE_RESPONSE_LOGGING=true LOG_LEVEL=warn`:

   ```bash
   bun run dist/server.mjs --server_root <temp> --config <temp>/publisher.config.json \
     --port 4200 --mcp_port 4240 --host 127.0.0.1 --no-mcp-config --init
   ```

   It waits for the `PUBLISHER_READY` line, and refuses to start if a package failed to load.

3. For each scenario × target × concurrency (× load op), scrapes `/metrics`, runs one k6 process with
   `--summary-export`, scrapes `/metrics` again and checks `/api/v0/status` for load errors.
4. Stops the server (SIGTERM, then SIGKILL after 15 s), deletes the temp directory, and writes
   `context.json`, `runs.json` and `report.md`. Ctrl-C does the same cleanup.

## Workloads

**storefront** — [`examples/storefront`](../examples/storefront) as it ships: four parquet/CSV
tables, ~25k order lines, one model and three reports. Queries run against `storefront.malloy`.

**gen** — a package generated by [`perf/gen-package.ts`](../packages/server/k6-tests/perf/gen-package.ts):

- `data/`: a fact table (`fact.parquet`, `--rows` rows, default 1M ≈ 18 MB) and three dimension
  tables (customers = rows/50, 2,000 products, 200 stores), written by DuckDB. Every value is a hash
  of the row number and the seed, so the same arguments always write the same data.
- `base.malloy`: the three dimension sources.
- `model_NN.malloy` × _models_: each declares _sources per model_ fact sources `sales_<m>_<s>` that
  join all three dimensions and carry seven fixed dimensions, six fixed measures and four fixed views
  (`kpis`, `by_month_status`, `by_category_region`, the nested `overview`), plus the preset's extra
  generated dimensions, measures and views.
- `index.malloy`: the surface — exports every fact source; the dimension sources stay hidden. Queries
  run against `index.malloy`, so every query passes the surface check.
- `overview.malloy` (grid) and `story.malloy` (linear): two reports over the first model.

| Preset   | Model files | Fact sources | Extra dims / measures / views per source | Malloy text |
| -------- | ----------: | -----------: | ---------------------------------------: | ----------: |
| `small`  |           2 |            4 |                                8 / 8 / 4 |      ~13 KB |
| `medium` |           8 |           32 |                              16 / 12 / 6 |     ~112 KB |
| `large`  |          24 |          144 |                              24 / 16 / 8 |     ~593 KB |

Generate one on its own to look at it: `bun run perf:gen-package /tmp/gen --preset medium`.

## Scenarios

Every measured request carries `phase=measure` and an `op` tag; warm-up requests carry
`phase=warmup` and are excluded from every number in the report. Thresholds exist only as an
error-rate sanity check (`--max-error-rate`); latency never fails a run.

### `loads`

One k6 run per load op × concurrency, executor **constant-vus** (N VUs back to back for
`--duration`): a closed loop, so the result is the throughput the server sustains at that concurrency
and the latency that costs.

| Op         | What one iteration does                                                                                                           |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `register` | `POST /api/v0/environments/perf/packages` with a unique name and the target's location: a cold install (copy, compile, register). |
| `delete`   | `DELETE …/packages/{name}` of the package that iteration just registered (reported, as it is part of the cycle).                  |
| `reload`   | `GET …/packages/{name}?reload=true`. Setup registers N copies of the target and each VU reloads its own, so N loads overlap.      |

Warm-up: `--warmup-rounds` rounds of N operations in parallel (`http.batch`) in `setup()`.

### `queries`

One k6 run per concurrency, executor **constant-vus**. Each iteration sends one request from a
fixed, interleaved weighted schedule, so two runs issue the same mix:

| Op             | Weight | storefront                                                | gen (rotated over up to 8 sources from different model files)     |
| -------------- | -----: | --------------------------------------------------------- | ----------------------------------------------------------------- |
| `query_cheap`  |     40 | one aggregate over `order_items`, no join                 | `-> kpis`: two counts over the fact table                         |
| `query_medium` |     35 | `-> sales_by_month`, `-> by_status`                       | `-> by_month_status`: group-by over the fact table                |
| `query_heavy`  |     20 | `-> business_overview` (nests), `-> category_performance` | `-> overview` (three joins, three nests), `-> by_category_region` |
| `report_list`  |    2.5 | `GET …/reports`                                           | `GET …/reports`                                                   |
| `report_get`   |    2.5 | `GET …/reports/{path}`, rotating                          | `GET …/reports/{path}`, rotating                                  |

Queries go through `POST …/models/{path}/query` with `compactJson: true`. Each VU holds one request
in flight, so the concurrency level is also the server-side query concurrency — note the server's
default `PUBLISHER_MAX_CONCURRENT_QUERIES` is 32, so at 32 VUs the cap is exactly reached.

Warm-up: every query text and report once, then `--warmup-rounds` × N requests of the mix.

### `mixed`

One k6 run per target, 2 × `--duration` long:

```
0 ─────────── D ─────────── 2D
queries  ═════════════════════════   constant-arrival-rate, --rate req/s, the same mix as `queries`
         window=baseline │ window=under_load
reload_same          ═════════════   --loader-vus VUs reloading the package being queried
load_other           ═════════════   --loader-vus VUs: register a copy of the OTHER package, reload it, delete it
```

Queries run at a **fixed arrival rate** (an open model), so the load-time window cannot hide
slowness by sending fewer requests: both windows see the same offered load. A query's window is the
one it started in. If k6 cannot start requests on time it reports `dropped_iterations`, and the
report says so. The loaders run with **constant-vus**.

## Reading the report

`report.md` opens with the context: Publisher version, git SHA and branch (and whether the tree was
dirty), CPU model, cores, RAM, OS, the 1-minute load average at the start and end of the run (a high
start value means something else was running), Bun/Node/k6 versions, server environment knobs, the
time to `PUBLISHER_READY`, and the size of each package.

Then one table per scenario, a row per target × concurrency × op:

| Column            | Meaning                                                                                            |
| ----------------- | -------------------------------------------------------------------------------------------------- |
| `conc` / `rate/s` | VUs (`loads`, `queries`) or the arrival rate (`mixed`).                                            |
| `reqs`            | Completed measured requests of that op.                                                            |
| `req/s`           | `reqs` ÷ the measured window (setup's end to teardown's start; for `mixed`, the window length).    |
| `avg` … `max`     | Client-side `http_req_duration` in ms (shown in s above 10 s): time from sending to the last byte. |
| `err %`           | Share of that op's requests that were not 2xx.                                                     |

For `mixed`, a second table compares each query op's avg and p95 in the `under_load` window with the
`baseline` window of the same run, as a ratio: 1.00× means loads did not disturb queries.

Under each scenario, a collapsed section lists the server's own histograms, as the difference
between the `/metrics` scrapes before and after the run: `malloy_package_load_duration` and its
compile / schema-fetch breakdown, `publisher_report_discovery_duration_ms`,
`publisher_report_read_duration_ms`, `malloy_model_query_duration`, and
`http_server_request_duration_ms` per route. Use them to cross-check the client timings: client
minus server is time on the wire and in Express; load duration minus compile is copy, queueing and
schema fetches. Server counts include warm-up, and their p50/p95 name the histogram bucket the
quantile falls in, because the buckets are too coarse to say more.

Raw files beside the report: per run, `*.summary.json` (k6 `--summary-export`), `*.k6.log`, and
`*.metrics-before.prom` / `*.metrics-after.prom`; plus `server.log`, `context.json` and `runs.json`.

## What limits accuracy

- **Client and server share the machine.** k6, the Publisher process, its package-load worker and
  DuckDB compete for the same cores, and nothing else should be running. The report records load
  average for that reason; a run whose start load average is well above zero is suspect.
- **One package-load worker by default.** `PACKAGE_LOAD_WORKERS` defaults to 1, so concurrent loads
  queue for the worker: `loads` at concurrency 2 and 4 measures that queue as much as compile speed.
  Re-run with `--server-env PACKAGE_LOAD_WORKERS=4` to see the pool's effect.
- **DuckDB and OS caching.** Parquet files are read through the OS page cache and DuckDB's own
  caches; after warm-up the queries measure warm reads, not first reads. Each package also has its
  own DuckDB connection, so concurrent queries against one package may serialize there.
- **First load vs. warm.** The server boot and the first load of each package (JIT, module loading,
  extension loading) happen before any measured run, and warm-up absorbs the rest. Boot time is in
  the context table; a cold first load is not measured separately.
- **Register includes a copy.** A register copies the package directory, data included, into the
  server root, so it scales with data size (18 MB at 1M rows) as well as model size. Reload re-reads
  the package from its location as well.
- **Closed-loop latency.** `loads` and `queries` use constant-vus, so latency there is measured at
  saturation; for latency at a given offered load use `mixed`'s baseline window, which is an open
  model.
- **Short windows.** At a 10–20 s window a heavy op contributes few samples and its p95/p99 is
  noisy; the baseline uses 60 s, and large packages need 120 s or more for enough reloads.

## Baseline

Recorded on 2026-10-12 with the [baseline commands](#run-it): `perf:baseline` (all three scenarios,
medium preset, 60 s per run, 3 warm-up rounds) and the `loads` command for the small and large
presets. The tables below condense the three `report.md` files; latencies are client-side
`http_req_duration` in ms, and every run finished with 0 % errors and no dropped iterations.

| item         | value                                                                                                                                                                                                  |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Publisher    | 0.9.7 at `bffa36291796` (branch `perf/load-test`, the harness commit), production build                                                                                                                |
| machine      | Apple M5 Pro, 18 cores, 48 GiB, macOS (Darwin 25.5.0 arm64)                                                                                                                                            |
| runtimes     | Bun 1.4.2, Node v24.6.0, k6 v2.0.0                                                                                                                                                                     |
| server knobs | defaults: `PACKAGE_LOAD_WORKERS=1`, `PUBLISHER_MAX_CONCURRENT_QUERIES=32`                                                                                                                              |
| load average | 2.66 before the first run and 4.45 after the last (1 min). Not idle: other sessions were open on the machine, so treat small differences (under ~10 %) between these numbers and a later run as noise. |
| boot → READY | 3.6 s (medium), 2.2 s (small), 7.4 s (large)                                                                                                                                                           |
| packages     | storefront: 6 files, 0.9 MB. gen: 1M fact rows (18 MB parquet); Malloy text 12.8 KB (small, 4 files), 112 KB (medium, 10 files), 593 KB (large, 26 files)                                              |

### Loads

The generated package, by preset. `req/s` is completed operations per second across all VUs.

| preset |    op    | c1 req/s | c1 avg | c1 p95 | c2 req/s | c2 avg | c2 p95 | c4 req/s | c4 avg | c4 p95 |
| ------ | :------: | -------: | -----: | -----: | -------: | -----: | -----: | -------: | -----: | -----: |
| small  | register |     6.38 |    154 |    179 |     7.56 |    243 |    301 |     7.55 |    514 |    755 |
| small  |  reload  |     7.02 |    142 |    172 |     9.44 |    212 |    247 |     9.56 |    417 |    566 |
| medium | register |     1.05 |    950 |   1027 |     1.31 |   1377 |   1618 |     1.42 |   2687 |   3584 |
| medium |  reload  |     1.06 |    940 |   1025 |     1.33 |   1497 |   1674 |     1.44 |   2766 |   3636 |
| large  | register |     0.18 |   5560 |   5849 |     0.22 |   8547 |   9446 |     0.23 | 16.9 s | 23.3 s |
| large  |  reload  |     0.17 |   5810 |   6560 |     0.20 |   9714 | 10.9 s |     0.20 | 19.5 s | 28.1 s |

storefront (from the medium run): register 4.09 / 4.64 / 5.14 req/s at c1 / c2 / c4 (avg 241 / 408 /
747 ms), reload 4.62 / 5.34 / 5.77 req/s (avg 216 / 374 / 691 ms). The `delete` of each register
cycle averages under 4 ms at c1 but reaches a p95 of 167–484 ms at c4 and 1.2 s for large at c2: it
waits behind loads in progress.

Server-side, a medium register is 952 ms at the HTTP layer, but `malloy_package_load_duration`
records 611 ms of it and the worker's compile 297 ms; for large the same three figures are 5.6 s,
3.9 s and 1.6 s. The rest is spent outside the worker, partly after the load metric is
recorded.

### Queries

Closed loop (`constant-vus`), 60 s per row. Each cell is that op's `req/s · p95 ms`; `total` is
every op together.

| target     | VUs | total req/s |  query_cheap | query_medium |  query_heavy |  report_get |
| ---------- | --: | ----------: | -----------: | -----------: | -----------: | ----------: |
| storefront |   1 |         357 |  142.7 · 1.5 |  124.9 · 2.3 |  71.4 · 18.5 |   8.9 · 0.3 |
| storefront |   4 |         507 | 202.8 · 19.5 | 177.4 · 20.1 | 101.4 · 34.1 |  12.7 · 1.4 |
| storefront |  16 |         522 | 208.8 · 59.6 | 182.7 · 60.5 | 104.4 · 63.5 |  13.0 · 2.4 |
| storefront |  32 |         532 |  212.8 · 141 |  186.1 · 146 |  106.4 · 150 | 13.3 · 54.2 |
| gen        |   1 |          59 |   23.7 · 5.8 |  20.7 · 15.6 |   11.8 · 106 |   1.5 · 0.3 |
| gen        |   4 |          68 |   27.1 · 128 |   23.7 · 138 |   13.6 · 206 |   1.7 · 0.4 |
| gen        |  16 |          64 |   25.4 · 438 |   22.2 · 441 |   12.7 · 463 |   1.6 · 1.0 |
| gen        |  32 |          56 |  22.3 · 1087 |  19.4 · 1011 |   11.2 · 971 |   1.4 · 393 |

At 32 VUs the gen p99 reaches 1.7 s (cheap), 7.3 s (medium) and the max 9.5 s; `report_list`
behaves like `report_get` (storefront p95 49.5 ms, gen 433 ms at 32 VUs).

### Mixed

Queries at 10 req/s (open model), 60 s without loads, then 60 s with one VU reloading the queried
package and one VU registering, reloading and deleting a copy of the other package.

| target     | op           | avg base | avg load | avg × | p95 base | p95 load | p95 × |
| ---------- | ------------ | -------: | -------: | ----: | -------: | -------: | ----: |
| storefront | query_cheap  |      4.9 |     47.4 |  9.6× |      6.3 |      167 | 26.7× |
| storefront | query_medium |      7.2 |     50.3 |  7.0× |      9.0 |      163 | 18.1× |
| storefront | query_heavy  |     19.7 |     53.4 |  2.7× |     38.5 |      174 |  4.5× |
| storefront | report_list  |      1.2 |     27.1 | 23.5× |      1.4 |     97.9 | 68.0× |
| storefront | report_get   |      1.2 |     32.4 | 26.0× |      1.7 |      124 | 72.9× |
| gen        | query_cheap  |      9.3 |     48.7 |  5.3× |     13.9 |      169 | 12.2× |
| gen        | query_medium |     21.8 |     57.1 |  2.6× |     28.0 |      181 |  6.5× |
| gen        | query_heavy  |     59.9 |     98.1 |  1.6× |      114 |      229 |  2.0× |
| gen        | report_list  |      1.3 |     49.3 | 37.8× |      1.6 |      143 | 88.3× |
| gen        | report_get   |      0.9 |     29.5 | 33.0× |      1.1 |      107 | 98.9× |

The loaders themselves, during the second window: a reload of the queried package took 581 ms avg
(storefront) and 1227 ms (gen); registering a copy of the other package took 1167–1224 ms.

### What the baseline shows

- **Loads serialize.** Throughput is nearly flat from 1 to 4 concurrent loads (medium: 1.05 →
  1.42 req/s) while latency grows about linearly with concurrency (950 → 2687 ms), because loads
  queue for the single package-load worker (`PACKAGE_LOAD_WORKERS=1`) and for the main thread.
  Load time is roughly proportional to Malloy text, ~9–10 ms per KB once past fixed costs: small
  154 ms, medium 950 ms, large 5.6 s.
- **Query throughput plateaus early.** storefront reaches ~210 cheap req/s (~530 total) at 4 VUs
  and gains nothing beyond. The generated package's 1M-row queries do not scale past one VU at all
  (~24 cheap req/s, ~60–68 total): latency grows linearly with VUs instead, to a p99 of up to 9 s
  at 32 VUs. The profile traces both plateaus to the package's single DuckDB connection.
- **Loads disturb everything else.** While packages load, cheap-query p95 rises 12–27× and report
  reads, which are sub-millisecond work, rise 68–99× (p95 1–2 ms → 98–143 ms). Work that does no
  I/O slowing down that much points at the main thread being blocked during loads, not at
  contention for DuckDB.

[performance-profile.md](performance-profile.md) attributes each of these to code, with the tooling
to reproduce it, and ranks what to change.

## After quick wins 1–5

Recorded on 2026-10-12 with the same commands as the [baseline](#baseline), on the same machine, after
quick wins #1–#5 from the [profile](performance-profile.md#quick-wins) (branch `perf/quick-wins`,
`37fd67934ab3`, production build):

1. the persist build plan compiles only models with `##! experimental.persistence` or a usable
   `#@ preaggregate`, as the worker's own compile reports;
2. each hydrated model builds its Malloy `QueryModel` once, and every render-tag prepare and query
   reuses it;
3. render-tag validation, the build plan and model hydration yield to the event loop every 10 ms;
4. a model's report facts (`carriesReportTag`) are derived once;
5. a new histogram, `publisher_package_load_phase_duration_ms{phase}`, covers the whole load.

**Machine load was higher than for the baseline.** 1-minute load average: `perf:baseline` 3.28 at
the start → 7.23 at the end (baseline: 2.66 → 4.45); small loads 5.1 → 4.75; large loads 4.37 →
5.68, with other sessions pushing it to ~14 during the large reload runs. Other sessions were open
throughout. Every run finished with 0 % errors and no dropped iterations. Treat differences under
~10 %, and the large reload rows, as noise; the load and mixed changes below are far larger.

### Loads

`req/s` · avg · p95 (ms, s where noted), before → after.

| preset | op       | c1                                      | c2                                        | c4                                              |
| ------ | -------- | --------------------------------------- | ----------------------------------------- | ----------------------------------------------- |
| small  | register | 6.38 · 154 · 179 → 7.34 · 131 · 244     | 7.56 · 243 · 301 → 7.65 · 253 · 532       | 7.55 · 514 · 755 → 9.78 · 398 · 495             |
| small  | reload   | 7.02 · 142 · 172 → 10.1 · 99 · 131      | 9.44 · 212 · 247 → 15.6 · 128 · 150       | 9.56 · 417 · 566 → 15.2 · 263 · 326             |
| medium | register | 1.05 · 950 · 1027 → 1.77 · 563 · 611    | 1.31 · 1377 · 1618 → 2.18 · 911 · 956     | 1.42 · 2687 · 3584 → 2.19 · 1809 · 2739         |
| medium | reload   | 1.06 · 940 · 1025 → 1.83 · 547 · 583    | 1.33 · 1497 · 1674 → 2.07 · 961 · 1106    | 1.44 · 2766 · 3636 → 2.25 · 1759 · 2657         |
| large  | register | 0.18 · 5560 · 5849 → 0.35 · 2821 · 3124 | 0.22 · 8547 · 9446 → 0.37 · 5408 · 6534   | 0.23 · 16.9 s · 23.3 s → 0.34 · 11.5 s · 19.2 s |
| large  | reload   | 0.17 · 5810 · 6560 → 0.29 · 3402 · 3973 | 0.20 · 9714 · 10.9 s → 0.39 · 5115 · 6198 | 0.20 · 19.5 s · 28.1 s → 0.40 · 9782 · 15.7 s   |

storefront (medium run): register 4.09 / 4.64 / 5.14 → 6.74 / 8.07 / 8.15 req/s at c1 / c2 / c4 (avg
241 / 408 / 747 → 145 / 242 / 466 ms); reload 4.62 / 5.34 / 5.77 → 7.41 / 10.57 / 10.50 req/s (avg
216 / 374 / 691 → 135 / 189 / 380 ms).

A single medium load is 41 % faster (950 → 563 ms) and a single large one 49 % (5.56 → 2.82 s), and
throughput rose 48–95 % at every concurrency for the medium and large presets. The small preset's
register p95 got worse at c1 and c2 while its average improved; that run started at a load average
of 5.1 and the p95 is a handful of slow outliers, so it is read as noise, not a regression.

### Where a load's time goes now

`publisher_package_load_phase_duration_ms` averages per register at c1, against the unchanged
`malloy_package_load_duration` and the server-side HTTP time:

| phase (ms)                     | storefront | gen medium | gen large |
| ------------------------------ | ---------: | ---------: | --------: |
| `worker`                       |       60.4 |        449 |      2384 |
| `hydrate`                      |        1.7 |        2.1 |       6.1 |
| `render_validation`            |       44.9 |       93.2 |       385 |
| `discovery`                    |        8.1 |        3.5 |       9.1 |
| `lint`                         |        9.4 |        1.9 |       2.2 |
| `build_plan`                   |        0.0 |        0.0 |       0.0 |
| `total`                        |        126 |        554 |      2806 |
| `malloy_package_load_duration` |        107 |        544 |      2775 |
| HTTP (server-side)             |        145 |        562 |      2820 |

The build plan, 330 ms of a medium load and 1.6 s of a large one, is gone for packages that do not
persist. Render-tag validation fell from 167 to 93 ms (medium) and from 1.5 s to 385 ms (large),
from the shared `QueryModel`. The worker is now ~80 % of every gen load. `total` is within 2–20 ms of
the HTTP time. The old metric is close to it now only because the build plan it never saw is gone;
it still misses discovery and lint.

### Event loop during loads

The profile's probe (`ctl.ts loads … register`, sequential, no CPU profiler; see
[method](performance-profile.md#method)):

| window              | p99           | p99.9         | max                | stalls > 100 ms | loop blocked | avg load       |
| ------------------- | ------------- | ------------- | ------------------ | --------------: | -----------: | -------------- |
| 10 medium registers | 14.4 → 8.3 ms | 118 → 20.8 ms | **135 → 42.6 ms**  |          11 → 0 |    49 → 18 % | 953 → 560 ms   |
| 4 large registers   | 23.7 → 9.1 ms | 49 → 22.4 ms  | **1142 → 48.5 ms** |           8 → 0 |    57 → 17 % | 5365 → 2774 ms |

The longest remaining stalls are the worker result's hand-off (structured clone, ~47 ms per large
load) and single render-tag prepares on the large `index.malloy`.

### Queries

Closed loop, 60 s per row: total req/s and each op's p95 (ms), before → after. The baseline section
recorded p95 per op only, so avg and p99 are after-only (below, and in the run's `report.md`).

| target     | VUs | total req/s | query_cheap p95 | query_medium p95 | query_heavy p95 | report_get p95 |
| ---------- | --: | ----------: | --------------: | ---------------: | --------------: | -------------: |
| storefront |   1 |   357 → 337 |       1.5 → 1.6 |        2.3 → 2.6 |     18.5 → 19.5 |      0.3 → 0.4 |
| storefront |   4 |   507 → 499 |     19.5 → 19.8 |      20.1 → 20.6 |     34.1 → 33.8 |      1.4 → 1.6 |
| storefront |  16 |   522 → 504 |     59.6 → 62.3 |      60.5 → 63.3 |     63.5 → 73.2 |      2.4 → 2.4 |
| storefront |  32 |   532 → 504 |       141 → 147 |        146 → 152 |       150 → 152 |    54.2 → 50.7 |
| gen        |   1 |     59 → 58 |       5.8 → 5.5 |      15.6 → 17.0 |       106 → 113 |      0.3 → 0.3 |
| gen        |   4 |     68 → 60 |       128 → 146 |        138 → 158 |       206 → 167 |      0.4 → 0.6 |
| gen        |  16 |     64 → 55 |       438 → 463 |        441 → 498 |       463 → 557 |      1.0 → 0.6 |
| gen        |  32 |     56 → 53 |     1087 → 1233 |      1011 → 1096 |      971 → 1091 |      1.4 → 650 |

After, at 32 VUs: storefront avg 63 / 67 / 70 ms and p99 198 / 206 / 200 ms (cheap / medium /
heavy); gen avg 642 / 598 / 608 ms and p99 1.6 / 1.5 / 1.4 s (baseline gen p99: 1.7 / 7.3 s for
cheap / medium).

As the profile predicted, query throughput did not move: both plateaus are each package's single
DuckDB connection (#6). The 3–12 % lower totals and slightly higher p95s line up with the heavier
machine load (load average 5–8 during these rows, against 3–4 for the baseline). The per-query
main-thread savings of #2 and #4 are a fraction of a millisecond per request and do not show at the
HTTP layer while DuckDB is the bottleneck. The gen `report_get` p95 at 32 VUs (1.4 → 650 ms, 79
requests) is a few samples queued behind 32 one-second queries; `report_list` at the same point went
433 → 352 ms.

### Mixed

Queries at 10 req/s, then the same with loads: p95 under load, and the under-load / baseline ratios,
before → after.

| target     | op           | p95 under load (ms) |       p95 × |      avg × |
| ---------- | ------------ | ------------------: | ----------: | ---------: |
| storefront | query_cheap  |          167 → 28.2 |  26.7 → 4.1 | 9.6 → 1.95 |
| storefront | query_medium |          163 → 27.3 |  18.1 → 2.6 | 7.0 → 1.31 |
| storefront | query_heavy  |          174 → 41.7 |  4.5 → 0.85 | 2.7 → 0.85 |
| storefront | report_list  |         97.9 → 11.1 |  68.0 → 4.4 | 23.5 → 2.0 |
| storefront | report_get   |          124 → 15.2 |  72.9 → 9.9 | 26.0 → 3.4 |
| gen        | query_cheap  |          169 → 32.3 | 12.2 → 0.91 | 5.3 → 0.71 |
| gen        | query_medium |          181 → 38.1 |  6.5 → 0.84 | 2.6 → 0.70 |
| gen        | query_heavy  |           229 → 127 |  2.0 → 0.71 | 1.6 → 0.78 |
| gen        | report_list  |          143 → 19.3 |  88.3 → 2.5 | 37.8 → 2.2 |
| gen        | report_get   |          107 → 17.8 |  98.9 → 8.4 | 33.0 → 6.4 |

The loaders themselves: a reload of the queried package 581 → 358 ms (storefront) and 1227 → 671 ms
(gen); registering a copy of the other package 1167–1224 → 667–704 ms. gen's ratios below 1.0 come
from a noisy baseline window (its cheap-query p99 there was 94 ms), not from loads speeding queries
up.

### Against the profile's prediction

| prediction                                            | result                                                                                                                                                                                                                                                                                    |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| medium load ~950 → ~570 ms                            | **Met:** 950 → 563 ms register, 940 → 547 ms reload.                                                                                                                                                                                                                                      |
| large load ~5.6 → ~3 s                                | **Met for register** (5.56 → 2.82 s); reload 5.81 → 3.40 s, measured while the load average was ~14.                                                                                                                                                                                      |
| mixed under-load ratios down by an order of magnitude | **Mostly.** Query p95 ratios fell 3–13× (to 0.7–4.1×) and report-list ratios 15–35×, but report reads still reach 8–10× (p95 15–18 ms against a 1.5–2 ms baseline). What is left on the main thread is render-tag validation's prepares and the worker hand-off; #8 would move the first. |
| longest event-loop stall: tens of ms                  | **Met:** 135 → 43 ms (medium), 1142 → 49 ms (large); no stall over 100 ms in either window.                                                                                                                                                                                               |
| query throughput unchanged                            | **As predicted:** unchanged within noise (3–12 % lower under a heavier machine load); DuckDB-bound until #6.                                                                                                                                                                              |

Memory cost of #2, measured standalone (every model file compiled and hydrated as Publisher does,
heap after a full GC, then one `QueryModel` per model and one query per fact source): +4.8 MB of heap
for the medium package (12 files, 33.6 → 38.4 MB) and +13.9 MB for the large one (28 files, 67.2 →
81.2 MB), retained while the package is loaded. Before, the same structures were built per prepare
or query and dropped.

Result directories (git-ignored): `results/perf-2026-10-12T04-37-01` (`perf:baseline`),
`perf-2026-10-12T05-03-12` (small loads) and `perf-2026-10-12T05-09-52` (large loads).
