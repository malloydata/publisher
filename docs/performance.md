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

_Not yet recorded._ Run the [baseline commands](#run-it) on an idle machine and paste each
`report.md`'s scenario tables here, with the context table (version, SHA, machine) they came from.
