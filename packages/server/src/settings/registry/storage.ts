// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import fs from "fs";
import os from "os";
import { setting } from "../setting";
import { boolean, integer, oneOf, string } from "../types";

/**
 * The whole CPUs a cgroup CPU quota allows, from a quota and period in
 * microseconds; undefined for no quota (v2 `max`, v1 `-1`) or text that is not
 * one. A fractional quota rounds up: half a CPU still runs one thread.
 */
export function cgroupCpuLimit(
   quota: string | undefined,
   period: string | undefined,
): number | undefined {
   const limit = Math.ceil(Number(quota) / Number(period));
   return Number.isFinite(limit) && limit >= 1 ? limit : undefined;
}

/** The CPUs the cgroup CPU quota allows (v2, else v1); undefined for none. */
function cgroupQuota(): number | undefined {
   const reads: (() => [string | undefined, string | undefined])[] = [
      // cgroup v2: "<quota> <period>", or "max <period>" for none.
      () => {
         const [quota, period] = fs
            .readFileSync("/sys/fs/cgroup/cpu.max", "utf8")
            .trim()
            .split(/\s+/);
         return [quota, period];
      },
      // cgroup v1: the quota is -1 for none.
      () => [
         fs.readFileSync("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "utf8").trim(),
         fs.readFileSync("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "utf8").trim(),
      ],
   ];
   for (const read of reads) {
      try {
         return cgroupCpuLimit(...read());
      } catch {
         // Not this cgroup version, or not Linux.
      }
   }
   return undefined;
}

/**
 * The CPUs this process may run on: the host's (its CPU affinity), bounded by
 * a cgroup CPU quota when the container sets one, as DuckDB bounds its own
 * default. `os.availableParallelism()` reports the node's cores inside a
 * CPU-limited container.
 */
export function availableCpus(): number {
   const cpus = os.availableParallelism();
   const quota = cgroupQuota();
   return quota === undefined ? cpus : Math.min(cpus, quota);
}

/** The configuration design's `duckdb`, `ducklake`, `persist`, `materialization` table. */
export const storage = [
   setting({
      key: "duckdb.extensionFetchPolicy",
      type: oneOf(["on-demand", "local-only"]),
      default: "on-demand",
      description:
         "on-demand installs a missing DuckDB extension on first use; local-only never reaches the network.",
      notes: "Whether the server may fetch DuckDB extensions from the network at runtime. `on-demand`: a missing extension is installed on first use (unchanged prior behavior). `local-only`: never install, and disable DuckDB's implicit auto-install \u2014 a locally-present (e.g. image-baked) extension still loads, but a missing one fails with an actionable error naming the extension. Use `local-only` for air-gapped / pinned-image deployments. See [ducklake.md](ducklake.md#duckdb-extension-provisioning).",
      aliases: { env: ["EXTENSION_FETCH_POLICY"] },
   }),
   // The sizes below are strings because DuckDB parses them (`1GB`);
   // assertDuckDBResourceConfig checks them at boot.
   setting({
      key: "duckdb.memoryLimit",
      type: string(),
      description:
         "memory_limit of every DuckDB session, as a DuckDB size (1GB), or off; unset warns at boot.",
      notes: "DuckDB `memory_limit` applied to **every** DuckDB session and instance Publisher owns, as a flat value (`1GB`, `512MB`) passed to DuckDB verbatim. `off` or unset leaves DuckDB's own default, which is roughly 80% of the container's memory computed **per instance** \u2014 so N instances in one process commit N times that share, and the kernel kills the process while each of them still believes it is inside its budget. Measured in a 3 GiB container: three instances each reported a 2.3 GiB limit, 6.9 GiB of committed budget against 3 GiB of real memory. **Sizing:** the divisor is not a number of builds. Count 1 metadata store + 1 serve-shape gate session + 1 environment lookup funnel + one sandbox per loaded package \u00d7 `PACKAGE_LOAD_WORKERS` (package loads run in `worker_threads`, so same address space and same cgroup) + one per in-flight materialization build, and nothing bounds concurrent builds. Budget roughly (container memory \u2212 resident baseline) \u00f7 that count. DuckDB does not reserve the limit up front, so an idle instance costs far less than its budget \u2014 the limit caps what it may reach, and it is the sum of the caps that has to fit. Too low fails a query with a DuckDB out-of-memory error and leaves the process up, which is the intended trade against losing the pod and every package on it. Validated at startup.",
      aliases: { env: ["PUBLISHER_DUCKDB_MEMORY_LIMIT"] },
   }),
   setting({
      key: "duckdb.threads",
      // A thread count or `off`.
      type: string(),
      // Never more than DuckDB's own default would start: on a host or
      // container with fewer than four CPUs, a flat 4 would raise each pool.
      default: () => String(Math.min(4, availableCpus())),
      defaultText: "`4`, or the CPUs available when fewer",
      description:
         "Threads each DuckDB instance starts with, unless its connection sets threads, or off.",
      notes: "The size of the thread pool each DuckDB instance Publisher opens starts with, unless the connection sets its own `threads`. DuckDB's own default is one thread per core **per instance**, and Publisher opens many instances in one process: one sandbox per loaded package (and per version), one per file-backed DuckDB, DuckLake or Azure connection, one per in-flight materialization build, plus the metadata store. Measured on an 18-core host: about 15 threads per loaded package. The pools are fixed when each instance opens, so they add up, and the process fails when they reach the thread limit (`kern.num_taskthreads`, 12,288 per process on macOS; on Linux the cgroup `pids.max`, which Kubernetes and `docker run --pids-limit` often set to a few thousand): DuckDB reports `thread constructor failed: Resource temporarily unavailable`, and under Bun the process aborts with `panic: A C++ exception occurred`, taking every package with it. **Sizing:** count the instances as for `PUBLISHER_DUCKDB_MEMORY_LIMIT` and keep (instances × this) well under the limit. A higher value buys parallelism within one large local query (a scan of package-local Parquet or CSV); a dashboard's small queries rarely use more than a few threads. The default is `4`, or the CPUs the process may use when fewer (the host's, bounded by a cgroup v1 or v2 CPU quota), so it never starts a larger pool than DuckDB itself would. `off` restores DuckDB's one-per-core default, the behaviour before this setting existed. Applies at instance open, so a change takes effect for instances opened after a restart. A materialization build that lowers threads for a partitioned write (`PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD`) still does so. Validated at startup.",
      aliases: { env: ["PUBLISHER_DUCKDB_THREADS"] },
   }),
   setting({
      key: "duckdb.tempDirectory",
      type: string(),
      description: "Directory DuckDB spills to.",
      notes: "Where DuckDB spills. A materialization build overrides this with its own disposable working directory (unique per build, removed with the build); every other session and instance uses this. Unset leaves DuckDB's default of `.tmp` relative to the process working directory, whose default `max_temp_directory_size` is 90% of whatever filesystem that lands on \u2014 on a container with no ephemeral-storage limit, the node's shared disk. Created at startup if missing, since `SET temp_directory` accepts a path that does not exist and only fails at the first spill. Unlike the memory limit there is no `off` sentinel, because a directory named `off` is a legal path. Note that setting `PUBLISHER_DUCKDB_MEMORY_LIMIT` creates spill on the `storage=` build path in one case: a `partition=` build sorts the whole passthrough result by its partition columns in DuckDB before writing it (see `PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD`), and that sort runs out of core against the build's own working directory, so the entire result can land on local disk. Every other `storage=` build pushes its SQL to the source warehouse and streams the result into the destination with nothing to spill. Local compute, and so spill, is otherwise the chained-build and serve paths.",
      aliases: { env: ["PUBLISHER_DUCKDB_TEMP_DIRECTORY"] },
   }),
   setting({
      key: "duckdb.partitionedWriteFlushThreshold",
      // A row count or `off`, which turns the partitioned-insert bounds off.
      type: string(),
      default: "8192",
      description:
         "Rows a partitioned storage build buffers before flushing, or off.",
      notes: "Rows the appender buffers before a `#@ persist partition=` build flushes them to the partition files, set on the build session as DuckDB's `partitioned_write_flush_threshold`. `off` turns the whole treatment off \u2014 this threshold, the ordered insert and the single appender thread together \u2014 and leaves DuckDB's own behaviour (a 524,288-row threshold, an unordered parallel insert); it is the escape hatch for a source that built fine before, at the cost of the sort. A partitioned build is DuckDB's partitioned COPY, and that operator buffers rows per partition in a column-data collection **inside** the buffer manager \u2014 so unlike the two DuckLake bounds above, `PUBLISHER_DUCKDB_MEMORY_LIMIT` does see this term, and the failure reads `Out of Memory Error: failed to pin block` against the session's limit rather than a container kill. Two things drive it: every partition the appender meets costs it one vector per column up front, before a row of it is flushed, and nothing is flushed until it has appended this many rows across all its partitions. On a passthrough-sourced build (Postgres, BigQuery, Snowflake; a chained build is untouched) the publisher bounds the first by ordering the insert's SELECT by the partition columns at the top of the INSERT statement \u2014 the SELECT the build was handed is unchanged, so what the warehouse runs, its query tag or label, and how the source is addressed are untouched \u2014 and by running that insert on one thread, since a DuckDB-side sort is read in parallel and several appenders each meet every partition again; this variable bounds the second. Sorted by DuckDB rather than by the warehouse because a multi-stream read (BigQuery) re-interleaves a sorted result on the way in. Measured at a `768MB` limit on 122 columns, 616k rows, 308 partitions: from Postgres, interleaved rows fail in under 3 s at any threshold, ordered rows fail at DuckDB's default after ~0.5M rows, ordered rows on one thread complete at `8192` in 9.5 s (4 threads fail at 7 s); from BigQuery, interleaved fails at 54 s and ordered on one thread completes in 36 s (4 threads fail at 39 s). Snowflake is not measured; the write side is the same. The treatment has a running cost: the sort reads the whole result before the first partition file is written, spills it to the build's working directory when it exceeds the memory limit, and writes on one thread \u2014 so a wide, many-partition build trades memory for local disk and wall-clock, and no production-scale build has been measured yet. The disk is the one to watch: the working directory is created under the process temp directory, which in a container with no volume there is the node's overlay filesystem, and a spill of tens of GB there is an ephemeral-storage eviction \u2014 a killed pod, not a publisher error. Give the worker a volume for its temp directory, or an ephemeral-storage limit that makes the failure a caught one, before relying on this at that scale. The single thread costs little on a single-stream read (`postgres_query`, `snowflake_query`, `bigquery_query`) and real scan parallelism on the labelled BigQuery split, whose result table is read multi-stream. An incremental delta into a laid-out table runs on the same session and gets the threshold and the single thread; its read is not ordered. Smaller is not free: a flush touches every partition the appender holds, so a very high-cardinality layout (thousands of partitions) does more file work at a low value \u2014 that is the many-small-files layout `docs/materialization.md` already warns off. Validated at startup.",
      aliases: { env: ["PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD"] },
   }),
   setting({
      key: "ducklake.rowGroupSizeBytes",
      type: string(),
      description:
         "Bytes DuckLake buffers per column before flushing a Parquet row group (16MB).",
      notes: "Bytes DuckLake buffers per Parquet row group before flushing, applied as a **catalog** option on each writable lake at attach. Unset issues nothing and leaves DuckLake's default of 122,880 ROWS. A DuckLake write buffers a whole row group per COLUMN, so its memory is driven by the table's width rather than its row count, and `PUBLISHER_DUCKDB_MEMORY_LIMIT` does not bound it \u2014 those buffers sit outside the buffer manager, so a low limit and a large row group OOM together. Measured on a 72-column, 5M-row write, sampling cgroup `memory.stat` anon: 2772 MiB at the default, 1530 MiB at `64MB`, 1006 MiB at `16MB`. Bytes rather than a row count because one row count cannot suit a 9-column and a 110-column table at once; the byte form derives rows-per-group from the data actually buffered, so it tracks width with nobody estimating it. Smaller is not free: more row groups cost Parquet metadata and read-side pruning quality, and the read cost has not been measured here. Requires `preserve_insertion_order=false`, which Publisher sets on the attaching session \u2014 DuckLake plans its copy parallel unconditionally and does not honour insertion order regardless (measured: 15 adjacent inversions per 1.5M rows with it on, 10 with it off), so the guarantee being waived is not one that was being provided. Written to `ducklake_metadata`, so it outlives the connection and is seen by every writer of that lake; skipped on a read-only attach. A catalog that refuses the option is logged and attaches anyway. Validated at startup.",
      aliases: { env: ["PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES"] },
   }),
   setting({
      key: "ducklake.targetFileSizeBytes",
      type: string(),
      description:
         "Size a DuckLake Parquet file grows to before rotating (256MB).",
      notes: "Bytes DuckLake writes into one Parquet file before rotating to the next, applied as a **catalog** option on each writable lake at attach. Unset issues nothing and leaves DuckLake's own default of roughly 512MB. A separate term from `PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES`, not a replacement: that bounds the per-column buffer _within_ a file, this bounds how much of the file stays resident. Writing to object storage, DuckDB copies each multipart part into a buffer it allocates itself and holds it until the file completes, so a file's bytes are resident however they are grouped inside it \u2014 and `PUBLISHER_DUCKDB_MEMORY_LIMIT` bounds neither, since both sit outside the buffer manager. The same write to a local path streams and shows none of this, so the cost is invisible to testing that does not target a cloud destination. Measured on a 72-column, 20M-row DuckLake write to GCS, all cells in one batch: 650 MiB at DuckLake's default, 550 MiB at `512MB`, 373 MiB at `256MB`, 262 MiB at `128MB`, 255 MiB at `64MB`. **Pick the largest value that clears your memory ceiling, not the smallest** \u2014 memory is the only axis where smaller wins and it stops improving below ~`128MB`, while write throughput, full-scan time and catalog size all get worse (a full scan measured 13.0s at `512MB` against 21.6s at `64MB`; the catalog holds one row per file per column, in a database every writer of that lake shares). Where sources carry a required equality filter, scan time is the least binding of the three: a selective lookup ran in 17\u201363 ms at every size tested. Unlike the row-group bound it does **not** require `preserve_insertion_order=false`, and the order the two are applied in does not matter. Written to `ducklake_metadata`, so it outlives the connection and is seen by every writer of that lake; skipped on a read-only attach. **Unsetting this variable does not revert it** \u2014 the last value written stays in the catalog; to change it back, run `CALL <lake>.set_option('target_file_size', '<value>')` against the catalog. A catalog that refuses the option is logged and attaches anyway. Validated at startup.",
      aliases: { env: ["PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES"] },
   }),
   setting({
      key: "persist.storageMode",
      type: oneOf(["off", "write-only", "on"]),
      default: "off",
      description:
         "How #@ persist storage= is honored: off, write-only (build only) or on (build and serve).",
      notes: "Controls the `#@ persist storage=<name>` materialization tier (materialize a source into a registered [storage destination](connections.md#storage-destinations) and serve it from there \u2014 a destination is declared alongside `connections`, not in it, and is not nameable from a model). `off`: the `storage=` annotation is inert \u2014 sources build and serve from their own warehouse exactly as without it. `write-only`: materialize into the storage destination but still serve live (the measurement rung). `on`: build **and** serve from the storage table via the virtual-source transform. Read at startup. A kill switch: moving it **down** never fails a loaded package \u2014 a `storage=` source just reverts to serving live and surfaces as a package warning. See [persist-storage-tutorial.md](persist-storage-tutorial.md).",
      aliases: { env: ["PERSIST_STORAGE_MODE"] },
   }),
   setting({
      key: "persist.addressIncludesDestination",
      type: boolean(),
      default: false,
      description:
         "Fold a storage= source's destination into its content address (re-addresses every storage= table once).",
      notes: "Materialization v2's P12: a `storage=` source's content address (its `sourceEntityId`) also covers the storage destination it builds into, so moving a source between colocated and `storage=`, or between destinations, gives it a new table instead of reusing or resolving the old one (#7087). The destination is folded in only when the source declares `storage=` and `PERSIST_STORAGE_MODE` is not `off`, so colocated addresses never move. **Turning it on re-addresses every `storage=` table once**: reuse misses, an incremental source re-seeds, each table rebuilds under its new address on its next run, and a `storage=` source serves live until it has. A table Publisher names itself (`name=` or the source's name) is rebuilt in place; a table a host instructed under a generational name is the host's to reclaim. Read once at startup: set it the same on every worker serving an environment and restart them together, or workers compute different addresses for the same source. Off by default; turn it on per deployment as a scheduled rollout, after checking the rebuild cost, and make the first run after it a full one so chained sources find their parents under the new address.",
      aliases: { env: ["PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION"] },
   }),
   setting({
      key: "materialize.enabled",
      type: boolean(),
      default: false,
      description:
         "Read #(materialize) and build its generated #@ persist sources (materialization v2, in development).",
      notes: "Materialization v2's `#(materialize)` front end, still being built (phase 3). On, a source annotated `#(materialize)` gets a generated build source, a query-shaped `#@ persist` over the source in a companion model the package never shows, and the build plan builds it under the author's source name: the plan, a name-scoped run (`sourceNames` takes the author's name), the manifest, warnings and the path report never name the generated source. With `PERSIST_STORAGE_MODE=on`, a built `storage=` source is also served from its **served copy**: the author's own files, held in memory, with the source re-rooted on its stored table and each computed dimension read as the stored column of its own name (its `#(doc)` and other annotations kept on the field), so extensions, views, joins and importing files are the author's text unchanged; a query the served copy cannot answer answers live. The served copy also re-roots a query-shaped `#@ persist storage=` source. A served copy that does not compile is a `served-copy-not-served` package warning. A reader that opts out with `#@ -persist` (Malloy core's opt-out, still supported) recomputes live, and the served copy declines it. On, a measure that declares v1's `#@ preaggregate` gets a `deprecated-preaggregate-annotation` warning naming its v2 spelling, `#(preaggregate)` ([materialization-migration.md](materialization-migration.md)); it still works as before. Off, `#(materialize)` is ignored and the build plan is exactly what it was. Read at startup. Leave it off outside development.",
      aliases: { env: ["PUBLISHER_MATERIALIZE"] },
   }),
   setting({
      key: "materialize.gatedSources",
      type: boolean(),
      default: false,
      description:
         "Let the served copy, persisted views and colocated rollups answer a query through a row-level gate, with the gate grafted on them (materialization v2's R3, in development).",
      notes: "Materialization v2's R3, contracts at read, off by default until the security review that question 14 of the plan asks for. Today a query whose entry point carries a row-level gate (`#(authorize)` with a row filter, `#(access_filter)`) is never answered from a `storage=` table. On, the served copy may answer it: the served copy is the author's own text, gates included, so the row filter is grafted and proven on the served compile exactly as on live. A persisted view and a colocated `#(preaggregate)` rollup are let through the same way, each redeclaring the queried source with its gates; a rollup only when every gate reads its grain dimensions. A v1 `#@ preaggregate` rollup is the rollup leg's and is let through as a `#(preaggregate)` one is. A `storage=` rollup over a gated source is neither built nor read until the review. A gate that cannot be grafted on the served copy denies, as it does live. It also decides what is stored: on, a `#(materialize) storage=` source whose gate is its own row filter or lock is built and bound, as is a colocated `#(preaggregate)` rollup over a gated source; off, the first is refused as every gated `storage=` source is, and the second is not planned. Storing a gated source puts its columns in the managed tier, which is why this waits for the review. Read at startup.",
      aliases: { env: ["PUBLISHER_MATERIALIZE_GATED_SOURCES"] },
   }),
   setting({
      key: "persist.collisionEnforcementEnabled",
      type: boolean(),
      default: true,
      description:
         "Refuse a publish where two persist sources resolve to the same table.",
      notes: "On by default: a publish in which two different persist sources of one package resolve to the same table in the same destination is refused, as is a build of such a package, before anything is written. Give each source a distinct `name=` to fix it. Set to `false` to opt out, which turns the refusal into a warning; loading a package only ever warns. Either way neither source is served from the shared table, which holds whichever build ran last: queries on both are answered live until each has its own `name=`. A `#(materialize)` source on another source's table is never built, whatever this says (`materialize-not-built`).",
      aliases: { env: ["PERSIST_COLLISION_ENFORCE"] },
   }),
   setting({
      key: "materialization.schedulerEnabled",
      type: boolean(),
      default: false,
      description:
         "Fire packages' materialization schedules from this process; never on an orchestrated worker.",
      notes: "Opt-in: enable the standalone materialization scheduler, which fires each loaded package's `materialization.schedule` cron, and each published package version's (see [package-versions.md](package-versions.md#materializations)), so a self-hosted Publisher rebuilds on a cadence with no control plane. **Never set this on a control-plane-driven (orchestrated) worker** \u2014 it is the primary guard against double-driving refresh. See [materialization.md](materialization.md).",
      aliases: { env: ["PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER"] },
   }),
   setting({
      key: "materialization.schedulerIntervalMs",
      type: integer({ min: 1000 }),
      default: 60_000,
      description: "How often the scheduler looks for due schedules, in ms.",
      notes: "How often the scheduler sweeps for due schedules, in ms. Minimum `1000`. Only read when the scheduler is enabled.",
      defaultText: "`60000` (1 min)",
      aliases: { env: ["PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS"] },
   }),
   setting({
      key: "materialization.schedulerMaxFiresPerTick",
      type: integer({ min: 1 }),
      default: 10,
      description: "Packages the scheduler fires per look.",
      notes: "Stampede guard: max packages fired per sweep. A capped package fires on a later tick. Must be a positive integer.",
      aliases: {
         env: ["PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK"],
      },
   }),
];
