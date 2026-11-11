// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The router latency baseline: what today's routing decision
 * (`Model.runQueryResults`) costs per query, on each path it takes.
 * Materialization v2 replaces that chain with a variant router, and its
 * numbers gate phase 1 against these.
 *
 * In process, on the R4 harness's real build-and-bind path, so the time is the
 * routing and the query, with no HTTP and no destination server. The stored
 * table holds the same rows as its source, so served minus live is roughly
 * the routing overhead, not a smaller table.
 *
 *   bun run perf:router [--rows 200000] [--iterations 30] [--sources 1]
 *
 * (`--name=value` works too.) `--sources N` declares N persisted sources over
 * the fact table, all bound. The serve shape is compiled package-wide, over
 * every binding, so its cost grows with N; queries read the first.
 *
 * Three fixtures over the same model, one per path, so measuring one path
 * never disturbs another's state: live (no bindings), warm (bindings set once,
 * the serve shape memoized), and cold (bindings re-set before every query, so
 * each compiles the shape). Re-setting is outside the stopwatch; only the
 * query is timed. Each iteration times the three paths in a rotating order,
 * so none is systematically measured first, and the logger is raised to
 * `error` while timing so no log line falls inside the stopwatch.
 */

import { logger } from "../../src/logger";
import { installSettings } from "../../src/test_helpers/settings_env";
import { buildScenario, type R4Fixture } from "../component/r4/harness";

/** Stop with a usage error, before any logger or fixture exists. */
function usage(message: string): never {
   console.error(`router_bench: ${message}`);
   process.exit(2);
}

/** A positive integer flag, as `--name value` or `--name=value`. */
function arg(name: string, fallback: number): number {
   const flag = `--${name}`;
   const argv = process.argv;
   let raw: string | undefined;
   for (let i = 0; i < argv.length; i++) {
      const a = argv[i];
      if (a === flag) {
         raw = argv[i + 1];
         if (raw === undefined || raw.startsWith("--")) {
            usage(`${flag} needs a value`);
         }
      } else if (a?.startsWith(`${flag}=`)) {
         raw = a.slice(flag.length + 1);
      }
   }
   if (raw === undefined) return fallback;
   const n = Number(raw);
   if (!Number.isInteger(n) || n < 1) {
      usage(`${flag} must be a whole number of at least 1, not "${raw}"`);
   }
   return n;
}

const ROWS = arg("rows", 200_000);
const ITERATIONS = arg("iterations", 30);
const SOURCES = arg("sources", 1);
const WARMUP = 5;

const persisted = (name: string, where: string) => `
#@ persist name="${name}_rows" storage="lake"
source: ${name} is duckdb.table('fact') -> { select: *${where} } extend {
  primary_key: id
  join_one: regions with region_code
  dimension: big is amount > 500
  measure: n is count(), total is amount.sum()
}
`;
// Each extra source filters differently, so each has its own table.
const model =
   "##! experimental.persistence\nsource: regions is duckdb.table('regions') extend { primary_key: code }\n" +
   persisted("facts", "") +
   Array.from({ length: SOURCES - 1 }, (_, i) =>
      persisted(`facts_${i + 2}`, `; where: id % ${i + 2} = 0`),
   ).join("");

const QUERIES = {
   cheap: "run: facts -> { aggregate: n, total }",
   grouped: "run: facts -> { group_by: region_code, big; aggregate: n, total }",
   // Through a join to a source that is not stored: the serve shape cannot
   // carry it, so the storage variant fails to compile and the query runs
   // live, reporting no tier (a clean compile miss, not a runtime fallback).
   miss: "run: facts -> { group_by: regions.region_name; aggregate: n }",
};

type Path = "live" | "warm" | "cold";
const PATHS: Path[] = ["live", "warm", "cold"];

function stats(ms: number[]) {
   const sorted = [...ms].sort((a, b) => a - b);
   const at = (q: number): number =>
      sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ??
      Number.NaN;
   return { median: at(0.5), p95: at(0.95) };
}

function scenario(): Promise<R4Fixture> {
   return buildScenario({
      files: { "m.malloy": model },
      seedSql: [
         `CREATE TABLE regions AS SELECT * FROM (VALUES
            ('e', 'East'), ('w', 'West'), ('n', 'North'), ('s', 'South')
          ) AS t(code, region_name)`,
         `CREATE TABLE fact AS
            SELECT i AS id,
                   ['e', 'w', 'n', 's'][1 + (i % 4)] AS region_code,
                   (hash(i) % 1000)::DOUBLE AS amount
            FROM range(${ROWS}) t(i)`,
      ],
   });
}

/** Whether a query on `path` reported the tier it must. */
function served(name: string, path: Path, servedFrom: string | null): boolean {
   if (path === "live") return servedFrom !== "storage";
   // A miss must be a clean compile miss, not a storage run that fell back.
   return name === "miss" ? servedFrom === null : servedFrom === "storage";
}

async function main() {
   const restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
   const level = logger.level;
   try {
      const fixtures: Record<Path, R4Fixture> = {
         live: await scenario(),
         warm: await scenario(),
         cold: await scenario(),
      };
      fixtures.live.setBindings([]);
      fixtures.warm.setBindings(fixtures.warm.bindings);
      // Untimed: the cold path invalidates the memoized serve shape here, so
      // the timed query compiles it.
      const prepare = (path: Path) => {
         if (path === "cold") fixtures.cold.setBindings(fixtures.cold.bindings);
      };

      logger.level = "error";
      const rows: string[] = [];
      for (const [name, q] of Object.entries(QUERIES)) {
         for (let i = 0; i < WARMUP; i++) {
            for (const path of PATHS) {
               prepare(path);
               await fixtures[path].runAsIs(q);
            }
         }
         const ms: Record<Path, number[]> = { live: [], warm: [], cold: [] };
         for (let i = 0; i < ITERATIONS; i++) {
            // Rotate the order, so each path is timed first a third of the time.
            for (let k = 0; k < PATHS.length; k++) {
               const path = PATHS[(i + k) % PATHS.length] as Path;
               prepare(path);
               const start = performance.now();
               const { servedFrom } = await fixtures[path].runAsIs(q);
               ms[path].push(performance.now() - start);
               if (!served(name, path, servedFrom)) {
                  throw new Error(
                     `${name} on the ${path} path reported servedFrom ${servedFrom}`,
                  );
               }
            }
         }
         const live = stats(ms.live);
         const warm = stats(ms.warm);
         const cold = stats(ms.cold);
         const over = (x: number) =>
            `${x - live.median >= 0 ? "+" : ""}${(x - live.median).toFixed(2)}`;
         rows.push(
            `| ${SOURCES} | ${name} | ${name === "miss" ? "miss, then live" : "storage"} | ${live.median.toFixed(2)} · ${live.p95.toFixed(2)} | ${warm.median.toFixed(2)} · ${warm.p95.toFixed(2)} (${over(warm.median)}) | ${cold.median.toFixed(2)} · ${cold.p95.toFixed(2)} (${over(cold.median)}) |`,
         );
      }
      logger.level = level;
      console.log(
         `\nRouter baseline: ${SOURCES} persisted source${SOURCES === 1 ? "" : "s"} bound, ${ROWS.toLocaleString()} fact rows, ${ITERATIONS} iterations after ${WARMUP} warm-up, paths interleaved, ms (median · p95; served minus live median in parentheses)\n`,
      );
      console.log(
         "| sources | query | path | live | served, warm | served, cold |",
      );
      console.log("| --- | --- | --- | --- | --- | --- |");
      for (const r of rows) console.log(r);
   } finally {
      logger.level = level;
      restore();
   }
}

await main();
