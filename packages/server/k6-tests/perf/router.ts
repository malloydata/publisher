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
 *   bun perf/router.ts [--rows 200000] [--iterations 50] [--sources 1]
 *
 * `--sources N` declares N persisted sources over the fact table, all bound.
 * The serve shape is compiled package-wide, over every binding, so its cost
 * grows with N; queries read the first.
 */

import { installSettings } from "../../src/test_helpers/settings_env";
import { buildScenario } from "../../tests/component/r4/harness";

function arg(name: string, fallback: number): number {
   const i = process.argv.indexOf(`--${name}`);
   return i > 0 ? Number(process.argv[i + 1]) : fallback;
}

const ROWS = arg("rows", 200_000);
const ITERATIONS = arg("iterations", 50);
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
   // carry it, so the storage variant fails to compile and the query runs live.
   miss: "run: facts -> { group_by: regions.region_name; aggregate: n }",
};

function stats(ms: number[]) {
   const sorted = [...ms].sort((a, b) => a - b);
   const at = (q: number): number =>
      sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ??
      Number.NaN;
   return { median: at(0.5), p95: at(0.95) };
}

async function time(
   fn: () => Promise<{ servedFrom: string | null }>,
   expected: (servedFrom: string | null) => boolean,
): Promise<number[]> {
   for (let i = 0; i < WARMUP; i++) await fn();
   const out: number[] = [];
   for (let i = 0; i < ITERATIONS; i++) {
      const start = performance.now();
      const { servedFrom } = await fn();
      out.push(performance.now() - start);
      if (!expected(servedFrom)) {
         throw new Error(`unexpected servedFrom ${servedFrom}`);
      }
   }
   return out;
}

async function main() {
   const restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
   try {
      const fixture = await buildScenario({
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
      const live = (q: string) => fixture.runWith([], q);
      const isLive = (s: string | null) => s !== "storage";
      const isStorage = (s: string | null) => s === "storage";

      const rows: string[] = [];
      for (const [name, q] of Object.entries(QUERIES)) {
         const liveMs = stats(await time(() => live(q), isLive));
         const hit = name !== "miss";
         // Warm: bindings set once, so the serve shape is memoized.
         fixture.setBindings(fixture.bindings);
         const warmMs = stats(
            await time(() => fixture.runAsIs(q), hit ? isStorage : isLive),
         );
         // Cold: bindings re-set before each query, so each compiles the shape.
         const coldMs = stats(
            await time(
               () => fixture.runWith(fixture.bindings, q),
               hit ? isStorage : isLive,
            ),
         );
         const over = (x: number) =>
            `${x - liveMs.median >= 0 ? "+" : ""}${(x - liveMs.median).toFixed(2)}`;
         rows.push(
            `| ${name} | ${hit ? "storage" : "miss, then live"} | ${liveMs.median.toFixed(2)} · ${liveMs.p95.toFixed(2)} | ${warmMs.median.toFixed(2)} · ${warmMs.p95.toFixed(2)} (${over(warmMs.median)}) | ${coldMs.median.toFixed(2)} · ${coldMs.p95.toFixed(2)} (${over(coldMs.median)}) |`,
         );
      }
      console.log(
         `\nRouter baseline: ${SOURCES} persisted source${SOURCES === 1 ? "" : "s"} bound, ${ROWS.toLocaleString()} fact rows, ${ITERATIONS} iterations after ${WARMUP} warm-up, ms (median · p95; served minus live median in parentheses)\n`,
      );
      console.log("| query | path | live | served, warm | served, cold |");
      console.log("| --- | --- | --- | --- | --- |");
      for (const r of rows) console.log(r);
   } finally {
      restore();
   }
}

await main();
