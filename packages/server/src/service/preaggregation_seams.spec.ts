// The two pre-aggregation seams, over a real package: the BUILD PLAN (rollups get
// planned and reported with their provenance) and the SERVE path (a covered query
// routes through the composite and still answers correctly).
//
// preaggregation_synthesis.spec.ts already proves the mechanism against
// hand-written models. What only a real package can show is that the seams fire
// at all — that `PREAGGREGATE_MODE` gates them, that a rollup reaches the wire
// plan under the author's model path, and that routing a live query through the
// synthesized model does not change the answer.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";

let rootDir: string;
let envPath: string;
let savedMode: string | undefined;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-preagg-seam-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
   savedMode = process.env.PREAGGREGATE_MODE;
});

afterEach(async () => {
   if (savedMode === undefined) delete process.env.PREAGGREGATE_MODE;
   else process.env.PREAGGREGATE_MODE = savedMode;
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

// Two grains and two measures, so the plan has something to group: `total` is
// rolled up at BOTH `category` and `order_day` — two annotations on one measure,
// two rollups — and `order_count` at `category`, which it therefore shares with
// `total`. `count()` is here because its partial re-aggregates with SUM rather
// than with itself, which is the case a naive synthesizer gets wrong.
const MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (10, 'A', TIMESTAMP '2024-01-01 08:00:00'),
    (20, 'A', TIMESTAMP '2024-01-01 09:00:00'),
    (30, 'B', TIMESTAMP '2024-01-02 08:00:00')
  ) AS t(amount, category, order_time)
""") extend {
  dimension: order_day is order_time.day
  #@ preaggregate grain="category"
  #@ preaggregate grain="order_day"
  measure: total is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()
}
`;

async function loadPackage(model = MODEL): Promise<Package> {
   const dir = path.join(envPath, "pkg");
   await fs.mkdir(dir, { recursive: true });
   await fs.writeFile(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "pkg", description: "fixture" }),
   );
   await fs.writeFile(path.join(dir, "model.malloy"), model);
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage("pkg", false);
}

function planSources(pkg: Package) {
   return Object.values(pkg.getBuildPlan()?.sources ?? {});
}

describe("the build-plan seam", () => {
   it(
      "plans nothing extra while the feature is off",
      async () => {
         process.env.PREAGGREGATE_MODE = "off";
         const pkg = await loadPackage();
         // The model declares no `#@ persist` of its own, so with rollups
         // suppressed there is no plan at all.
         expect(planSources(pkg)).toEqual([]);
      },
      { timeout: 60000 },
   );

   it(
      "plans one rollup per grain, reported against the author's model",
      async () => {
         process.env.PREAGGREGATE_MODE = "build-only";
         const pkg = await loadPackage();
         const sources = planSources(pkg);
         // Two distinct grains ⇒ two rollups. Note that is neither one per measure
         // nor one per annotation: `total` declares two grains and gets two
         // rollups, while its `category` grain is SHARED with `order_count` so the
         // two measures land in one table and one GROUP BY.
         expect(sources).toHaveLength(2);
         for (const source of sources) {
            expect(source.origin).toBe("preaggregate");
            // A rollup is declared by no file, so it reports the model holding
            // the annotations it came from — where an author would go to change it.
            expect(source.modelPath).toBe("model.malloy");
            expect(source.preaggregate?.baseSourceName).toBe("orders");
            // It must be a real, buildable plan entry and not just provenance.
            expect(source.sql).toContain("SELECT");
            expect(source.sourceEntityId).toBeTruthy();
         }
         const byGrain = new Map(
            sources.map((s) => [
               (s.preaggregate?.grainDimensions ?? []).join(","),
               s.preaggregate?.measures ?? [],
            ]),
         );
         // The grain is reported sorted, so two authors writing the same
         // dimensions in different orders land on one rollup rather than two.
         expect([...byGrain.keys()].sort()).toEqual(["category", "order_day"]);
         expect(byGrain.get("category")).toEqual(["order_count", "total"]);
         expect(byGrain.get("order_day")).toEqual(["total"]);
      },
      { timeout: 60000 },
   );

   it(
      "plans rollups even though the base is not a persist source",
      async () => {
         // The seam runs ahead of the two `continue`s that skip a model with no
         // persist source and no `experimental.persistence` flag. Both would make
         // a valid annotation a silent no-op, which is what the publish gate
         // exists to prevent — so this is the case that keeps them honest.
         process.env.PREAGGREGATE_MODE = "build-only";
         const pkg = await loadPackage(`##! experimental.composite_sources
source: orders is duckdb.sql("""SELECT 10 AS amount, 'A' AS category""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`);
         expect(planSources(pkg)).toHaveLength(1);
      },
      { timeout: 60000 },
   );

   it(
      "leaves an ordinary persist source alone, and marks it as authored",
      async () => {
         process.env.PREAGGREGATE_MODE = "build-only";
         const pkg =
            await loadPackage(`##! experimental { persistence composite_sources }
source: raw is duckdb.sql("""SELECT 10 AS amount, 'A' AS category""")

#@ persist name="orders_t"
source: orders is raw -> { group_by: category; aggregate: amount_sum is amount.sum() } extend {
  #@ preaggregate grain="category"
  measure: total is amount_sum.sum()
}
`);
         const sources = planSources(pkg);
         const byOrigin = new Map(sources.map((s) => [s.origin, s]));
         // Both entries, each labelled — and exactly two, which is the assertion
         // that matters: the synthesized model imports the author's, so its plan
         // sees `orders` too, and counting it twice would look like two plan
         // entries for one table (a persist-target collision).
         expect(sources).toHaveLength(2);
         expect(byOrigin.get("persist")?.name).toBe("orders");
         expect(byOrigin.get("persist")?.preaggregate ?? null).toBeNull();
         expect(
            byOrigin.get("preaggregate")?.preaggregate?.baseSourceName,
         ).toBe("orders");
      },
      { timeout: 60000 },
   );
});

describe("the serve seam", () => {
   /** Run `query` and return its rows as plain objects. */
   async function run(pkg: Package, query: string) {
      const model = pkg.getModel("model.malloy");
      if (!model) throw new Error("model.malloy did not load");
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         query,
      );
      // Numbers arrive as bigint from DuckDB sums; normalize so the assertions
      // read as the numbers an author would expect.
      return (compactResult as Record<string, unknown>[]).map((row) =>
         Object.fromEntries(
            Object.entries(row).map(([k, v]) => [
               k,
               typeof v === "bigint" ? Number(v) : v,
            ]),
         ),
      );
   }

   const COVERED = "run: orders -> { group_by: category; aggregate: total }";

   it(
      "answers a covered query identically with routing on and off",
      async () => {
         // The property that makes the whole mechanism safe: routing through a
         // rollup is a cache, so it must not be observable in the answer. Nothing
         // is materialized in this test, so `on` exercises the composite with its
         // rollup member recomputing from the base — the path a query takes
         // before a build has run, and the one that would silently return partial
         // aggregates instead of re-aggregated ones if the emitted measures were
         // wrong.
         process.env.PREAGGREGATE_MODE = "off";
         const live = await run(await loadPackage(), COVERED);

         await fs.rm(path.join(envPath, "pkg"), {
            recursive: true,
            force: true,
         });
         process.env.PREAGGREGATE_MODE = "on";
         const routed = await run(await loadPackage(), COVERED);

         expect(routed).toEqual(live);
         // And the numbers are actually right, not merely equal to each other.
         expect(routed).toEqual([
            { category: "A", total: 30 },
            { category: "B", total: 30 },
         ]);
      },
      { timeout: 60000 },
   );

   it(
      "re-aggregates a count correctly through the rollup",
      async () => {
         // `count()`'s partial re-aggregates with SUM. If it re-aggregated with
         // count() the answer would be the number of GROUPS, which for this
         // fixture is 2 rather than 3 — close enough to look plausible, which is
         // why it is asserted.
         process.env.PREAGGREGATE_MODE = "on";
         const rows = await run(
            await loadPackage(),
            "run: orders -> { aggregate: order_count }",
         );
         expect(rows).toEqual([{ order_count: 3 }]);
      },
      { timeout: 60000 },
   );

   it(
      "falls back to live for a query no rollup covers",
      async () => {
         process.env.PREAGGREGATE_MODE = "on";
         const pkg = await loadPackage();
         // `order_time` is not a grain of either rollup, so this can only be
         // answered from the base.
         const rows = await run(
            pkg,
            "run: orders -> { group_by: order_time; aggregate: total; order_by: order_time }",
         );
         expect(rows).toHaveLength(3);
      },
      { timeout: 60000 },
   );

   it(
      "leaves a model with no annotations untouched",
      async () => {
         process.env.PREAGGREGATE_MODE = "on";
         const pkg = await loadPackage(`##! experimental.composite_sources
source: orders is duckdb.sql("""SELECT 10 AS amount, 'A' AS category""") extend {
  measure: total is amount.sum()
}
`);
         const rows = await run(pkg, COVERED);
         expect(rows).toEqual([{ category: "A", total: 10 }]);
      },
      { timeout: 60000 },
   );
});
