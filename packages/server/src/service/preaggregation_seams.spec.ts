// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The two pre-aggregation seams, over a real package: the BUILD PLAN (rollups get
// planned and reported with their provenance) and the SERVE path (a covered query
// routes through the composite and still answers correctly).
//
// preaggregation_synthesis.spec.ts already proves the mechanism against
// hand-written models. What only a real package can show is that the seams fire
// at all — that a rollup reaches the wire plan under the author's model path, and
// that routing a live query through the synthesized model does not change the
// answer.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";

let rootDir: string;
let envPath: string;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-preagg-seam-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
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

// The same model with the annotations stripped — same data, same measures, no
// rollups. Derived from MODEL rather than written out so the two cannot drift,
// which is what makes it a usable control for "routing changes no answer".
const MODEL_UNANNOTATED = MODEL.replace(/^\s*#@ preaggregate.*\n/gm, "");

async function loadPackageFiles(
   files: Record<string, string>,
): Promise<Package> {
   const dir = path.join(envPath, "pkg");
   await fs.mkdir(dir, { recursive: true });
   await fs.writeFile(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "pkg", description: "fixture" }),
   );
   for (const [name, contents] of Object.entries(files)) {
      await fs.writeFile(path.join(dir, name), contents);
   }
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage("pkg", false);
}

async function loadPackage(model = MODEL): Promise<Package> {
   return loadPackageFiles({ "model.malloy": model });
}

/** Run `query` and return its rows as plain objects. */
async function runGatedQuery(
   pkg: Package,
   query: string,
   givens: Record<string, unknown>,
) {
   const model = pkg.getModel("model.malloy");
   if (!model) throw new Error("model.malloy did not load");
   const { compactResult } = await model.getQueryResults(
      undefined,
      undefined,
      query,
      undefined,
      undefined,
      givens as never,
   );
   return (compactResult as Record<string, unknown>[]).map((row) =>
      Object.fromEntries(
         Object.entries(row).map(([k, v]) => [
            k,
            typeof v === "bigint" ? Number(v) : v,
         ]),
      ),
   );
}

function planSources(pkg: Package) {
   return Object.values(pkg.getBuildPlan()?.sources ?? {});
}

describe("the build-plan seam", () => {
   it(
      "plans nothing for a model that declares no rollup",
      async () => {
         // The seam runs for every model, so the case that keeps it honest is a
         // model it must leave alone: no `#@ preaggregate` and no `#@ persist`
         // means no plan at all, rather than an empty rollup invented for it.
         const pkg = await loadPackage(MODEL_UNANNOTATED);
         expect(planSources(pkg)).toEqual([]);
      },
      { timeout: 60000 },
   );

   it(
      "plans one rollup per grain, reported against the author's model",
      async () => {
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
      "plans a rollup for a model that declares no experimental flags",
      async () => {
         // The author's model needs no `##! experimental` of its own: the
         // synthesized companion uses `compose()` and `#@ persist` in its own
         // right and declares the flags for them, and `##!` flags do not cross an
         // import. Asserted because requiring one would be invisible — an
         // un-flagged model's annotation would go quietly inert, which is the
         // failure this feature is built to avoid.
         const pkg = await loadPackage(`source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'B')) AS t(amount, category)
""") extend {
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

   it(
      "reports the synthesized rollups, and only those, as preaggregate-origin",
      async () => {
         // The set Model.withoutPreaggregateEntries strips from the manifest, so
         // getting it wrong in either direction is a live bug: too wide drops an
         // author's own persist routing, too narrow puts a rollup entry in front
         // of a model that cannot use it. Read off the plan's `origin` rather
         // than matched on the `__preagg__` naming convention, which an author's
         // source could collide with.
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
         const authored = sources.find((s) => s.origin === "persist");
         const synthesized = sources.find((s) => s.origin === "preaggregate");
         const ids = pkg.getPreaggregateEntityIds();
         expect(ids.size).toBe(1);
         expect(ids.has(synthesized!.sourceEntityId!)).toBe(true);
         expect(ids.has(authored!.sourceEntityId!)).toBe(false);
      },
      { timeout: 60000 },
   );
});

describe("the serve seam", () => {
   /** Run `query` and return its rows as plain objects. */
   async function run(
      pkg: Package,
      query: string,
      givens?: Record<string, unknown>,
   ) {
      const model = pkg.getModel("model.malloy");
      if (!model) throw new Error("model.malloy did not load");
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         query,
         undefined,
         undefined,
         givens as never,
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

   // `order_by: category` is load-bearing, not tidiness. Both categories total
   // 30, so Malloy's default ordering (by the first measure, descending) is a
   // TIE, and the two shapes below break it differently — the live shape reads
   // the base while the routed one reads the composite. Without an explicit
   // order the row-for-row comparison in the first test failed on roughly two
   // runs in three, which reads as a routing bug and is not one.
   const COVERED =
      "run: orders -> { group_by: category; aggregate: total; order_by: category }";

   it(
      "answers a covered query identically to the same model without rollups",
      async () => {
         // The property that makes the whole mechanism safe: routing through a
         // rollup is a cache, so it must not be observable in the answer. The
         // control is the same model with its annotations stripped, so the only
         // difference between the two runs is whether a rollup exists.
         //
         // Nothing is materialized here, so the routed run exercises the
         // composite with its rollup member recomputing from the base — the path
         // a query takes before a build has run, and the one that would silently
         // return partial aggregates instead of re-aggregated ones if the emitted
         // measures were wrong.
         const live = await run(await loadPackage(MODEL_UNANNOTATED), COVERED);

         await fs.rm(path.join(envPath, "pkg"), {
            recursive: true,
            force: true,
         });
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

   it(
      "serves a source the companion does not import, rather than failing",
      async () => {
         // The case the routing block's catch exists for, and the one neither
         // test above reaches: annotations DO exist (so a companion is compiled
         // and every query on the file is offered to it) but the query names a
         // source the companion never imported, because that source has no
         // measure to roll up.
         //
         // `"falls back to live for a query no rollup covers"` does not cover
         // this: it groups by a field ON the annotated source, which compiles
         // against the companion fine and returns through the base member.
         // `"leaves a model with no annotations untouched"` does not either:
         // with no annotations there is no companion and the block is skipped.
         //
         // Malloy compiles lazily, so before the eager `getSQL` probe the
         // compile error escaped PAST that catch and surfaced as a 400 —
         // `Reference to undefined object 'regions'` — for every query naming an
         // unannotated sibling. With no flag to turn pre-aggregation off, that
         // made one annotation enough to break the rest of the file.
         const pkg = await loadPackage(`${MODEL}
source: regions is duckdb.sql("""
  SELECT * FROM (VALUES ('north'), ('north'), ('south')) AS t(region)
""") extend {
  measure: region_count is count()
}
`);
         const rows = await run(
            pkg,
            "run: regions -> { group_by: region; aggregate: region_count; order_by: region }",
         );
         expect(rows).toEqual([
            { region: "north", region_count: 2 },
            { region: "south", region_count: 1 },
         ]);
         // And the annotated source in the same file still routes.
         expect(await run(pkg, COVERED)).toEqual([
            { category: "A", total: 30 },
            { category: "B", total: 30 },
         ]);
      },
      { timeout: 60000 },
   );

   it(
      "answers a given-supplying query, which cannot route",
      async () => {
         // A model-level `given:` does not cross the companion's `import`, so the
         // companion surfaces no givens and a query supplying one cannot compile
         // against it. That is a coverage limit (documented in
         // docs/preaggregation.md) but it must not be an ERROR, and it is only
         // not one because the probe is given the same givens the run will use:
         // probe without them and the query compiles, then fails at run with
         // "unknown given 'MIN_AMOUNT'. Model surfaces []".
         //
         // The rollup entry is bound to a table that does not exist, so a query
         // that wrongly routed here would fail loudly instead of quietly
         // returning the right answer from the base.
         const pkg =
            await loadPackage(`##! experimental { persistence composite_sources givens }

given: MIN_AMOUNT :: number is 0

source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'A'), (30, 'B')) AS t(amount, category)
""") extend {
  where: amount >= $MIN_AMOUNT
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`);
         const rollupIds = [...pkg.getPreaggregateEntityIds()];
         expect(rollupIds).toHaveLength(1);
         pkg.bindColocatedServeManifest(
            Object.fromEntries(
               rollupIds.map((id) => [
                  id,
                  {
                     tableName: "no_such_rollup_table",
                     connectionName: "duckdb",
                  },
               ]),
            ),
         );
         const model = pkg.getModel("model.malloy");
         const { compactResult } = await model!.getQueryResults(
            undefined,
            undefined,
            "run: orders -> { group_by: category; aggregate: total; order_by: category }",
            undefined,
            undefined,
            { MIN_AMOUNT: 15 },
         );
         // 15 excludes the amount-10 row, so this also proves the given was
         // applied rather than dropped on the way through.
         expect(
            (compactResult as Record<string, unknown>[]).map((row) =>
               Object.fromEntries(
                  Object.entries(row).map(([k, v]) => [
                     k,
                     typeof v === "bigint" ? Number(v) : v,
                  ]),
               ),
            ),
         ).toEqual([
            { category: "A", total: 20 },
            { category: "B", total: 30 },
         ]);
      },
      { timeout: 60000 },
   );

   it(
      "keeps a synthesized rollup's manifest entry away from the author's model",
      async () => {
         // A rollup exists only in the companion, so its manifest entry can
         // substitute nothing in the author's model — and handing it over is not
         // merely useless: Malloy refuses a non-empty `buildManifest` against a
         // model without `##! experimental.persistence`, which a model that only
         // declares `#@ preaggregate` has no reason to carry (the companion
         // declares its own flags). A bound manifest therefore turned every query
         // served from the author's model into a 400.
         //
         // The query has to be one that does NOT reach the companion, since the
         // companion is the one runnable entitled to the full manifest. So this
         // asserts the fallback path specifically: an unannotated sibling, which
         // the companion never imports, with a rollup entry bound. Reachable only
         // because the eager compile probe above lets the fallback happen at all —
         // the two fixes have to land together.
         const pkg = await loadPackage(`source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'A'), (30, 'B')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}

source: regions is duckdb.sql("""
  SELECT * FROM (VALUES ('north'), ('north'), ('south')) AS t(region)
""") extend {
  measure: region_count is count()
}
`);
         const rollupIds = [...pkg.getPreaggregateEntityIds()];
         expect(rollupIds).toHaveLength(1);
         // Deliberately a table that does not exist: if the entry ever reaches
         // the author's model this test fails loudly rather than passing on a
         // substitution that happened to work.
         pkg.bindColocatedServeManifest(
            Object.fromEntries(
               rollupIds.map((id) => [
                  id,
                  {
                     tableName: "no_such_rollup_table",
                     connectionName: "duckdb",
                  },
               ]),
            ),
         );
         expect(pkg.hasBoundTableNameManifest()).toBe(true);
         expect(
            await run(
               pkg,
               "run: regions -> { group_by: region; aggregate: region_count; order_by: region }",
            ),
         ).toEqual([
            { region: "north", region_count: 2 },
            { region: "south", region_count: 1 },
         ]);
      },
      { timeout: 60000 },
   );
});

// ---------------------------------------------------------------------------
// Pre-aggregation x row-level `#(authorize)`. Nothing covered the combination,
// and the two tiers guard differently: `routingBlockedByRowLevelGate` was
// `&&`-ed with `storageRoutingPossible` and guarded only the storage branch,
// while the pre-aggregation branch had no gate check and `preaggRouted` is never
// reset.
// ---------------------------------------------------------------------------

describe("pre-aggregation and a row-level gate", () => {
   const GATED = `##! experimental { persistence composite_sources givens }

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (10, 'A', 1),
    (20, 'A', 2),
    (30, 'B', 1)
  ) AS t(amount, category, org_id)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;

   it(
      "plans the rollup — the gate stops materialization, not synthesis",
      async () => {
         // Worth pinning both halves separately. Synthesis is unaffected by the
         // gate, so the rollup reaches the plan; what keeps the combination safe
         // is that MATERIALIZING it is then refused, which is asserted directly
         // against the build gate in `materialization_eligibility.spec.ts` (a
         // rollup-shaped `#@ persist` over a gated base). A frozen rollup
         // pre-aggregates ACROSS org_id, so it could not be filtered by org_id
         // afterwards at all.
         const pkg = await loadPackage(GATED);
         expect(planSources(pkg)).toHaveLength(1);
      },
      { timeout: 60000 },
   );

   it(
      "answers a covered query with the gate's row filter applied, not the unfiltered rollup",
      async () => {
         // `category` IS the rollup's grain, so this is the query a rollup covers
         // — the one that would read a frozen table if one could exist. `org_id`
         // is not in the grain, so an unfiltered answer is unmistakable: A would
         // total 30 rather than 10.
         const pkg = await loadPackage(GATED);
         expect(
            await runGatedQuery(
               pkg,
               "run: orders -> { group_by: category; aggregate: total; order_by: category }",
               { GROUPS: [1] },
            ),
         ).toEqual([
            { category: "A", total: 10 },
            { category: "B", total: 30 },
         ]);
      },
      { timeout: 60000 },
   );

   it(
      "answers an uncovered query with the gate's row filter applied",
      async () => {
         // `org_id` is not in any rollup's grain, so the composite falls back to
         // the base member. This is the path on which `effectiveBuildManifest`
         // would hand a rollup manifest to a runnable rebuilt against the live
         // model if the tier were not blocked for a gated entry point.
         const pkg = await loadPackage(GATED);
         expect(
            await runGatedQuery(
               pkg,
               "run: orders -> { group_by: org_id; aggregate: total; order_by: org_id }",
               { GROUPS: [1] },
            ),
         ).toEqual([{ org_id: 1, total: 40 }]);
      },
      { timeout: 60000 },
   );

   it(
      "denies when the caller supplies no value for the gate's given",
      async () => {
         const pkg = await loadPackage(GATED);
         await expect(
            runGatedQuery(
               pkg,
               "run: orders -> { group_by: category; aggregate: total }",
               {},
            ),
         ).rejects.toThrow();
      },
      { timeout: 60000 },
   );
});

// ---------------------------------------------------------------------------
// The routing pre-check's own reachability. Blocking a gated entry point from
// the storage / pre-aggregation tiers is guarded by a model-wide "is there an
// authorize note ANYWHERE" sweep, so a deployment with rollups and no gates
// doesn't pay a live compile per query. A sweep that misses a gate un-guards
// the tier: this is the shape that missed.
// ---------------------------------------------------------------------------

describe("a gate reached only through a derivation hop", () => {
   // Three files, because two import hops is what it takes: the gate is
   // declared in `base`, `mid` derives a `query_source` from it, and `model`
   // imports only the derivation. `model`'s `contents` therefore holds `qs`
   // alone — `gated` is an INLINE struct hanging off `qs.query.structRef`, in
   // neither `contents` nor `sourceRegistry`. `ungated` is what turns the
   // pre-aggregation serve tier on, so the routing pre-check actually runs.
   const LAYERED = {
      "base.malloy": `##! experimental { persistence composite_sources givens }

given:
  GROUPS :: number[]

#(authorize) "org_id in $GROUPS"
source: gated is duckdb.sql("""
  SELECT * FROM (VALUES
    (10, 'A', 1),
    (20, 'A', 2),
    (30, 'B', 1)
  ) AS t(amount, category, org_id)
""")
`,
      "mid.malloy": `##! experimental { persistence composite_sources givens }
import { gated } from "base.malloy"

given:
  GROUPS :: number[]

source: qs is gated -> { select: * }
`,
      "model.malloy": `##! experimental { persistence composite_sources givens }
import { qs } from "mid.malloy"

given:
  GROUPS :: number[]

source: ungated is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'A')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
   };

   it(
      "is found by the routing pre-check's model-wide sweep",
      async () => {
         // Asserted on the predicate rather than on an answer, because a miss
         // here is not (yet) a wrong answer: it un-guards the tier, and what
         // catches the query after that is the authoritative gate. The
         // predicate's contract is what has to hold — a superset of every gate
         // the entry-point walks can reach — so that is what this pins.
         const pkg = await loadPackageFiles(LAYERED);
         const model = pkg.getModel("model.malloy");
         if (!model) throw new Error("model.malloy did not load");
         expect(
            (
               model as unknown as { hasAnyAuthorizeNote(): boolean }
            ).hasAnyAuthorizeNote(),
         ).toBe(true);
      },
      { timeout: 60000 },
   );

   it(
      "is still enforced on the gated entry point",
      async () => {
         // The other half: two import hops do not lose the gate. `org_id` 2 is
         // outside `GROUPS`, so an unfiltered answer is unmistakable.
         const pkg = await loadPackageFiles(LAYERED);
         expect(
            await runGatedQuery(
               pkg,
               "run: qs -> { group_by: org_id; aggregate: t is amount.sum(); order_by: org_id }",
               { GROUPS: [1] },
            ),
         ).toEqual([{ org_id: 1, t: 40 }]);
      },
      { timeout: 60000 },
   );
});
