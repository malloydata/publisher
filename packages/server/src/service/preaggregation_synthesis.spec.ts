// Tests for the synthesizer: the plan, the emitted text, and the two properties
// the seam decision made load-bearing — determinism, and that the emitted text
// actually routes against a real built table.
//
// The end-to-end tests here overlap the E2.1 spike deliberately. The spike proved
// the MECHANISM works against text written by hand; these prove the text this
// module GENERATES is that text. A green spike and a broken emitter would
// otherwise look identical from the outside.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { BuildManifest, FixedConnectionMap } from "@malloydata/malloy";
import { InMemoryURLReader, Runtime } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { computeSourceEntityId } from "./build_plan";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";
import {
   baseAlias,
   planModelPreaggregation,
   planSourcePreaggregation,
   rollupSourceName,
   synthesizePreaggregationModel,
   type RollupPlan,
} from "./preaggregation_synthesis";
import type { ValidatableSource } from "./preaggregation_validation";

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;

beforeAll(() => {
   ({ duckdb, connections } = duckdbTestConnections());
});

const ROWS = `SELECT * FROM (VALUES
    (10, DATE '2024-01-01', 'A'),
    (20, DATE '2024-01-01', 'B'),
    (30, DATE '2024-01-02', 'A')
  ) AS t(amount, order_date, category)`;

/** An author model with `body` inside `extend {}` on source `orders`. */
function authorModel(body: string): string {
   return `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""${ROWS}""") extend {
${body}
}
`;
}

/** Compile an author model and return source `orders` from its contents. */
async function compileOrders(body: string): Promise<ValidatableSource> {
   const compiled = await loadTestModel(
      connections,
      authorModel(body),
   ).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return (compiled as any)._modelDef.contents["orders"] as ValidatableSource;
}

async function planFor(body: string): Promise<RollupPlan[]> {
   return planSourcePreaggregation("orders", await compileOrders(body));
}

describe("the plan groups by grain, not by measure", () => {
   it("two measures at one grain share one rollup", async () => {
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()`);
      expect(plans).toHaveLength(1);
      expect(plans[0].grainDimensions).toEqual(["category"]);
      expect(plans[0].measures.map((m) => m.name)).toEqual([
         "order_count",
         "total",
      ]);
   });

   it("count's partial merges with SUM, not with count", async () => {
      // The classifier owns this rule; asserted here because the emitter is where
      // getting it wrong would produce a plausible wrong number rather than an
      // error, and this is the last place it can be caught by reading.
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: order_count is count()
  #@ preaggregate grain="category"
  measure: biggest is amount.max()`);
      const byName = Object.fromEntries(
         plans[0].measures.map((m) => [m.name, m.reaggregate]),
      );
      expect(byName).toEqual({ order_count: "sum", biggest: "max" });
   });

   it("different grains produce different rollups", async () => {
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="order_date"
  measure: daily is amount.sum()`);
      expect(plans).toHaveLength(2);
      expect(plans.map((p) => p.grainDimensions)).toEqual(
         // Sorted by rollup name, which is derived from the grain.
         expect.arrayContaining([["category"], ["order_date"]]),
      );
   });

   it("a measure that would be REFUSED at publish is skipped, not emitted", async () => {
      // The planner trusts the validator: an unusable declaration produces no
      // rollup rather than a second error, so the two cannot disagree about what
      // is buildable. Here everything is refusable — so there is nothing to build.
      expect(
         await planFor(`  #@ preaggregate grain="category"
  measure: avg_amount is amount.avg()
  #@ preaggregate
  measure: total is amount.sum()
  #@ preaggregate grain="nonexistent"
  measure: other is amount.sum()`),
      ).toEqual([
         // `other`'s grain does not resolve, but that is the validator's call and
         // it fails the publish; the planner only drops what it cannot express.
         expect.objectContaining({ grainDimensions: ["nonexistent"] }),
      ]);
   });

   it("a negated measure is not planned", async () => {
      expect(
         await planFor(`  #@ -preaggregate
  measure: total is amount.sum()`),
      ).toEqual([]);
   });
});

describe("determinism, because two legs synthesize independently", () => {
   it("grain order in the annotation does not change the plan", async () => {
      const a = await planFor(`  #@ preaggregate grain="category, order_date"
  measure: total is amount.sum()`);
      const b = await planFor(`  #@ preaggregate grain="order_date, category"
  measure: total is amount.sum()`);
      expect(a).toEqual(b);
      expect(a[0].grainDimensions).toEqual(["category", "order_date"]);
   });

   it("measure declaration order does not change the emitted text", async () => {
      const first = await planFor(`  #@ preaggregate grain="category"
  measure: alpha is amount.sum()
  #@ preaggregate grain="category"
  measure: zulu is amount.max()`);
      const second = await planFor(`  #@ preaggregate grain="category"
  measure: zulu is amount.max()
  #@ preaggregate grain="category"
  measure: alpha is amount.sum()`);
      expect(synthesizePreaggregationModel(first, "a.malloy")).toBe(
         synthesizePreaggregationModel(second, "a.malloy") as string,
      );
   });

   it("the name distinguishes grains a readable slug would collide", async () => {
      // `[a_b, c]` and `[a, b_c]` both slug to `a_b_c`; one name for two grains
      // would mean one table serving queries it does not cover.
      expect(rollupSourceName("s", ["a_b", "c"])).not.toBe(
         rollupSourceName("s", ["a", "b_c"]),
      );
   });

   it("the name is stable across calls and independent of grain order", async () => {
      expect(rollupSourceName("s", ["b", "a"])).toBe(
         rollupSourceName("s", ["b", "a"]),
      );
      // Callers sort before naming; this pins that the name is not accidentally
      // order-insensitive on its own, which would silently merge two grains.
      expect(rollupSourceName("s", ["a", "b"])).not.toBe(
         rollupSourceName("s", ["b", "a"]),
      );
   });

   it("no plans means no model, so a caller can skip the compile", () => {
      expect(synthesizePreaggregationModel([], "a.malloy")).toBeUndefined();
   });
});

describe("the emitted model", () => {
   const BODY = `  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()`;

   it("imports the base under an alias and composes with it last", async () => {
      const text = synthesizePreaggregationModel(
         await planFor(BODY),
         "orders.malloy",
      ) as string;
      expect(text).toContain(
         `import { ${baseAlias("orders")} is orders } from "orders.malloy"`,
      );
      // The base is the LAST member: the resolver takes the first covering member,
      // and the base covers everything, so anything after it would be dead.
      expect(text).toMatch(
         new RegExp(
            `source: orders is compose\\(.+, ${baseAlias("orders")}\\)`,
         ),
      );
      expect(text).toContain("#@ persist");
   });

   it("stores partials and re-declares each measure under its ORIGINAL name", async () => {
      const text = synthesizePreaggregationModel(
         await planFor(BODY),
         "orders.malloy",
      ) as string;
      // Stored column renamed, measure name preserved — a query asking for
      // `total` has to find `total` on the member or the member is rejected.
      expect(text).toContain("total__partial is total");
      expect(text).toContain("total is total__partial.sum()");
      expect(text).toContain("order_count__partial is order_count");
      expect(text).toContain("order_count is order_count__partial.sum()");
   });

   it("gives one import and one compose per base, however many grains", async () => {
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="order_date"
  measure: daily is amount.sum()`);
      const text = synthesizePreaggregationModel(
         plans,
         "orders.malloy",
      ) as string;
      expect(text.match(/^import /gm)).toHaveLength(1);
      expect(text.match(/^source: orders is compose\(/gm)).toHaveLength(1);
      expect(text.match(/^#@ persist$/gm)).toHaveLength(2);
   });
});

/**
 * Compile an author model and its synthesized companion as two real files, build
 * every rollup in the plan, and return what is needed to check routing.
 */
async function synthesizeAndBuild(body: string): Promise<{
   /** Load a query against the synthesized model. */
   load: (file: string) => ReturnType<Runtime["loadModel"]>;
   manifest: BuildManifest;
   tables: string[];
}> {
   const root = `file:///syn${Math.random().toString(36).slice(2)}/`;
   const source = await compileOrders(body);
   const plans = planSourcePreaggregation("orders", source);
   const synth = synthesizePreaggregationModel(
      plans,
      "author.malloy",
   ) as string;
   const urlReader = new InMemoryURLReader(
      new Map([
         [`${root}author.malloy`, authorModel(body)],
         [`${root}synth.malloy`, synth],
      ]),
   );
   const load = (file: string) =>
      new Runtime({ urlReader, connections }).loadModel(
         new URL(`${root}${file}`),
         {
            importBaseURL: new URL(root),
         },
      );

   const digest = await duckdb.getDigest();
   const plan = (await load("synth.malloy").getModel()).getBuildPlan();
   const entries: BuildManifest["entries"] = {};
   const tables: string[] = [];
   for (const built of Object.values(plan.sources)) {
      // A table name DuckDB accepts unquoted, from a name that may be long.
      const table = `mz_${built.name.replace(/[^a-zA-Z0-9_]/g, "_")}`;
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${table} AS ${built.getSQL()}`,
      );
      entries[computeSourceEntityId(built, { duckdb: digest })] = {
         tableName: table,
      };
      tables.push(table);
   }
   return { load, manifest: { entries, strict: false }, tables };
}

describe("end to end: the emitted text builds, routes, and agrees with live", () => {
   const BODY = `  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()`;

   it("the build plan contains the rollup and NOT the imported base", async () => {
      // The base is imported, not persisted, so it must not turn into a build. If
      // it did, every pre-aggregated package would silently materialize its base.
      const { tables } = await synthesizeAndBuild(BODY);
      expect(tables).toHaveLength(1);
      expect(tables[0]).toContain("preagg");
      expect(tables[0]).not.toContain("preagg_base");
   });

   it("a covered query reads the built table", async () => {
      const { load, manifest, tables } = await synthesizeAndBuild(BODY);
      const query = "run: orders -> { group_by: category; aggregate: total }";
      const sql = await load("synth.malloy")
         .loadQuery(query)
         .getSQL({ buildManifest: manifest });
      expect(sql).toContain(tables[0]);
      // And without the manifest it must NOT, or the assertion above proves
      // nothing about substitution.
      const live = await load("synth.malloy").loadQuery(query).getSQL();
      expect(live).not.toContain(tables[0]);
   });

   it("a re-aggregated COUNT equals the live count", async () => {
      // The count trap, end to end: a stored count re-read with `count` would
      // return the number of rollup rows. Only a value assertion catches it.
      const { load, manifest } = await synthesizeAndBuild(BODY);
      const query =
         "run: orders -> { group_by: category; aggregate: order_count, total }";
      const served = await load("synth.malloy")
         .loadQuery(query)
         .run({ buildManifest: manifest });
      const live = await load("author.malloy").loadQuery(query).run();
      expect(served.data.toObject()).toEqual(live.data.toObject());
      // Pinned explicitly: 'A' has two rows, so a wrong merge reads 1 here.
      const rows = served.data.toObject() as unknown as Record<
         string,
         unknown
      >[];
      expect(rows.find((r) => r.category === "A")?.order_count).toBe(2);
   });

   it("one measure at TWO grains builds two rollups, and both route", async () => {
      // Why the reader walks annotation notes instead of trusting the merged tag.
      // Coverage alone would not justify this — the combined grain covers both
      // queries — but cost does: a combined grain multiplies cardinalities, so a
      // query by `category` should read the small `category` table rather than one
      // nearly as large as the base.
      const { load, manifest, tables } = await synthesizeAndBuild(
         `  #@ preaggregate grain="category"
  #@ preaggregate grain="order_date"
  measure: total is amount.sum()`,
      );
      expect(tables).toHaveLength(2);

      // Each query must reach its OWN rollup, not merely some rollup — that is the
      // whole benefit, and the assertion a single combined table would fail.
      const byGrain: [string, string][] = [
         [
            "run: orders -> { group_by: category; aggregate: total }",
            "category",
         ],
         [
            "run: orders -> { group_by: order_date; aggregate: total }",
            "order_date",
         ],
      ];
      for (const [query, grain] of byGrain) {
         const expected = tables.find((t) => t.includes(grain));
         expect(expected).toBeDefined();
         const sql = await load("synth.malloy")
            .loadQuery(query)
            .getSQL({ buildManifest: manifest });
         expect(sql).toContain(expected as string);
         const served = await load("synth.malloy")
            .loadQuery(query)
            .run({ buildManifest: manifest });
         const live = await load("author.malloy").loadQuery(query).run();
         expect(served.data.toObject()).toEqual(live.data.toObject());
      }
   });

   it("a rollup serves queries COARSER than its own grain", async () => {
      // Coverage, as distinct from cost. A rollup at `category, order_date` is not
      // limited to that pair: a query grouping by either dimension alone, or by
      // nothing at all, is answered by summing its partials. So a combined grain is
      // a legitimate third choice — one table instead of two, at the price of being
      // larger than either — and an author picks between them on size.
      const { load, manifest, tables } = await synthesizeAndBuild(
         `  #@ preaggregate grain="category, order_date"
  measure: total is amount.sum()
  #@ preaggregate grain="category, order_date"
  measure: order_count is count()`,
      );
      for (const query of [
         "run: orders -> { group_by: category; aggregate: total }",
         "run: orders -> { group_by: order_date; aggregate: total }",
         "run: orders -> { aggregate: total }",
         // The count too: re-aggregating across the dropped dimension is where a
         // wrong merge function would show up as a plausible-looking number.
         "run: orders -> { group_by: category; aggregate: order_count }",
      ]) {
         const sql = await load("synth.malloy")
            .loadQuery(query)
            .getSQL({ buildManifest: manifest });
         expect(sql).toContain(tables[0]);
         const served = await load("synth.malloy")
            .loadQuery(query)
            .run({ buildManifest: manifest });
         const live = await load("author.malloy").loadQuery(query).run();
         expect(served.data.toObject()).toEqual(live.data.toObject());
      }
   });

   it("CANARY: a query naming a VIEW does not route, because compose() drops views", async () => {
      // A real coverage limit, pinned so it is a documented fact rather than
      // folklore. `compose()` carries its members' fields but not their VIEWS, so
      // the composite has `revenue` and `category` and no `by_cat`. A query naming
      // the view therefore does not compile against the synthesized model at all
      // and is served live — the right failure (correct answer, no acceleration),
      // but it means the REST `queryName` form and any dashboard built on named
      // views go unaccelerated while the equivalent ad-hoc query routes.
      //
      // If this ever starts passing, compose() has gained view inheritance:
      // delete the canary and update the "What does not route" section of
      // docs/preaggregation.md, which documents this to authors.
      const { load, manifest, tables } = await synthesizeAndBuild(
         `  #@ preaggregate grain="category"
  measure: revenue is amount.sum()

  view: by_cat is {
    group_by: category
    aggregate: revenue
  }`,
      );
      await expect(
         load("synth.malloy")
            .loadQuery("run: orders -> by_cat")
            .getSQL({ buildManifest: manifest }),
      ).rejects.toThrow("'by_cat' is not defined");

      // The same shape written ad hoc DOES route, which is what identifies the
      // cause as the view rather than the grain.
      const sql = await load("synth.malloy")
         .loadQuery("run: orders -> { group_by: category; aggregate: revenue }")
         .getSQL({ buildManifest: manifest });
      expect(sql).toContain(tables[0]);
   });

   it("an UNCOVERED query falls back to the base and is still correct", async () => {
      const { load, manifest, tables } = await synthesizeAndBuild(BODY);
      const query = "run: orders -> { group_by: order_date; aggregate: total }";
      const sql = await load("synth.malloy")
         .loadQuery(query)
         .getSQL({ buildManifest: manifest });
      expect(sql).not.toContain(tables[0]);
      const served = await load("synth.malloy")
         .loadQuery(query)
         .run({ buildManifest: manifest });
      const live = await load("author.malloy").loadQuery(query).run();
      expect(served.data.toObject()).toEqual(live.data.toObject());
   });

   it("a measure through a join_one is served correctly", async () => {
      // The case the narrowed fan-out gate exists to allow. Nothing in the emitter
      // handles joins specially — the rollup names the base's measure, so the join
      // comes along with it — which is exactly why this needs a test.
      const root = "file:///synj/";
      const author = `##! experimental { persistence composite_sources }
source: prices is duckdb.sql("""SELECT * FROM (VALUES
    ('A', 2), ('B', 5)
  ) AS t(category, unit_cost)""")
source: orders is duckdb.sql("""${ROWS}""") extend {
  join_one: price is prices on category = price.category
  #@ preaggregate grain="category"
  measure: total_cost is price.unit_cost.sum()
}
`;
      const compiled = await loadTestModel(connections, author).getModel();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const source = (compiled as any)._modelDef.contents["orders"];
      const plans = planSourcePreaggregation("orders", source);
      expect(plans).toHaveLength(1);

      const synth = synthesizePreaggregationModel(
         plans,
         "author.malloy",
      ) as string;
      const urlReader = new InMemoryURLReader(
         new Map([
            [`${root}author.malloy`, author],
            [`${root}synth.malloy`, synth],
         ]),
      );
      const load = (file: string) =>
         new Runtime({ urlReader, connections }).loadModel(
            new URL(`${root}${file}`),
            { importBaseURL: new URL(root) },
         );

      const digest = await duckdb.getDigest();
      const plan = (await load("synth.malloy").getModel()).getBuildPlan();
      const entries: BuildManifest["entries"] = {};
      let table = "";
      for (const built of Object.values(plan.sources)) {
         table = `mz_${built.name.replace(/[^a-zA-Z0-9_]/g, "_")}`;
         await duckdb.runSQL(
            `CREATE OR REPLACE TABLE ${table} AS ${built.getSQL()}`,
         );
         entries[computeSourceEntityId(built, { duckdb: digest })] = {
            tableName: table,
         };
      }
      const manifest = { entries, strict: false };
      const query =
         "run: orders -> { group_by: category; aggregate: total_cost }";
      const sql = await load("synth.malloy")
         .loadQuery(query)
         .getSQL({ buildManifest: manifest });
      expect(sql).toContain(table);
      const served = await load("synth.malloy")
         .loadQuery(query)
         .run({ buildManifest: manifest });
      const live = await load("author.malloy").loadQuery(query).run();
      expect(served.data.toObject()).toEqual(live.data.toObject());
   });
});

describe("THE IDENTITY TEST: two independent syntheses agree on sourceEntityId", () => {
   it("synthesizing twice yields the same entity id", async () => {
      // The property the seam decision rests on. The build leg and the serve leg
      // each run this module; if their rollups hash differently the serve model
      // binds to a table the build leg never created, every query quietly falls
      // back to live, and nothing anywhere reports an error — the failure is a
      // bill, not a stack trace.
      const body = `  #@ preaggregate grain="order_date, category"
  measure: total is amount.sum()
  #@ preaggregate grain="category, order_date"
  measure: order_count is count()`;

      const ids: string[] = [];
      const digest = await duckdb.getDigest();
      for (const attempt of [0, 1]) {
         // A fresh compile of the author model each time, and a distinct URL, so
         // nothing is shared between the two runs but this module's output.
         const root = `file:///id${attempt}/`;
         const source = await compileOrders(body);
         const plans = planSourcePreaggregation("orders", source);
         const synth = synthesizePreaggregationModel(
            plans,
            "author.malloy",
         ) as string;
         const urlReader = new InMemoryURLReader(
            new Map([
               [`${root}author.malloy`, authorModel(body)],
               [`${root}synth.malloy`, synth],
            ]),
         );
         const model = await new Runtime({ urlReader, connections })
            .loadModel(new URL(`${root}synth.malloy`), {
               importBaseURL: new URL(root),
            })
            .getModel();
         const built = Object.values(model.getBuildPlan().sources);
         expect(built).toHaveLength(1);
         ids.push(computeSourceEntityId(built[0], { duckdb: digest }));
      }
      expect(ids[0]).toBe(ids[1]);
   });
});

describe("planning a whole model", () => {
   it("reaches every source and orders the result deterministically", async () => {
      const text = `##! experimental { persistence composite_sources }
source: zulu is duckdb.sql("""${ROWS}""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
source: alpha is duckdb.sql("""${ROWS}""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;
      const compiled = await loadTestModel(connections, text).getModel();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const contents = (compiled as any)._modelDef.contents;
      const plans = planModelPreaggregation(contents);
      expect(plans.map((p) => p.baseSourceName)).toEqual(["alpha", "zulu"]);
   });
});
