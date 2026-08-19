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
import { readPreaggregateAnnotation } from "./preaggregation_annotation";
import {
   baseAlias,
   planModelPreaggregation,
   isSpliceableNamespace,
   persistNamespace,
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

/** An author model whose `orders` carries `annotation` above it. */
function annotatedAuthorModel(annotation: string, body: string): string {
   return `##! experimental { persistence composite_sources }
${annotation}
source: orders is duckdb.sql("""${ROWS}""") extend {
${body}
}
`;
}

/** Compile an annotated author model and return source `orders`. */
async function compileAnnotatedOrders(
   annotation: string,
   body: string,
): Promise<ValidatableSource> {
   const compiled = await loadTestModel(
      connections,
      annotatedAuthorModel(annotation, body),
   ).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return (compiled as any)._modelDef.contents["orders"] as ValidatableSource;
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

describe("a rollup is created where its base lives", () => {
   const GRAIN = `  measure:\n    #@ preaggregate grain="category"\n    total is sum(amount)`;

   it("qualifies the rollup with the base's namespace", async () => {
      // A rollup of X belongs beside X. On a dialect that requires qualification
      // this is also what makes it buildable at all: BigQuery rejects an
      // unqualified CREATE, so a bare name is not a cosmetic difference there.
      const plans = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders(
            '#@ persist name="analytics.orders_tbl"',
            GRAIN,
         ),
      );
      expect(plans).toHaveLength(1);
      expect(plans[0].namespace).toBe("analytics");
      expect(synthesizePreaggregationModel(plans, "./orders.malloy")).toContain(
         `#@ persist name="analytics.${plans[0].rollupSourceName}"`,
      );
   });

   it("takes the namespace, not the base's table name", async () => {
      // The rollup is its own table; only the namespace is inherited. Appending to
      // the base's table name would collide two different things in one name.
      const plans = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders(
            '#@ persist name="analytics.orders_tbl"',
            GRAIN,
         ),
      );
      const emitted =
         synthesizePreaggregationModel(plans, "./orders.malloy") ?? "";
      expect(emitted).not.toContain("orders_tbl__preagg");
   });

   it("keeps a project-qualified namespace whole", async () => {
      // BigQuery names can carry a project as well as a dataset, so the split is on
      // the LAST dot: the rollup lands in the same dataset, not the same project.
      const plans = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders(
            '#@ persist name="proj.analytics.orders_tbl"',
            GRAIN,
         ),
      );
      expect(plans[0].namespace).toBe("proj.analytics");
   });

   it("takes an author-declared namespace over the base's", async () => {
      // `namespace=` is the answer for a base that is not persisted at all — which
      // is legal, since `#@ preaggregate` goes on a measure — and an override when
      // it is. It names the container only; the table name stays derived.
      const plans = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders(
            '#@ persist name="analytics.orders_tbl"',
            `  measure:\n    #@ preaggregate grain="category" namespace="rollups"\n    total is sum(amount)`,
         ),
      );
      expect(plans[0].namespace).toBe("rollups");
      expect(synthesizePreaggregationModel(plans, "./orders.malloy")).toContain(
         `#@ persist name="rollups.${plans[0].rollupSourceName}"`,
      );
   });

   it("accepts a namespace on a base that is not persisted", async () => {
      // The case the fallback cannot serve: nothing to inherit from.
      const plans = await planFor(
         `  measure:\n    #@ preaggregate grain="category" namespace="rollups"\n    total is sum(amount)`,
      );
      expect(plans[0].namespace).toBe("rollups");
   });

   it("gives each grain the namespace named at it", async () => {
      // Two grains are two tables and can be created in two places. A namespace
      // resolved across the source would apply one grain's choice to the other's
      // table, silently, decided by whichever the IR reported first.
      const plans = await planFor(
         `  measure:\n    #@ preaggregate grain="category" namespace="ns_a"\n    total is sum(amount)\n  measure:\n    #@ preaggregate grain="order_date" namespace="ns_b"\n    daily is sum(amount)`,
      );
      expect(plans.map((p) => [p.grainDimensions, p.namespace])).toEqual([
         [["category"], "ns_a"],
         [["order_date"], "ns_b"],
      ]);
   });

   it("does not lend a storage= base's namespace to a colocated rollup", async () => {
      // A `storage=` base's `name=` is a name in the DESTINATION's catalog, while
      // the rollup is colocated — synthesis emits a bare `#@ persist`. Inheriting
      // across that boundary would aim CREATE TABLE at a schema of the
      // destination's that need not exist in the source warehouse, so adding
      // `storage=` to a working base would break its rollup.
      const plans = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders(
            '#@ persist storage="lake" name="lakeschema.orders_tbl"',
            `  measure:\n    #@ preaggregate grain="category"\n    total is sum(amount)`,
         ),
      );
      expect(plans[0].namespace).toBeUndefined();
      // An author who wants one still says so, and that is honoured.
      const declared = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders(
            '#@ persist storage="lake" name="lakeschema.orders_tbl"',
            `  measure:\n    #@ preaggregate grain="category" namespace="rollups"\n    total is sum(amount)`,
         ),
      );
      expect(declared[0].namespace).toBe("rollups");
   });

   it("derives nothing from a quoted or malformed base name", async () => {
      // A quoted name is canonical SQL, not a dotted identifier path: `"My.Schema"`
      // is ONE identifier containing a dot, and a last-dot split tears it in half.
      // Even a well-formed `"A"."B"` would hand back a quoted prefix that joins to
      // an unquoted derived segment, and the CREATE and bind sides quote a mixed
      // path differently. Yield nothing and let the author name it explicitly.
      expect(persistNamespace('"My.Schema"')).toBeUndefined();
      expect(persistNamespace("`My.Schema`")).toBeUndefined();
      expect(persistNamespace('"Analytics"."Orders Tbl"')).toBeUndefined();
      // ...but a quoted TABLE beside a plain namespace is fine: the rollup takes
      // only the namespace, so both of its segments are unquoted and the mixed-path
      // disagreement never arises.
      expect(persistNamespace('analytics."Orders Tbl"')).toBe("analytics");
      // A trailing dot leaves the base's own table segment empty — malformed, so
      // inventing a namespace from it would hide that.
      expect(persistNamespace("trailing.")).toBeUndefined();
      // The forms that ARE sound still work.
      expect(persistNamespace("analytics.orders")).toBe("analytics");
      expect(persistNamespace("proj.analytics.orders")).toBe("proj.analytics");
      expect(persistNamespace("orders")).toBeUndefined();
   });

   it("refuses a quoted namespace= rather than emitting a broken annotation", async () => {
      // The value is interpolated into a generated `#@ persist name="…"`, where a
      // quote character ends the annotation's own string.
      const source = await compileOrders(
         `  measure:\n    #@ preaggregate grain="category" namespace="\\"Odd\\""\n    total is sum(amount)`,
      );
      const declaration = readPreaggregateAnnotation(
         (source.fields ?? []).find(
            (f) => (f.as ?? f.name) === "total",
         ) as never,
      );
      expect(declaration.errors.map((e) => e.kind)).toContain(
         "invalid_namespace",
      );
      // The line is refused whole, so its grain goes with it: a rollup must never
      // be built in a namespace the author did not get to choose.
      expect(declaration.grains).toHaveLength(0);
   });

   it("accepts the namespaces a real deployment needs, and rejects the rest", () => {
      // Dot-separated because BigQuery addresses a dataset as `project.dataset`,
      // and hyphens because a BigQuery project id ordinarily carries them — the
      // dialect this feature broke on requires both.
      expect(isSpliceableNamespace("analytics")).toBe(true);
      expect(isSpliceableNamespace("my-project.analytics")).toBe(true);
      expect(isSpliceableNamespace("_staging$1")).toBe(true);
      // A space or a quote cannot be written bare into a generated name; a leading
      // digit is not an identifier; an empty part means a stray or doubled dot.
      expect(isSpliceableNamespace("my schema")).toBe(false);
      expect(isSpliceableNamespace('"Analytics"')).toBe(false);
      expect(isSpliceableNamespace("1analytics")).toBe(false);
      expect(isSpliceableNamespace("a..b")).toBe(false);
      expect(isSpliceableNamespace("")).toBe(false);
      // Nothing that could end the annotation's string or the statement.
      expect(isSpliceableNamespace('a";DROP TABLE x;--')).toBe(false);
   });

   it("emits a bare persist when the base names no namespace", async () => {
      // Nothing to inherit, so nothing is invented — nor is the previous behaviour
      // changed for the dialects that accept an unqualified name.
      const unqualified = planSourcePreaggregation(
         "orders",
         await compileAnnotatedOrders('#@ persist name="orders_tbl"', GRAIN),
      );
      expect(unqualified[0].namespace).toBeUndefined();
      expect(
         synthesizePreaggregationModel(unqualified, "./orders.malloy"),
      ).toContain("#@ persist\nsource:");

      const unpersisted = await planFor(GRAIN);
      expect(unpersisted[0].namespace).toBeUndefined();
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

   it("CANARY: when two grains both cover a query, the FIRST member wins, not the smallest", async () => {
      // Pinned because it bounds what multiple grains buy, and the natural
      // assumption is the opposite. Members are emitted sorted by synthesized
      // name (grain slug plus digest), and the resolver takes the first that
      // covers the query — a name has nothing to do with size, so where two
      // declared grains BOTH cover a query the larger can win.
      //
      // Built to make that visible: 200 distinct `a_dim`, 2 distinct `b_dim`, so
      // the {b_dim} rollup is 2 rows and {a_dim, b_dim} is 200, and both answer a
      // by-b_dim query. Dimensions are sorted into the slug, so `a_dim_b_dim`
      // sorts BEFORE `b_dim` and the 200-row table is offered first.
      //
      // Multiple grains still pay off for the case they exist for — grains that
      // cover DIFFERENT queries — and docs/preaggregation.md says so without
      // promising the smallest covering table. If this test starts failing
      // because member order became size-aware, that is an improvement: update
      // the doc's "Which rollup answers a query" note along with it.
      const { load, manifest, tables } = await synthesizeAndBuild(
         `  dimension: a_dim is concat('a', amount::string)
  dimension: b_dim is pick 'even' when amount % 2 = 0 else 'odd'
  #@ preaggregate grain="b_dim"
  #@ preaggregate grain="a_dim, b_dim"
  measure: total is amount.sum()`,
      );
      expect(tables).toHaveLength(2);
      const combined = tables.find((t) => t.includes("a_dim_b_dim"));
      // By exclusion: the narrow grain's own slug is a SUFFIX of the combined
      // one's, so matching it by name would match both.
      const narrow = tables.find((t) => t !== combined);
      expect(combined).toBeDefined();
      expect(narrow).toBeDefined();

      const sql = await load("synth.malloy")
         .loadQuery("run: orders -> { group_by: b_dim; aggregate: total }")
         .getSQL({ buildManifest: manifest });
      expect(sql).toContain(combined as string);
      expect(sql).not.toContain(narrow as string);
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
