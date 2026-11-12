// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { InMemoryURLReader, SingleConnectionRuntime } from "@malloydata/malloy";
import { persistBlankedFiles } from "./rollup_compile";
import {
   generateRollupBuildFile,
   planModelRollups,
   rollupName,
   type RollupRefusal,
   type RollupVariantPlan,
} from "./rollup_plan";
import { SourceMap } from "./source_map";

const connection = new DuckDBConnection("duckdb", ":memory:");
afterAll(async () => {
   await connection.close();
});

const ROWS = `duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 10, 'A', 'x', 'AA', TIMESTAMP '2024-01-01 08:00:00'),
    (2, 20, 'A', 'y', 'BB', TIMESTAMP '2024-01-02 09:00:00'),
    (3, 30, 'B', 'x', 'AA', TIMESTAMP '2024-02-01 10:00:00')
  ) AS t(id, amount, category, region, carrier, order_time)
""")`;

const CARRIERS = `duckdb.sql("SELECT 'AA' AS code, 5 AS fleet UNION ALL SELECT 'BB', 7")`;

/** Compile `files` and return the first file's model definition. */
async function compile(files: Record<string, string>): Promise<unknown> {
   const urls = new Map(
      Object.entries(files).map(([name, text]) => [
         `file:///pkg/${name}`,
         text,
      ]),
   );
   const runtime = new SingleConnectionRuntime({
      urlReader: new InMemoryURLReader(urls),
      connection,
   });
   const [first] = Object.keys(files);
   const model = await runtime
      .loadModel(new URL(`file:///pkg/${first}`))
      .getModel();
   return model._modelDef;
}

async function plan(
   files: Record<string, string>,
   declaredIn?: string,
): Promise<{ plans: RollupVariantPlan[]; refused: RollupRefusal[] }> {
   const modelDef = await compile(files);
   return planModelRollups(
      SourceMap.of(modelDef),
      {
         packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
      },
      declaredIn ? (url) => url === `file:///pkg/${declaredIn}` : undefined,
   );
}

describe("#(preaggregate) declarations (A1)", () => {
   it("groups measures by grain, sorted coarsest first, with a model-scoped name", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  dimension: order_day is order_time.day
  #(preaggregate) grain=["category"]
  #(preaggregate) grain=["category", "order_day"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]
  measure: order_count is count()
}`,
      });
      expect(refused).toEqual([]);
      expect(
         plans.map((p) => [p.grainDimensions, p.measures.map((m) => m.name)]),
      ).toEqual([
         [["category"], ["order_count", "total"]],
         [["category", "order_day"], ["total"]],
      ]);
      expect(plans[0]?.rollupSourceName).toBe(
         rollupName("orders", "orders@orders.malloy", ["category"], {}),
      );
      // The same grain on a same-named source in another model is another table.
      expect(
         rollupName("orders", "orders@other.malloy", ["category"], {}),
      ).not.toBe(plans[0]?.rollupSourceName);
   });

   it("attributes an inherited declaration to the source that wrote it", async () => {
      const { plans } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
source: more_orders is orders extend { dimension: big is amount > 15 }
source: renamed is orders`,
      });
      expect(plans.map((p) => p.baseSourceName)).toEqual(["orders"]);
   });

   it("gives an include-list item's declaration to the including source only", async () => {
      const { plans } = await plan({
         "orders.malloy": `##! experimental.access_modifiers
source: orders is ${ROWS} extend {
  measure: total is amount.sum()
}
source: ops is orders include {
  public:
    #(preaggregate) grain=["region"]
    total
  public: *
}`,
      });
      expect(plans.map((p) => [p.baseSourceName, p.grainDimensions])).toEqual([
         ["ops", ["region"]],
      ]);
   });

   it("plans a source only in the model that declares it", async () => {
      const files = {
         "index.malloy": `import "orders.malloy"\nsource: alias is orders`,
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`,
      };
      expect((await plan(files, "index.malloy")).plans).toEqual([]);
      expect(
         (await plan(files, "orders.malloy")).plans.map(
            (p) => p.baseSourceName,
         ),
      ).toEqual(["orders"]);
   });

   it("refuses a malformed declaration at publish", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  dimension: big is amount > 15
  #(preaggregate) grain="category"
  measure: a is amount.sum()
  #(preaggregate) grain=["category"] grane=["x"]
  measure: b is amount.sum()
  #(preaggregate) grain=["nope"]
  measure: c is amount.sum()
  #(preaggregate) grain=["category"]
  measure: d is amount.avg()
  #(preaggregate) grain=["category"] storage="lake" namespace="x"
  measure: e is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.measure, r.tier, r.code])).toEqual([
         ["big", "malformed", "preaggregate-not-a-measure"],
         ["a", "malformed", "preaggregate-malformed"],
         ["b", "malformed", "preaggregate-unknown-key"],
         ["e", "malformed", "preaggregate-malformed"],
         ["c", "malformed", "preaggregate-unknown-grain"],
         ["d", "malformed", "preaggregate-not-mergeable"],
      ]);
   });
});

describe("#(preaggregate) joins (A2, A6)", () => {
   const MODEL = `source: carriers is ${CARRIERS} extend { primary_key: code }
source: lines is duckdb.sql("SELECT 1 AS order_id, 'p' AS kind UNION ALL SELECT 1, 'q'")
source: orders is ${ROWS} extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: lines on lines.order_id = id
  dimension: line_kind is lines.kind
  #(preaggregate) grain=["carrier"]
  #(preaggregate) grain=["category"]
  measure: fleet is carriers.fleet.sum()
  #(preaggregate) grain=["category", "line_kind"]
  measure: total is amount.sum()
}`;

   it("stores a join-level aggregate only when the join's key is in the grain", async () => {
      const { plans, refused } = await plan({ "orders.malloy": MODEL });
      expect(
         plans.map((p) => [p.grainDimensions, p.measures.map((m) => m.name)]),
      ).toEqual([
         [["carrier"], ["fleet"]],
         [["category", "line_kind"], ["total"]],
      ]);
      expect(refused.map((r) => [r.measure, r.tier, r.code])).toEqual([
         ["fleet", "unservable", "preaggregate-join-level"],
      ]);
   });

   it("marks a grain dimension read through a join_many as fan-out", async () => {
      const { plans } = await plan({ "orders.malloy": MODEL });
      expect(plans.map((p) => p.fanOutDimensions)).toEqual([[], ["line_kind"]]);
   });
});

describe("#(preaggregate) access filters (A4)", () => {
   it("warns and leaves out a rollup whose grain drops the gate's field", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  #(preaggregate) grain=["category", "region"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.grainDimensions)).toEqual([
         ["category", "region"],
      ]);
      expect(refused.map((r) => [r.tier, r.code])).toEqual([
         ["unservable", "preaggregate-access-filter"],
      ]);
   });
});

describe("the rollup build file", () => {
   it("declares one persisted rollup per grain over the imported base", async () => {
      const { plans } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"] storage="lake"
  measure: paid is amount.sum() { where: region = 'x' }
  #(preaggregate) grain=["category"] storage="lake"
  measure: per_order is amount.sum() / count()
}`,
      });
      const text = generateRollupBuildFile(plans, "orders.malloy") ?? "";
      expect(text).toContain(
         'import { orders__rollup_base is orders } from "orders.malloy"',
      );
      expect(text).toContain('#@ persist storage="lake"');
      expect(text).toContain(
         "    paid__partial is amount.sum() { where: region = 'x' }",
      );
      expect(text).toContain("    per_order__partial0 is amount.sum()");
      expect(text).toContain("    per_order__partial1 is count()");
      // It compiles beside the author's file.
      await compile({
         "orders.malloy.preaggregate.malloy": text,
         "orders.malloy": `source: orders is ${ROWS} extend {
  measure: paid is amount.sum() { where: region = 'x' }
}`,
      });
   });

   it("blanks every source-level #@ line, keeping positions", async () => {
      const text = `##! experimental.persistence
#@ persist storage="lake"
source: daily is ${ROWS} -> { group_by: category; aggregate: n is count() }
`;
      const modelDef = await compile({ "m.malloy": text });
      const blanked = persistBlankedFiles(modelDef, () => text);
      const out = [...blanked.values()][0] ?? "";
      expect(out.split("\n")[1]?.trim()).toBe("");
      expect(out.split("\n").length).toBe(text.split("\n").length);
      expect(out).toContain("source: daily is");
   });
});
