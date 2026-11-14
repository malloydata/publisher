// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { installSettings } from "../../../test_helpers/settings_env";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { InMemoryURLReader, SingleConnectionRuntime } from "@malloydata/malloy";
import { persistBlankedFiles } from "../text/persist_blanking";
import {
   generateRollupBuildFile,
   planModelRollups,
   rollupName,
   allowGatedStorageRollupsForTests,
   rollupGateEnforcedAtRead,
   rollupsServeGatedBases,
   type RollupRefusal,
   type RollupVariantPlan,
} from "./rollup_plan";
import { emitMeasureExpr } from "./rollup_measure_grammar";
import { SourceMap } from "../text/source_map";

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
   readUnderGates?: boolean,
): Promise<{ plans: RollupVariantPlan[]; refused: RollupRefusal[] }> {
   const modelDef = await compile(files);
   return planModelRollups(
      SourceMap.of(modelDef),
      {
         packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
         ...(readUnderGates !== undefined ? { readUnderGates } : {}),
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

   it("marks every grain dimension but the key as fan-out for a join-level sum or count", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: carriers is ${CARRIERS} extend { primary_key: code }
source: orders is ${ROWS} extend {
  primary_key: id
  join_one: carriers with carrier
  #(preaggregate) grain=["carrier", "category"]
  measure: fleet is carriers.fleet.sum()
  #(preaggregate) grain=["carrier", "category"]
  measure: carrier_count is carriers.count()
  #(preaggregate) grain=["carrier", "category"]
  measure: total is amount.sum()
}`,
      });
      expect(refused).toEqual([]);
      expect(
         plans[0]?.rollupMeasures.map((m) => [m.name, m.fanOutDimensions]),
      ).toEqual([
         ["carrier_count", ["category"]],
         ["fleet", ["category"]],
         ["total", []],
      ]);
   });

   it("leaves out a join-level sum taken through more than one join", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: hubs is duckdb.sql("SELECT 'h' AS hub, 3 AS gates") extend { primary_key: hub }
source: carriers is duckdb.sql("SELECT 'AA' AS code, 'h' AS hub UNION ALL SELECT 'BB', 'h'") extend {
  primary_key: code
  join_one: hubs with hub
}
source: orders is ${ROWS} extend {
  primary_key: id
  join_one: carriers with carrier
  #(preaggregate) grain=["carrier"]
  measure: gates is carriers.hubs.gates.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.measure, r.code])).toEqual([
         ["gates", "preaggregate-join-level"],
      ]);
   });
});

const GATED = {
   "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  #(preaggregate) grain=["category", "region"]
  measure: total is amount.sum()
}`,
};

describe("#(preaggregate) access filters (A4)", () => {
   it("warns and leaves out a rollup whose grain drops the gate's field", async () => {
      // As planned once a gated entry point can read a rollup (row 1).
      const { plans, refused } = await plan(GATED, undefined, true);
      expect(plans.map((p) => p.grainDimensions)).toEqual([
         ["category", "region"],
      ]);
      expect(refused.map((r) => [r.tier, r.code])).toEqual([
         ["unservable", "preaggregate-access-filter"],
      ]);
   });
});

describe("#(preaggregate) over a gated source with gated sources off (row 21)", () => {
   it("plans no rollup over a gated source, even one whose grain keeps the gate's field", async () => {
      expect(rollupsServeGatedBases({})).toBe(false);
      const { plans, refused } = await plan(GATED);
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.tier, r.code])).toEqual([
         ["unservable", "preaggregate-access-filter"],
         ["unservable", "preaggregate-access-filter"],
      ]);
      expect(refused[0]?.message).toContain(
         "PUBLISHER_MATERIALIZE_GATED_SOURCES",
      );
   });

   it("plans none under an #(authorize) gate that reads no field", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: ROLE :: string is 'viewer'
#(authorize) 'admin' = $ROLE
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => r.code)).toEqual([
         "preaggregate-access-filter",
      ]);
   });

   it("plans none over an extension that inherits the gate, and still plans the ungated", async () => {
      const { plans } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: gated is ${ROWS}
source: orders is gated extend {
  #(preaggregate) grain=["category", "region"]
  measure: total is amount.sum()
}
source: open is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.baseSourceName)).toEqual(["open"]);
   });
});

describe("#(preaggregate) access filters through a join (A4, fail closed)", () => {
   it("leaves out every rollup when the gate reads through a join, which no grain keeps", async () => {
      const { plans, refused } = await plan(
         {
            "orders.malloy": `##! experimental.givens
given: FLEET :: number is 5
source: carriers is ${CARRIERS} extend { primary_key: code }
#(access_filter) carriers.fleet = $FLEET
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  #(preaggregate) grain=["category", "carrier"]
  measure: total is amount.sum()
}`,
         },
         undefined,
         true,
      );
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.tier, r.code])).toEqual([
         ["unservable", "preaggregate-access-filter"],
      ]);
   });
});

describe("#(preaggregate) join-level min and max (row 23)", () => {
   const MODEL = (grain: string) => ({
      "orders.malloy": `source: carriers is ${CARRIERS} extend { primary_key: code }
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  #(preaggregate) grain=${grain}
  measure: top_fleet is carriers.fleet.max()
  #(preaggregate) grain=${grain}
  measure: low_fleet is carriers.fleet.min()
}`,
   });

   it("stores carriers.x.max() and .min() with the join's key in the grain, merged by max and min, with no fan-out", async () => {
      const { plans, refused } = await plan(MODEL(`["carrier", "category"]`));
      expect(refused).toEqual([]);
      expect(
         plans[0]?.rollupMeasures.map((m) => [
            m.name,
            m.partials.map((p) => emitMeasureExpr(p.leaf.expr)),
            emitMeasureExpr(m.merged),
            m.fanOutDimensions,
         ]),
      ).toEqual([
         [
            "low_fleet",
            ["carriers.fleet.min()"],
            "low_fleet__partial.min()",
            [],
         ],
         [
            "top_fleet",
            ["carriers.fleet.max()"],
            "top_fleet__partial.max()",
            [],
         ],
      ]);
   });

   it("still serves live a join-level max whose join key the grain drops", async () => {
      const { plans, refused } = await plan(MODEL(`["category"]`));
      expect(plans).toEqual([]);
      expect(refused.map((r) => r.code)).toEqual([
         "preaggregate-join-level",
         "preaggregate-join-level",
      ]);
   });
});

describe("#(preaggregate) measures checked against the compiled expression", () => {
   it("stores a measure whose text and compiled form agree", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: carriers is ${CARRIERS} extend { primary_key: code }
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  measure: order_count is count()
  #(preaggregate) grain=["carrier"]
  measure: mixed is coalesce(amount.sum() / order_count, 0) + carriers.fleet.sum()
}`,
      });
      expect(refused).toEqual([]);
      expect(plans[0]?.rollupMeasures[0]?.partials.length).toBe(3);
   });

   it("serves live (never refuses) a measure whose compiled form the text does not account for", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: carriers is ${CARRIERS} extend {
  primary_key: code
  measure: fleet_total is fleet.sum()
}
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  #(preaggregate) grain=["carrier"]
  measure: joined is carriers.fleet_total
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.tier, r.code])).toEqual([
         ["unservable", "preaggregate-unreadable-measure"],
      ]);
   });

   it("compares as multisets: a leaf the compiler does not agree with is unreadable", async () => {
      const { checkedAgainstIR } = await import("./rollup_plan");
      const { decomposeMeasure } = await import("./rollup_measure_grammar");
      const d = decomposeMeasure("m", {
         measureCode: () => "amount.sum() + count()",
      });
      expect(
         checkedAgainstIR(d, [
            { fn: "sum", structPath: [] },
            { fn: "count", structPath: [] },
         ]),
      ).toBe(d);
      for (const compiled of [
         undefined,
         [{ fn: "sum", structPath: [] }],
         [
            { fn: "sum", structPath: [] },
            { fn: "call:stddev", structPath: [] },
         ],
         [
            { fn: "sum", structPath: ["carriers"] },
            { fn: "count", structPath: [] },
         ],
      ]) {
         expect(checkedAgainstIR(d, compiled)).toMatchObject({
            kind: "unreadable",
         });
      }
   });
});

describe("#(preaggregate) and the current time (S25b)", () => {
   it("leaves out a rollup whose rows, grain or measure read now", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: recent is ${ROWS} extend {
  where: order_time > now - 7 days
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
source: orders is ${ROWS} extend {
  dimension: is_recent is order_time > now - 7 days
  #(preaggregate) grain=["category", "is_recent"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]
  measure: recent_total is amount.sum() { where: order_time > now - 7 days }
  #(preaggregate) grain=["category"]
  measure: all_total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.rollupMeasures.map((m) => m.name))).toEqual([
         ["all_total"],
      ]);
      expect(refused.map((r) => [r.sourceName, r.measure, r.code])).toEqual([
         ["orders", "total", "preaggregate-current-time"],
         ["orders", "recent_total", "preaggregate-current-time"],
         ["recent", "total", "preaggregate-current-time"],
      ]);
   });
});

describe("a source that sets its own time zone (#1686)", () => {
   it("plans its rollup only where the join leg reads it: joined from a source in the same zone", async () => {
      const zoned = (
         name: string,
         storage = "",
      ) => `source: ${name} is ${ROWS} extend {
  timezone: 'America/New_York'
  primary_key: id
  dimension: order_day is order_time.day
  #(preaggregate) grain=["id", "order_day"]${storage}
  measure: total is amount.sum()
}`;
      const { plans, refused } = await plan({
         "m.malloy": `${zoned("lonely")}
${zoned("joined")}
${zoned("joined_stored", ' storage="lake"')}
${zoned("joined_elsewhere")}
source: inherits is lonely extend {
  #(preaggregate) grain=["category"]
  measure: total2 is amount.sum()
}
source: plain is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
source: parent is ${ROWS} extend {
  timezone: 'America/New_York'
  join_one: j is joined on j.id = id
  join_one: js is joined_stored on js.id = id
}
source: parent_utc is ${ROWS} extend {
  join_one: je is joined_elsewhere on je.id = id
}`,
      });
      // A storage= rollup joined from the same zone is planned too: the join
      // leg reads it when the joining source is stored in that destination
      // (#1704), and declines at read when it is not.
      expect(plans.map((p) => p.baseSourceID.split("@")[0]).sort()).toEqual([
         "joined",
         "joined_stored",
         "plain",
      ]);
      expect(
         refused
            .filter((r) => r.code === "preaggregate-time-zone")
            .map((r) => [r.sourceName, r.measure])
            .sort(),
      ).toEqual([
         ["inherits", "total2"],
         ["joined_elsewhere", "total"],
         ["lonely", "total"],
      ]);
      const message = refused.find((r) => r.sourceName === "lonely")?.message;
      expect(message).toContain("timezone: 'America/New_York'");
      expect(refused.find((r) => r.sourceName === "lonely")?.tier).toBe(
         "unservable",
      );
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
      const blanked = await persistBlankedFiles(modelDef, () => text);
      const out = [...(blanked?.values() ?? [])][0] ?? "";
      expect(out.split("\n")[1]?.trim()).toBe("");
      expect(out.split("\n").length).toBe(text.split("\n").length);
      expect(out).toContain("source: daily is");
   });

   it("blanks nothing when a file no longer holds its #@ line where it compiled", async () => {
      const text = `##! experimental.persistence
#@ persist storage="lake"
source: daily is ${ROWS} -> { group_by: category; aggregate: n is count() }
`;
      const modelDef = await compile({ "m.malloy": text });
      // The file moved on since it compiled: the span holds something else.
      expect(
         await persistBlankedFiles(modelDef, () => `\n\n${text}`),
      ).toBeUndefined();
      // Or cannot be read at all.
      expect(
         await persistBlankedFiles(modelDef, () => undefined),
      ).toBeUndefined();
   });
});

describe("#(preaggregate) over a gated source with gated sources on (row 1)", () => {
   let restore: () => void;
   beforeEach(() => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
   });
   afterEach(() => restore());

   const placed = (placement: string) => ({
      "orders.malloy": `##! experimental { givens access_modifiers }
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category", "region"]${placement}
  measure: total is amount.sum()
}`,
   });

   it("plans a colocated rollup whose grain keeps the gate's field, read at its gate", async () => {
      const { plans, refused } = await plan(placed(""));
      expect(refused).toEqual([]);
      expect(plans).toHaveLength(1);
      expect(plans[0]?.baseGated).toBe(true);
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(true);
   });

   it("leaves out a storage= rollup over a gated base until the managed-tier review (question 14)", async () => {
      const { plans, refused } = await plan(placed(' storage="lake"'));
      expect(plans).toEqual([]);
      expect(refused.map((r) => r.code)).toEqual([
         "preaggregate-access-filter",
      ]);
      expect(refused[0]?.message).toContain("storage destination");
      const allow = allowGatedStorageRollupsForTests();
      try {
         const reviewed = await plan(placed(' storage="lake"'));
         expect(reviewed.plans).toHaveLength(1);
         expect(rollupGateEnforcedAtRead(reviewed.plans[0])).toBe(true);
      } finally {
         allow();
      }
   });

   it("never reads a v1 plan or an ungated rollup past a gate", async () => {
      expect(rollupGateEnforcedAtRead({ baseGated: true })).toBe(false);
      const { plans } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(false);
   });

   it("stores an internal gate field in the grain, and no other hidden field (A4)", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental { givens access_modifiers }
given: TENANT :: string is 'x'
#(access_filter) tenant = $TENANT
source: orders is ${ROWS} extend {
  internal dimension: tenant is region
  private dimension: secret is carrier
  #(preaggregate) grain=["category", "tenant"]
  #(preaggregate) grain=["category", "tenant", "secret"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => [p.grainDimensions, p.internalGrain])).toEqual([
         [["category", "tenant"], ["tenant"]],
      ]);
      expect(refused.map((r) => r.code)).toEqual(["preaggregate-hidden-field"]);
      const text = generateRollupBuildFile(plans, "orders.malloy") ?? "";
      expect(text).toContain("include { public: * } -> {");
   });

   it("walks an inherited gate as the governor does, on an extension's own rollup", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: gated is ${ROWS}
source: orders is gated extend {
  #(preaggregate) grain=["category", "region"]
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.grainDimensions)).toEqual([
         ["category", "region"],
      ]);
      expect(refused.map((r) => r.code)).toEqual([
         "preaggregate-access-filter",
      ]);
   });

   it("leaves out a rollup whose gate field reads through a join, even in the grain", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: FLEET :: number is 5
source: carriers is ${CARRIERS} extend { primary_key: code }
#(access_filter) fleet = $FLEET
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  dimension: fleet is carriers.fleet
  #(preaggregate) grain=["category", "fleet"]
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => r.code)).toEqual([
         "preaggregate-access-filter",
      ]);
   });

   it("leaves out a gated base's rollup when a source it joins is gated too", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
given: FLEET :: number is 5
#(access_filter) fleet = $FLEET
source: carriers is ${CARRIERS} extend { primary_key: code }
#(access_filter) region = $REGION
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  #(preaggregate) grain=["category", "region"]
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused[0]?.message).toContain("a source it joins");
   });

   it("plans a lock that reads no field at any grain", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: ROLE :: string is 'viewer'
#(authorize) 'admin' = $ROLE
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(refused).toEqual([]);
      expect(plans).toHaveLength(1);
   });
});

describe("v1 #@ preaggregate read as the rollup leg's declarations (the v1 cutover's G4)", () => {
   const V1 = `##! experimental.persistence
source: orders is ${ROWS} extend {
  dimension: order_day is order_time.day
  #@ preaggregate grain="category" storage="lake"
  #@ preaggregate grain="order_day, category"
  measure: total is amount.sum()
  #@ preaggregate grain="category"
  #@ -preaggregate
  measure: n is count()
}`;

   async function planV1(readsV1: boolean) {
      const modelDef = await compile({ "orders.malloy": V1 });
      return planModelRollups(SourceMap.of(modelDef), {
         packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
         readsV1,
      });
   }

   it("plans each v1 grain as a rollup, with its own line's placement, negation applied", async () => {
      const { plans, refused } = await planV1(true);
      expect(refused).toEqual([]);
      expect(
         plans.map((p) => [
            p.grainDimensions,
            p.storage ?? null,
            p.measures.map((m) => m.name),
         ]),
      ).toEqual([
         [["category"], "lake", ["total"]],
         [["category", "order_day"], null, ["total"]],
      ]);
      // Under the v2 rollup's name: a v1 rollup rebuilds once (decision 3).
      expect(plans[0]?.rollupSourceName).toBe(
         rollupName("orders", "orders@orders.malloy", ["category"], {
            storage: "lake",
         }),
      );
   });

   it("reads no v1 line before the cutover", async () => {
      expect((await planV1(false)).plans).toEqual([]);
   });
});

describe("a v1 rollup's namespace under the v1 cutover", () => {
   it("inherits its base's #@ persist name= namespace when its line names none, as v1 places it", async () => {
      const modelDef = await compile({
         "orders.malloy": `##! experimental.persistence
#@ persist name="analytics.orders_tbl"
source: orders is ${ROWS} -> {
  select: category, region, amount
} extend {
  measure:
    #@ preaggregate grain="region" namespace="rollups"
    regional is amount.sum()
  measure:
    #@ preaggregate grain="category"
    total is amount.sum()
  measure:
    #@ preaggregate grain="category, region" storage="lake"
    both is amount.sum()
}`,
      });
      const { plans, refused } = planModelRollups(SourceMap.of(modelDef), {
         packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
         readsV1: true,
      });
      expect(refused).toEqual([]);
      expect(
         plans
            .map((p) => [
               p.grainDimensions.join(","),
               p.namespace ?? null,
               p.storage ?? null,
            ])
            .sort(),
      ).toEqual([
         ["category", "analytics", null],
         ["category,region", null, "lake"],
         ["region", "rollups", null],
      ]);
   });
});
