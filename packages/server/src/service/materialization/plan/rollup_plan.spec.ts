// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { installSettings } from "../../../test_helpers/settings_env";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { persistBlankedFiles } from "../text/persist_blanking";
import {
   planModelRollups,
   rollupName,
   rollupGateEnforcedAtRead,
   type RollupRefusal,
} from "./rollup_plan";
import {
   emitMeasureExpr,
   generateRollupBuildFile,
   rollupBuildCopyText,
} from "../text/read_text";
import type { RollupVariantPlan } from "../domain/table_plans";
import { SourceMap } from "../text/source_map";

const mz = materializationFixture();

const ROWS = `duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 10, 'A', 'x', 'AA', TIMESTAMP '2024-01-01 08:00:00'),
    (2, 20, 'A', 'y', 'BB', TIMESTAMP '2024-01-02 09:00:00'),
    (3, 30, 'B', 'x', 'AA', TIMESTAMP '2024-02-01 10:00:00')
  ) AS t(id, amount, category, region, carrier, order_time)
""")`;

const CARRIERS = `duckdb.sql("SELECT 'AA' AS code, 5 AS fleet UNION ALL SELECT 'BB', 7")`;

async function plan(
   files: Record<string, string>,
   declaredIn?: string,
   readUnderGates?: boolean,
): Promise<{ plans: RollupVariantPlan[]; refused: RollupRefusal[] }> {
   const modelDef = await mz.compile(files);
   return planModelRollups(
      SourceMap.of(modelDef),
      {
         packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
         ...(readUnderGates !== undefined ? { readUnderGates } : {}),
      },
      declaredIn ? (url) => url === `file:///pkg/${declaredIn}` : undefined,
   );
}

/** A refusal's reason, or the code of one that refuses nothing (an invalid declaration). */
function why(r: { code: string; refused?: { reason: string } }): string {
   return r.refused?.reason ?? r.code;
}

describe("#(rollup) declarations (A1)", () => {
   it("groups measures by grain, sorted coarsest first, with a model-scoped name", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  dimension: order_day is order_time.day
  #(rollup) grain=["category"]
  #(rollup) grain=["category", "order_day"]
  measure: total is amount.sum()
  #(rollup) grain=["category"]
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
  #(rollup) grain=["category"]
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
    #(rollup) grain=["region"]
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
  #(rollup) grain=["category"]
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

   it("reports a malformed declaration, which fails the load", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  dimension: big is amount > 15
  #(rollup) grain="category"
  measure: a is amount.sum()
  #(rollup) grain=["category"] grane=["x"]
  measure: b is amount.sum()
  #(rollup) grain=["nope"]
  measure: c is amount.sum()
  #(rollup) grain=["category"]
  measure: d is amount.avg()
  #(rollup) grain=["category"] storage="lake" namespace="x"
  measure: e is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.measure, r.tier, why(r)])).toEqual([
         ["big", "malformed", "rollup-not-a-measure"],
         ["a", "malformed", "rollup-invalid"],
         ["b", "malformed", "rollup-unknown-key"],
         ["e", "malformed", "rollup-invalid"],
         ["c", "malformed", "rollup-unknown-grain"],
         ["d", "malformed", "rollup-not-mergeable"],
      ]);
   });
});

describe("#(rollup) joins (A2, A6)", () => {
   const MODEL = `source: carriers is ${CARRIERS} extend { primary_key: code }
source: lines is duckdb.sql("SELECT 1 AS order_id, 'p' AS kind UNION ALL SELECT 1, 'q'")
source: orders is ${ROWS} extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: lines on lines.order_id = id
  dimension: line_kind is lines.kind
  #(rollup) grain=["carrier"]
  #(rollup) grain=["category"]
  measure: fleet is carriers.fleet.sum()
  #(rollup) grain=["category", "line_kind"]
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
      expect(refused.map((r) => [r.measure, r.tier, why(r)])).toEqual([
         ["fleet", "unservable", "join_not_in_grain"],
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
  #(rollup) grain=["carrier", "category"]
  measure: fleet is carriers.fleet.sum()
  #(rollup) grain=["carrier", "category"]
  measure: carrier_count is carriers.count()
  #(rollup) grain=["carrier", "category"]
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
  #(rollup) grain=["carrier"]
  measure: gates is carriers.hubs.gates.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.measure, why(r)])).toEqual([
         ["gates", "join_not_in_grain"],
      ]);
   });
});

const GATED = {
   "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}`,
};

describe("#(rollup) access filters (A4)", () => {
   it("warns and leaves out a rollup whose grain drops the gate's field", async () => {
      // As planned once a gated entry point can read a rollup (row 1).
      const { plans, refused } = await plan(GATED, undefined, true);
      expect(plans.map((p) => p.grainDimensions)).toEqual([
         ["category", "region"],
      ]);
      expect(refused.map((r) => [r.tier, why(r)])).toEqual([
         ["unservable", "access_filter"],
      ]);
   });
});

describe("#(rollup) over a gated source with gated sources off (G3 for rollups)", () => {
   it("plans a database rollup over the base's own gate when its grain keeps the gate's field, and no other", async () => {
      expect(rollupGateEnforcedAtRead({ variant: true, baseGated: true })).toBe(
         false,
      );
      const { plans, refused } = await plan(GATED);
      expect(plans.map((p) => [p.grainDimensions, p.baseGateOwn])).toEqual([
         [["category", "region"], true],
      ]);
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(true);
      // The grain that drops the gate's field.
      expect(refused.map((r) => [r.tier, why(r)])).toEqual([
         ["unservable", "access_filter"],
      ]);
   });

   it("plans one under the base's own #(authorize) gate that reads no field", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: ROLE :: string is 'viewer'
#(authorize) 'admin' = $ROLE
source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.baseGateOwn)).toEqual([true]);
      expect(refused).toEqual([]);
   });

   it("plans one over a lock it inherits that reads no row field, on either tier, and marks it lock-only (g-inh-lock)", async () => {
      for (const storage of ["", ' storage="lake"']) {
         const { plans, refused } = await plan({
            "orders.malloy": `##! experimental.givens
given: ROLE :: string is 'viewer'
given: REGION :: string is 'x'
#(authorize) 'admin' = $ROLE
source: locked is ${ROWS}
source: mid is locked extend { dimension: c2 is lower(category) }
source: orders is mid extend {
  #(rollup) grain=["category"]${storage}
  measure: total is amount.sum()
}
#(access_filter) region = $REGION
source: filtered is locked extend {
  #(rollup) grain=["category", "region"]${storage}
  measure: total is amount.sum()
}
#(authorize) false
source: shut is ${ROWS}
#(authorize) true
source: reopened is shut extend {
  #(rollup) grain=["category"]${storage}
  measure: total is amount.sum()
}`,
         });
         expect(
            plans.map((p) => [
               p.baseSourceName,
               p.baseGateOwn,
               p.baseGateLocksOnly,
            ]),
         ).toEqual([
            ["orders", undefined, true],
            ["reopened", true, true],
         ]);
         for (const p of plans) expect(rollupGateEnforcedAtRead(p)).toBe(true);
         // An inherited lock beside a row filter is not lock-only.
         expect(refused.map((r) => [r.sourceName, why(r)])).toEqual([
            ["filtered", "access_filter"],
         ]);
      }
   });

   it("plans none over an extension that inherits the gate, and still plans the ungated", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: gated is ${ROWS}
source: orders is gated extend {
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}
source: open is ${ROWS} extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.baseSourceName)).toEqual(["open"]);
      expect(refused.map((r) => why(r))).toEqual(["access_filter"]);
      expect(refused[0]?.message).toContain(
         "PUBLISHER_MATERIALIZE_GATED_SOURCES",
      );
   });

   it("plans none over a gate a query-shaped base derives from, beside a gate of its own", async () => {
      // Its own note is declared by its own statement, but the walk also
      // reads the derivation base's filter (not the base's own): G3 keeps
      // only a base's own gates.
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
given: CAT :: string is 'A'
#(access_filter) region = $REGION
source: gated is ${ROWS}
#(access_filter) category = $CAT
source: orders is gated -> { select: * } extend {
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => why(r))).toEqual(["access_filter"]);
      // The same with the switch's test seam proves the gates themselves
      // are exact on the grain: only their ownership refuses it.
      expect(
         (
            await plan(
               {
                  "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
given: CAT :: string is 'A'
#(access_filter) region = $REGION
source: gated is ${ROWS}
#(access_filter) category = $CAT
source: orders is gated -> { select: * } extend {
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}`,
               },
               undefined,
               true,
            )
         ).plans.map((p) => p.baseGateOwn),
      ).toEqual([undefined]);
   });

   it("plans one in a storage destination over the base's own gate (P3/P4)", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: orders is ${ROWS} extend {
  #(rollup) grain=["category", "region"] storage="lake"
  measure: total is amount.sum()
}`,
      });
      expect(refused).toEqual([]);
      expect(plans.map((p) => [p.storage, p.baseGateOwn])).toEqual([
         ["lake", true],
      ]);
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(true);
   });
});

describe("a storage= rollup whose grain no stored read declares", () => {
   const EXOTIC = `duckdb.sql("""
  SELECT 1 AS id, 10 AS amount, 'ab'::BLOB AS b, INTERVAL 2 DAY AS iv,
    MAP {'k': 1} AS m, '{"a": 1}'::JSON AS j, uuid() AS u
""")`;
   const rollup = (grain: string, storage: string) => ({
      "orders.malloy": `source: orders is ${EXOTIC} extend {
  #(rollup) grain=["${grain}"]${storage}
  measure: total is amount.sum()
}`,
   });

   it("refuses a BLOB, INTERVAL, MAP or JSON grain at plan, as unreadable_grain", async () => {
      for (const grain of ["b", "iv", "m", "j"]) {
         const { plans, refused } = await plan(
            rollup(grain, ' storage="lake"'),
         );
         expect(plans).toEqual([]);
         expect(refused.map((r) => why(r))).toEqual(["unreadable_grain"]);
         expect(refused[0]?.message).toContain(`\`${grain}\``);
      }
   });

   it("refuses a dimension that reads such a column, which keeps its type", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: orders is ${EXOTIC} extend {
  dimension: bb is b
  #(rollup) grain=["bb"] storage="lake"
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => why(r))).toEqual(["unreadable_grain"]);
   });

   it("plans a UUID grain in storage, and every one of them database", async () => {
      expect((await plan(rollup("u", ' storage="lake"'))).plans).toHaveLength(
         1,
      );
      for (const grain of ["b", "iv", "m", "j", "u"]) {
         const { plans, refused } = await plan(rollup(grain, ""));
         expect(refused).toEqual([]);
         expect(plans).toHaveLength(1);
      }
   });
});

describe("#(rollup) access filters through a join (A4, fail closed)", () => {
   it("leaves out every rollup when the gate reads through a join, which no grain keeps", async () => {
      const { plans, refused } = await plan(
         {
            "orders.malloy": `##! experimental.givens
given: FLEET :: number is 5
source: carriers is ${CARRIERS} extend { primary_key: code }
#(access_filter) carriers.fleet = $FLEET
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  #(rollup) grain=["category", "carrier"]
  measure: total is amount.sum()
}`,
         },
         undefined,
         true,
      );
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.tier, why(r)])).toEqual([
         ["unservable", "access_filter"],
      ]);
   });
});

describe("#(rollup) join-level min and max (row 23)", () => {
   const MODEL = (grain: string) => ({
      "orders.malloy": `source: carriers is ${CARRIERS} extend { primary_key: code }
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  #(rollup) grain=${grain}
  measure: top_fleet is carriers.fleet.max()
  #(rollup) grain=${grain}
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
      expect(refused.map((r) => why(r))).toEqual([
         "join_not_in_grain",
         "join_not_in_grain",
      ]);
   });
});

describe("#(rollup) measures checked against the compiled expression", () => {
   it("stores a measure whose text and compiled form agree", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: carriers is ${CARRIERS} extend { primary_key: code }
source: orders is ${ROWS} extend {
  join_one: carriers with carrier
  measure: order_count is count()
  #(rollup) grain=["carrier"]
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
  #(rollup) grain=["carrier"]
  measure: joined is carriers.fleet_total
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.tier, why(r)])).toEqual([
         ["unservable", "unreadable_measure"],
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

describe("#(rollup) and the current time (S25b)", () => {
   it("leaves out a rollup whose rows, grain or measure read now", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: recent is ${ROWS} extend {
  where: order_time > now - 7 days
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}
source: orders is ${ROWS} extend {
  dimension: is_recent is order_time > now - 7 days
  #(rollup) grain=["category", "is_recent"]
  measure: total is amount.sum()
  #(rollup) grain=["category"]
  measure: recent_total is amount.sum() { where: order_time > now - 7 days }
  #(rollup) grain=["category"]
  measure: all_total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.rollupMeasures.map((m) => m.name))).toEqual([
         ["all_total"],
      ]);
      expect(refused.map((r) => [r.sourceName, r.measure, why(r)])).toEqual([
         ["orders", "total", "current_time"],
         ["orders", "recent_total", "current_time"],
         ["recent", "total", "current_time"],
      ]);
   });
});

describe("a source that sets its own time zone (#1686)", () => {
   it("plans its rollup only where the join branch reads it: joined from a source in the same zone, stored beside a storage= rollup", async () => {
      const zoned = (
         name: string,
         storage = "",
      ) => `source: ${name} is ${ROWS} extend {
  timezone: 'America/New_York'
  primary_key: id
  dimension: order_day is order_time.day
  #(rollup) grain=["id", "order_day"]${storage}
  measure: total is amount.sum()
}`;
      const { plans, refused } = await plan({
         "m.malloy": `${zoned("lonely")}
${zoned("joined")}
${zoned("joined_stored", ' storage="lake"')}
${zoned("joined_lake", ' storage="lake"')}
${zoned("joined_persisted", ' storage="lake"')}
${zoned("joined_other_lake", ' storage="lake"')}
${zoned("joined_from_pond", ' storage="lake"')}
${zoned("joined_elsewhere")}
source: inherits is lonely extend {
  #(rollup) grain=["category"]
  measure: total2 is amount.sum()
}
source: plain is ${ROWS} extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}
source: parent is ${ROWS} extend {
  timezone: 'America/New_York'
  join_one: j is joined on j.id = id
  join_one: js is joined_stored on js.id = id
}
source: parent_utc is ${ROWS} extend {
  join_one: je is joined_elsewhere on je.id = id
}
#(materialize) storage="lake"
source: parent_lake is ${ROWS} extend {
  timezone: 'America/New_York'
  join_one: jl is joined_lake on jl.id = id
}
#@ persist storage="lake"
source: parent_persisted is ${ROWS} -> { select: * } extend {
  timezone: 'America/New_York'
  join_one: jp is joined_persisted on jp.id = id
}
source: parent_lake_extended is parent_lake extend {
  join_one: jo is joined_other_lake on jo.id = id
}
#(materialize) storage="pond"
source: parent_pond is ${ROWS} extend {
  timezone: 'America/New_York'
  join_one: jf is joined_from_pond on jf.id = id
}`,
      });
      // A storage= rollup is planned only when a source in the same zone
      // that joins it is stored in its destination, or is built on one that
      // is (#1704): the join leg reads it from no other source, so
      // `joined_stored`, joined from a source on its own connection, and
      // `joined_from_pond`, joined from one stored in another destination,
      // do not make one; `parent_lake_extended`, built on a stored source,
      // does.
      expect(plans.map((p) => p.baseSourceID.split("@")[0]).sort()).toEqual([
         "joined",
         "joined_lake",
         "joined_other_lake",
         "joined_persisted",
         "plain",
      ]);
      expect(
         refused
            .filter((r) => why(r) === "time_zone")
            .map((r) => [r.sourceName, r.measure])
            .sort(),
      ).toEqual([
         ["inherits", "total2"],
         ["joined_elsewhere", "total"],
         ["joined_from_pond", "total"],
         ["joined_stored", "total"],
         ["lonely", "total"],
      ]);
      expect(
         refused.find((r) => r.sourceName === "joined_stored")?.message,
      ).toContain("is stored in `lake`");
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
  #(rollup) grain=["category"] storage="lake"
  measure: paid is amount.sum() { where: region = 'x' }
  #(rollup) grain=["category"] storage="lake"
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
      await mz.compile({
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
      const modelDef = await mz.compile({ "m.malloy": text });
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
      const modelDef = await mz.compile({ "m.malloy": text });
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

describe("#(rollup) over a gated source with gated sources on (row 1)", () => {
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
  #(rollup) grain=["category", "region"]${placement}
  measure: total is amount.sum()
}`,
   });

   it("plans a database rollup whose grain keeps the gate's field, read at its gate", async () => {
      const { plans, refused } = await plan(placed(""));
      expect(refused).toEqual([]);
      expect(plans).toHaveLength(1);
      expect(plans[0]?.baseGated).toBe(true);
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(true);
   });

   it("plans a storage= rollup over a gated base on the database terms (P3/P4)", async () => {
      const { plans, refused } = await plan(placed(' storage="lake"'));
      expect(refused).toEqual([]);
      expect(plans).toHaveLength(1);
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(true);
   });

   it("never reads a v1 plan or an ungated rollup past a gate", async () => {
      expect(rollupGateEnforcedAtRead({ baseGated: true })).toBe(false);
      const { plans } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(rollupGateEnforcedAtRead(plans[0])).toBe(false);
   });

   it("stores an internal gate field in the grain, and a private one its source declares, each redeclared hidden (A4)", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental { givens access_modifiers }
given: TENANT :: string is 'x'
#(access_filter) tenant = $TENANT
source: orders is ${ROWS} extend {
  internal dimension: tenant is region
  private dimension: secret is carrier
  #(rollup) grain=["category", "tenant"]
  #(rollup) grain=["category", "tenant", "secret"]
  measure: total is amount.sum()
}`,
      });
      expect(
         plans.map((p) => [p.grainDimensions, p.internalGrain, p.privateGrain]),
      ).toEqual([
         [["category", "tenant"], ["tenant"], undefined],
         [["category", "secret", "tenant"], ["tenant"], ["secret"]],
      ]);
      expect(refused).toEqual([]);
      const text = generateRollupBuildFile(plans, "orders.malloy") ?? "";
      expect(text).toContain("include { public: * } -> {");
      // The private one is read from the base's build copy, which lifts it.
      expect(text).toContain('from "orders.malloy.orders.rollup-build.malloy"');
   });

   it("stores a private grain dimension a source it extends declares in its file, lifting each statement down to it (b-hidden)", async () => {
      const files = {
         "orders.malloy": `##! experimental { access_modifiers }
source: base is ${ROWS} extend {
  private dimension: hidden_cat is category
}
source: middle is base extend {
  dimension: doubled is amount * 2
}
source: orders is middle extend {
  #(rollup) grain=["hidden_cat", "region"]
  measure: total is amount.sum()
}`,
      };
      const { plans, refused } = await plan(files);
      expect(refused).toEqual([]);
      expect(plans.map((p) => p.privateGrain)).toEqual([["hidden_cat"]]);
      // `middle` and `base`, in file order.
      expect(plans[0]?.liftedAncestors?.map((a) => a.range.start.line)).toEqual(
         [1, 10],
      );
      const copy = rollupBuildCopyText(
         files["orders.malloy"],
         plans[0] as RollupVariantPlan,
         true,
      );
      expect("copy" in copy && copy.copy).toContain(
         "  dimension: hidden_cat is category",
      );
      expect("copy" in copy && copy.copy).not.toContain("private dimension");
      const text = generateRollupBuildFile(plans, "orders.malloy") ?? "";
      expect(text).toContain('from "orders.malloy.orders.rollup-build.malloy"');
   });

   it("lifts only the base's own statement for a private dimension it declares itself", async () => {
      const { plans } = await plan({
         "orders.malloy": `##! experimental { access_modifiers }
source: orders is ${ROWS} extend {
  private dimension: secret is carrier
  #(rollup) grain=["category", "secret"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.liftedAncestors)).toEqual([undefined]);
   });

   it("refuses a private grain dimension a source in another file declares: the build copy cannot lift it (b-hidden)", async () => {
      const { plans, refused } = await plan(
         {
            "orders.malloy": `##! experimental { access_modifiers }
import "base.malloy"
source: orders is base extend {
  #(rollup) grain=["hidden_cat", "region"]
  measure: total is amount.sum()
}`,
            "base.malloy": `##! experimental { access_modifiers }
source: base is ${ROWS} extend {
  private dimension: hidden_cat is category
}`,
         },
         "orders.malloy",
      );
      expect(plans).toEqual([]);
      expect(refused.map((r) => why(r))).toEqual(["hidden_field"]);
      expect(refused[0]?.message).toContain("another file");
   });

   it("never plans a rollup of a hidden measure, private or internal (b-hidden)", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental { access_modifiers }
source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  private measure: secret_total is amount.sum()
  #(rollup) grain=["category"]
  internal measure: internal_total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => why(r))).toEqual([
         "hidden_field",
         "hidden_field",
      ]);
   });

   it("walks an inherited gate as the governor does, on an extension's own rollup", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `##! experimental.givens
given: REGION :: string is 'x'
#(access_filter) region = $REGION
source: gated is ${ROWS}
source: orders is gated extend {
  #(rollup) grain=["category", "region"]
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(plans.map((p) => p.grainDimensions)).toEqual([
         ["category", "region"],
      ]);
      expect(refused.map((r) => why(r))).toEqual(["access_filter"]);
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
  #(rollup) grain=["category", "fleet"]
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => why(r))).toEqual(["access_filter"]);
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
  #(rollup) grain=["category", "region"]
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
  #(rollup) grain=["category"]
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

   async function planV1(readsPreaggregateLines: boolean) {
      const modelDef = await mz.compile({ "orders.malloy": V1 });
      return planModelRollups(SourceMap.of(modelDef), {
         packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
         readsPreaggregateLines,
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

describe("a v1 rollup's container under the v1 cutover", () => {
   it("inherits its base's #@ persist name= container when its line names none, as v1 places it", async () => {
      const modelDef = await mz.compile({
         "orders.malloy": `##! experimental.persistence
#@ persist name="analytics.orders_tbl"
source: orders is ${ROWS} -> {
  select: category, region, amount
} extend {
  measure:
    #@ preaggregate grain="region" name="rollups.regional"
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
         readsPreaggregateLines: true,
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

describe("#(preaggregate), #(rollup)'s deprecated spelling", () => {
   const model = (route: string) => `source: orders is ${ROWS} extend {
  ${route} grain=["category"] storage="lake"
  measure: total is amount.sum()
  ${route} grain=["category"]
  measure: order_count is count()
}`;

   it("plans the same rollups as #(rollup), and warns once per measure naming the spelling it found", async () => {
      const current = await plan({ "orders.malloy": model("#(rollup)") });
      const alias = await plan({ "orders.malloy": model("#(preaggregate)") });
      expect(current.refused).toEqual([]);
      expect(alias.plans).toEqual(current.plans);
      expect(alias.refused.map((r) => [r.tier, r.code, r.measure])).toEqual([
         ["deprecated", "deprecated-preaggregate-annotation", "total"],
         ["deprecated", "deprecated-preaggregate-annotation", "order_count"],
      ]);
      for (const r of alias.refused) {
         expect(r.message).toContain("#(preaggregate)");
         expect(r.message).toContain("Write #(rollup)");
      }
   });

   it("refuses both spellings on one measure, failing the load (rollup-invalid)", async () => {
      const { plans, refused } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  #(preaggregate) grain=["region"]
  measure: total is amount.sum()
}`,
      });
      expect(plans).toEqual([]);
      expect(refused.map((r) => [r.tier, r.code, r.measure])).toEqual([
         ["malformed", "rollup-invalid", "total"],
      ]);
      expect(refused[0]?.message).toContain(
         "both #(rollup) and #(preaggregate)",
      );
   });

   it("refuses both spellings on one source, failing the load (rollup-invalid)", async () => {
      const { refused } = await plan({
         "orders.malloy": `#(rollup) grain=["category"]
#(preaggregate) grain=["category"]
source: orders is ${ROWS} extend {
  measure: total is amount.sum()
}`,
      });
      expect(refused.map((r) => [r.tier, r.code])).toContainEqual([
         "malformed",
         "rollup-invalid",
      ]);
   });

   it("names its malformed lines with #(rollup)'s codes", async () => {
      const { refused } = await plan({
         "orders.malloy": `source: orders is ${ROWS} extend {
  #(preaggregate) grian=["category"]
  measure: total is amount.sum()
}`,
      });
      expect(refused.map((r) => r.code)).toContain("rollup-unknown-key");
   });
});
