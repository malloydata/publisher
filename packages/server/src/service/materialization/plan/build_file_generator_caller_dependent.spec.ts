// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { addressSQL } from "./build_plan";
import {
   compilePackageBuildPlan,
   computePackageBuildPlan,
} from "./build_plan_compile";
import {
   planModelMaterialize,
   type MaterializePlan,
} from "./build_file_generator";

/**
 * P9 on the `#(materialize)` build side: a dimension whose value depends on
 * the caller (it reads a given, the current time or the query time zone) is
 * left out of the build, so the table never holds one caller's value, and the
 * served copy computes it at read. The probe compiler decides beside the IR
 * walks; a dimension it clears of the time zone is marked `zoneFree`.
 */

const HEAD = `##! experimental { givens access_modifiers }
given:
  ORG_ID :: number is 1
source: raw is duckdb.sql("""
  select * from (values
    (1, 1, 'a', 10.5, TIMESTAMP '2026-01-01 03:00:00', DATE '2026-01-01'),
    (2, 2, 'b', 20.0, TIMESTAMP '2026-01-02 23:30:00', DATE '2026-01-02')
  ) as t(id, org_id, s, amount, ts, d)
""")
`;

const packages = tempBuildPlanPackages();
let restore: () => void;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
afterEach(async () => {
   restore?.();
   await packages.cleanup();
});

async function compiled(text: string) {
   const pkg = await packages.create(text);
   return { pkg, c: await compilePackageBuildPlan(pkg) };
}

function planOf(
   c: Awaited<ReturnType<typeof compilePackageBuildPlan>>,
   name: string,
): MaterializePlan | undefined {
   return Object.values(c.materializePlans ?? {}).find(
      (p) => p.sourceName === name,
   );
}

function notBuilt(
   c: Awaited<ReturnType<typeof compilePackageBuildPlan>>,
   source: string,
): string | undefined {
   return (c.annotationFindings ?? []).find(
      (f) => f.code === "materialize-not-built" && f.subject === source,
   )?.message;
}

const ORDERS = `${HEAD}
source: base is raw extend { dimension: base_hour is hour(ts) }
#(materialize) storage="lake"
source: orders is base extend {
  dimension: mine is org_id = $ORG_ID
  dimension: label is pick 'me' when mine else 'other'
  internal dimension: imine is org_id = $ORG_ID
  dimension: hr is hour(ts)
  dimension: ts_month is ts.month
  dimension: d_month is d.month
  dimension: age is hours(ts to now)
  dimension: big is amount > 10
  measure: n is count()
}
`;

describe("P9: caller-dependent dimensions are left out of the build", () => {
   it(
      "leaves out what reads a given, the current time or the zone, and stores the rest",
      async () => {
         const { pkg, c } = await compiled(ORDERS);
         const p = planOf(c, "orders");
         expect(p).toBeDefined();
         expect(
            p?.leftOut?.map(({ dimension, reads }) => ({ dimension, reads })),
         ).toEqual([
            // Declared on a base whose block the served copy keeps, so it is
            // left out too and computed at read in that block.
            { dimension: "base_hour", reads: ["time_zone"] },
            { dimension: "mine", reads: ["given"] },
            { dimension: "label", reads: ["given"] },
            { dimension: "imine", reads: ["given"] },
            { dimension: "hr", reads: ["time_zone"] },
            { dimension: "ts_month", reads: ["time_zone"] },
            { dimension: "age", reads: ["current_time"] },
         ]);
         // Only a base's dimension says where it is declared.
         expect(
            p?.leftOut?.filter((d) => d.at).map((d) => d.dimension),
         ).toEqual(["base_hour"]);
         expect(p?.keptBases?.map((b) => b.sourceID.split("@")[0])).toEqual([
            "base",
         ]);
         expect(
            p?.columns.map((col) =>
               col.zoneFree ? `${col.column}*` : col.column,
            ),
         ).toEqual([
            "id",
            "org_id",
            "s",
            "amount",
            "ts",
            "d",
            "d_month*",
            "big*",
         ]);
         // The build selects none of them, so it reads no given and bakes no
         // default: the eligibility gate admits it.
         const source = Object.values(c.sources).find(
            (s) => s.name === "orders",
         );
         const sql = addressSQL(source!, c.targets?.[p!.sourceID]);
         for (const left of ["mine", "label", "imine", "hr", "age"]) {
            expect(sql).not.toContain(`"${left}"`);
         }
         expect(sql).not.toMatch(/=\s*1\b/);
         const outcome = await computePackageBuildPlan(pkg);
         expect(outcome.plan?.refusedSources?.[p!.sourceID]).toBeUndefined();
         expect(outcome.sourceEligibility?.eligible).toContain(p!.sourceID);
      },
      { timeout: 30000 },
   );

   it(
      "leaves out a given-reading dimension declared on a base, keeping that base's block",
      async () => {
         const { pkg, c } = await compiled(`${HEAD}
source: base is raw extend { dimension: mine is org_id = $ORG_ID }
source: mid is base extend { dimension: big is amount > 10 }
#(materialize) storage="lake"
source: orders is mid extend { measure: n is count() }
`);
         const p = planOf(c, "orders");
         expect(
            p?.leftOut?.map(({ dimension, reads }) => ({ dimension, reads })),
         ).toEqual([{ dimension: "mine", reads: ["given"] }]);
         expect(p?.leftOut?.[0].at?.url).toEndWith("/m.malloy");
         // The farthest base it needs, and every one between.
         expect(p?.keptBases?.map((b) => b.sourceID.split("@")[0])).toEqual([
            "mid",
            "base",
         ]);
         expect(p?.columns.map((col) => col.column)).not.toContain("mine");
         const outcome = await computePackageBuildPlan(pkg);
         expect(outcome.plan?.refusedSources?.[p!.sourceID]).toBeUndefined();
         expect(outcome.sourceEligibility?.eligible).toContain(p!.sourceID);
      },
      { timeout: 30000 },
   );

   it(
      "refuses a given-reading dimension declared on a base in another file",
      async () => {
         const pkg = await packages.create({
            "b.malloy": `${HEAD}
source: base is raw extend { dimension: mine is org_id = $ORG_ID }
`,
            "m.malloy": `##! experimental { givens access_modifiers }
import "b.malloy"
#(materialize) storage="lake"
source: orders is base extend { measure: n is count() }
`,
         });
         const c = await compilePackageBuildPlan(pkg);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain(
            "the dimension `mine` reads a given and is declared where the served copy cannot keep its definition",
         );
      },
      { timeout: 30000 },
   );

   it(
      "leaves out a dimension reading the given its own where: reads (the IR walk finds it)",
      async () => {
         const { c } = await compiled(`${HEAD}
#(materialize) storage="lake"
source: scoped is raw extend {
  where: org_id = $ORG_ID
  dimension: mine is org_id = $ORG_ID
  dimension: big is amount > 10
}
`);
         const p = planOf(c, "scoped");
         expect(p?.leftOut).toEqual([{ dimension: "mine", reads: ["given"] }]);
         expect(p?.columns.map((col) => col.column)).not.toContain("mine");
         // P2 still leaves the term out of the build.
         expect(p?.readTimeTerms?.map((t) => t.code)).toEqual([
            "org_id = $ORG_ID",
         ]);
         expect(p?.columns.find((col) => col.column === "big")?.zoneFree).toBe(
            true,
         );
      },
      { timeout: 30000 },
   );

   it(
      "without the probe compiler, the IR walks still leave out given and clock readers and clear nothing",
      async () => {
         const contents = await authorContents(ORDERS);
         const unprobed = planModelMaterialize(contents).plans.find(
            (q) => q.sourceName === "orders",
         )!;
         expect(unprobed.leftOut?.map((d) => d.dimension)).toEqual([
            "mine",
            "label",
            "imine",
            "age",
         ]);
         expect(unprobed.columns.some((col) => col.zoneFree)).toBe(false);
         // The zone-dependent ones are stored, as before P9: a timezone:
         // query over the table is then declined.
         expect(unprobed.columns.map((col) => col.column)).toContain("hr");
      },
      { timeout: 30000 },
   );

   it(
      "honours a probe answer the IR walks do not see: a given, the clock, or an unsettled zone",
      async () => {
         const contents = await authorContents(ORDERS);
         const sourceID = planModelMaterialize(contents).plans.find(
            (q) => q.sourceName === "orders",
         )!.sourceID;
         const planned = planModelMaterialize(
            contents,
            undefined,
            undefined,
            new Map([
               [
                  sourceID,
                  new Map([
                     ["big", { givens: ["given/x"] }],
                     [
                        "d_month",
                        { givens: [], currentTime: true, timeZone: false },
                     ],
                     ["hr", { givens: [], currentTime: false }],
                  ]),
               ],
            ]),
         ).plans.find((q) => q.sourceID === sourceID)!;
         expect(planned.leftOut).toContainEqual({
            dimension: "big",
            reads: ["given"],
         });
         expect(planned.leftOut).toContainEqual({
            dimension: "d_month",
            reads: ["current_time"],
         });
         // A zone the probe could not settle is neither left out nor cleared.
         const hr = planned.columns.find((col) => col.column === "hr");
         expect(hr).toBeDefined();
         expect(hr?.zoneFree).toBeUndefined();
      },
      { timeout: 30000 },
   );

   it(
      "refuses an inlined join through which a given is read",
      async () => {
         const { c } = await compiled(`${HEAD}
source: orgs is duckdb.sql("select 1 as org_id, 'x' as name") extend {
  primary_key: org_id
  dimension: is_mine is org_id = $ORG_ID
}
#(materialize) storage="lake" inline=["orgs"]
source: orders is raw extend {
  join_one: orgs with org_id
}
`);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain(
            "inline= names `orgs`, through which a given is read",
         );
      },
      { timeout: 30000 },
   );
});

/** The compiled `contents` of a one-file model, for calling the planner directly. */
async function authorContents(text: string): Promise<Record<string, unknown>> {
   const { Runtime, InMemoryURLReader, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const url = new URL("file:///p9/m.malloy");
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[url.toString(), text]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", new DuckDBConnection("duckdb", ":memory:")]]),
         "duckdb",
      ),
   });
   const model = await runtime.loadModel(url).getModel();
   return model._modelDef.contents as Record<string, unknown>;
}
