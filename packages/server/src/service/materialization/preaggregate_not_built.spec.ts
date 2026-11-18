// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A valid rollup a compile-time gate refuses alone (a read-only dialect, a
 * base scoped by a given, a shared table name, the `storage=` gate) is
 * reported as `rollup-not-built`, its message naming its base, its grain
 * and the gate's reason, while a refused `#@ persist` source keeps
 * `persist-source-refused`. Which rollups are refused does not change.
 */
import { rollupPlansIn } from "../../test_helpers/materialization_test_fixtures";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../api";
import { tempBuildPlanPackages } from "../../test_helpers/incremental_test_harness";
import { installSettings } from "../../test_helpers/settings_env";
import {
   loadTempPackage,
   usePackageLoadPool,
} from "../../test_helpers/temp_package";
import { BadRequestError } from "../../errors";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "./package_materialization";
import { deriveBuildPlanOutcome } from "./plan/build_plan";
import { compilePackageBuildPlan } from "./plan/build_plan_compile";

type BuildPlan = components["schemas"]["BuildPlan"];

const buildPlanPackages = tempBuildPlanPackages();

let restore: (() => void) | undefined;
beforeAll(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
afterAll(() => restore?.());
afterEach(() => buildPlanPackages.cleanup());

const ORDERS = `source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'B')) AS t(amount, category)
""") extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}
`;

/** A materialization holding `plan` and `refusedRollups`, as a load leaves them. */
function holding(
   plan: BuildPlan | null,
   refusedRollups: Record<
      string,
      { baseSourceName: string; grainDimensions: string[] }
   > = {},
): PackageMaterialization {
   const materialization = new PackageMaterialization({
      models: () => new Map(),
      packageName: () => "pkg",
   } as unknown as PackageMaterializationOwner);
   materialization.plan.buildPlan = plan;
   Object.assign((materialization as unknown as { plan: object }).plan, {
      refusedRollups,
   });
   return materialization;
}

const GATE_MESSAGE =
   "Pre-aggregation rollup 'orders__category__0a1b2c3d' cannot be built: the reason.";

function refusedPlan(
   tier: "preaggregate" | "database",
   sources: BuildPlan["sources"] = {},
): BuildPlan {
   return {
      sources,
      refusedSources: {
         "r@file:///m.malloy": {
            name: "orders__category__0a1b2c3d",
            sourceID: "r@file:///m.malloy",
            modelPath: "m.malloy",
            tier,
            reason: "read_only_dialect",
            message: GATE_MESSAGE,
         },
      },
   } as unknown as BuildPlan;
}

describe("rollup-not-built", () => {
   it(
      "names the refused rollup's base and grain, and keeps it refused",
      async () => {
         const compiled = await compilePackageBuildPlan(
            await buildPlanPackages.create({ "orders.malloy": ORDERS }),
         );
         const [rollupID] = Object.keys(rollupPlansIn(compiled));
         expect(rollupID).toBeDefined();
         // A rollup on a dialect whose connection creates no table: the
         // database gate refuses it, and nothing else about it changes. The
         // dialect is shadowed on the compiled source itself, so the plan
         // still projects that same object and keeps the rollup in
         // `sources`, as a real read-only-dialect refusal does.
         Object.defineProperty(
            compiled.tables[rollupID as string].source,
            "dialectName",
            { get: () => "sqlserver" },
         );
         const outcome = deriveBuildPlanOutcome(compiled, null);
         expect(
            outcome.plan?.refusedSources?.[rollupID as string],
         ).toMatchObject({
            origin: "rollup",
            tier: "database",
            reason: "read_only_dialect",
         });
         expect(
            outcome.plan?.sources?.[rollupID as string]?.preaggregate,
         ).toMatchObject({
            baseSourceName: "orders",
            grainDimensions: ["category"],
         });
         expect(outcome.plan?.omittedSources ?? []).toEqual([]);
         expect(outcome.refusedRollups).toEqual({
            [rollupID as string]: {
               baseSourceName: "orders",
               grainDimensions: ["category"],
            },
         });

         const warnings = holding(
            outcome.plan,
            outcome.refusedRollups,
         ).persistRefusalWarnings();
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toMatchObject({
            code: "rollup-not-built",
            severity: "warn",
            subject: "orders",
            model: "orders.malloy",
         });
         expect(warnings[0]?.message).toStartWith(
            '#(rollup) rollup of source "orders" at (category) is not built, so its measures are served live: Pre-aggregation rollup',
         );
         expect(warnings[0]?.message).not.toContain("file://");
      },
      { timeout: 60000 },
   );

   it("names a rollup the refusal left out of the plan's sources", () => {
      const [warning] = holding(refusedPlan("preaggregate"), {
         "r@file:///m.malloy": {
            baseSourceName: "orders",
            grainDimensions: ["category", "region"],
         },
      }).persistRefusalWarnings();
      expect(warning).toEqual({
         code: "rollup-not-built",
         severity: "warn",
         model: "m.malloy",
         subject: "orders",
         message: `#(rollup) rollup of source "orders" at (category, region) is not built, so its measures are served live: ${GATE_MESSAGE}`,
      });
   });

   it("still warns when a worker sent no base or grain", () => {
      const [warning] = holding(
         refusedPlan("preaggregate"),
      ).persistRefusalWarnings();
      expect(warning?.code).toBe("rollup-not-built");
      expect(warning?.subject).toBeUndefined();
      expect(warning?.message).toBe(
         `A #(rollup) rollup is not built, so its measures are served live: ${GATE_MESSAGE}`,
      );
   });

   it("warns persist-not-built for a refused #@ persist source, and its former code beside it for one release", () => {
      const warnings = holding(
         refusedPlan("database"),
      ).persistRefusalWarnings();
      const expected = {
         severity: "warn" as const,
         model: "m.malloy",
         subject: "orders__category__0a1b2c3d",
         message: GATE_MESSAGE,
      };
      expect(warnings).toEqual([
         { ...expected, code: "persist-not-built" },
         { ...expected, code: "persist-source-refused" },
      ]);
   });
});

describe("rollup-invalid", () => {
   // After the file's settings: a load worker keeps the settings it started with.
   usePackageLoadPool();

   it(
      "fails the load for a malformed #(rollup) line",
      async () => {
         let error: unknown;
         try {
            const loaded = await loadTempPackage({
               "orders.malloy": `source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)
""") extend {
  #(rollup) grain=[]
  measure: total is amount.sum()
}
`,
            });
            await loaded.cleanup();
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         const message = (error as Error).message;
         expect(message).toContain(
            '#(rollup) on measure "total" of source "orders"',
         );
         expect(message).toContain("[rollup-invalid]");
      },
      { timeout: 60000 },
   );
});
