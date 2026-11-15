// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A valid rollup a compile-time gate refuses alone (a read-only dialect, a
 * base scoped by a given, a shared table name, the `storage=` gate) is
 * reported as `preaggregate-not-built`, its message naming its base, its grain
 * and the gate's reason, while a refused `#@ persist` source keeps
 * `persist-source-refused`. Which rollups are refused does not change.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../api";
import { tempBuildPlanPackages } from "../../test_helpers/incremental_test_harness";
import { installSettings } from "../../test_helpers/settings_env";
import {
   loadTempPackage,
   usePackageLoadPool,
} from "../../test_helpers/temp_package";
import { formatPublishRejections } from "../publish_checks";
import {
   PackageMaterialization,
   PERSIST_SOURCE_REFUSED_CODE,
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
  #(preaggregate) grain=["category"]
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
   Object.assign(materialization, { buildPlan: plan, refusedRollups });
   return materialization;
}

const GATE_MESSAGE =
   "Pre-aggregation rollup 'orders__category__0a1b2c3d' cannot be built: the reason.";

function refusedPlan(
   tier: "preaggregate" | "colocated",
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

describe("preaggregate-not-built", () => {
   it(
      "names the refused rollup's base and grain, and keeps it refused",
      async () => {
         const compiled = await compilePackageBuildPlan(
            await buildPlanPackages.create({ "orders.malloy": ORDERS }),
         );
         const [rollupID] = Object.keys(compiled.preaggregatePlans ?? {});
         expect(rollupID).toBeDefined();
         // A rollup on a dialect whose connection creates no table: the
         // colocated gate refuses it, and nothing else about it changes.
         const source = compiled.sources[rollupID as string];
         compiled.sources[rollupID as string] = new Proxy(source, {
            get: (target, key, receiver) =>
               key === "dialectName"
                  ? "sqlserver"
                  : Reflect.get(target, key, receiver),
         });
         const outcome = deriveBuildPlanOutcome(compiled, null);
         expect(
            outcome.plan?.refusedSources?.[rollupID as string],
         ).toMatchObject({ tier: "preaggregate", reason: "read_only_dialect" });
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
            code: "preaggregate-not-built",
            severity: "warn",
            subject: "orders",
            model: "orders.malloy",
         });
         expect(warnings[0]?.message).toStartWith(
            "The rollup of `orders` at (category) is not built, so its measures are served live: Pre-aggregation rollup",
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
         code: "preaggregate-not-built",
         severity: "warn",
         model: "m.malloy",
         subject: "orders",
         message: `The rollup of \`orders\` at (category, region) is not built, so its measures are served live: ${GATE_MESSAGE}`,
      });
   });

   it("still warns when a worker sent no base or grain", () => {
      const [warning] = holding(
         refusedPlan("preaggregate"),
      ).persistRefusalWarnings();
      expect(warning?.code).toBe("preaggregate-not-built");
      expect(warning?.subject).toBeUndefined();
      expect(warning?.message).toBe(
         `A rollup is not built, so its measures are served live: ${GATE_MESSAGE}`,
      );
   });

   it("leaves a refused #@ persist source on persist-source-refused", () => {
      const [warning] = holding(
         refusedPlan("colocated"),
      ).persistRefusalWarnings();
      expect(warning).toEqual({
         code: PERSIST_SOURCE_REFUSED_CODE,
         severity: "warn",
         model: "m.malloy",
         subject: "orders__category__0a1b2c3d",
         message: GATE_MESSAGE,
      });
   });
});

describe("preaggregate-malformed", () => {
   // After the file's settings: a load worker keeps the settings it started with.
   usePackageLoadPool();

   it(
      "still fails the publish for a malformed #(preaggregate) line",
      async () => {
         const loaded = await loadTempPackage({
            "orders.malloy": `source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A')) AS t(amount, category)
""") extend {
  #(preaggregate) grain=[]
  measure: total is amount.sum()
}
`,
         });
         try {
            const warning = (
               loaded.pkg.getPackageMetadata().warnings ?? []
            ).find((w) => w.subject === "orders.total");
            expect(warning).toMatchObject({
               code: "preaggregate-malformed",
               severity: "error",
            });
            expect(formatPublishRejections(loaded.pkg)).toContain(
               '#(preaggregate) on measure "total" of source "orders"',
            );
         } finally {
            await loaded.cleanup();
         }
      },
      { timeout: 60000 },
   );
});
