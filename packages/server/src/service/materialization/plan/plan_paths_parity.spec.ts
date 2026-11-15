// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The load worker's build plan and compilePackageBuildPlan's are one plan:
 * the same sources planned, refused alone and found invalid, in the same
 * order of judgement (a collision before an unbuildable incremental
 * declaration), so a package the load admits is the package a run builds.
 */

import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as path from "path";
import { getPackageLoadPool } from "../../../package_load/package_load_pool";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { usePackageLoadPool } from "../../../test_helpers/temp_package";
import { computePackageBuildPlan } from "./build_plan_compile";
import type { BuildPlanOutcome } from "./build_plan";

const RAW = `##! experimental { persistence composite_sources }
source: raw is duckdb.sql("""SELECT 1 AS id, DATE '2024-01-01' AS d, 'A' AS c""")
`;

const SHAPES: [string, string][] = [
   [
      "two #(materialize) at one name=, one incremental",
      `${RAW}#(materialize) name="t" refresh="incremental" watermark="d"
source: a is raw extend { where: id > 0 }
#(materialize) name="t"
source: b is raw extend { where: id < 0 }
`,
   ],
   [
      "two #@ persist at one name=, one incremental",
      `${RAW}#@ persist name="t" refresh="incremental" watermark="d"
source: a is raw -> { group_by: id, d }
#@ persist name="t"
source: b is raw -> { group_by: c }
`,
   ],
   [
      "an incremental #@ persist on DuckDB beside a plain one",
      `${RAW}#@ persist name="t1" refresh="incremental" watermark="d"
source: a is raw -> { group_by: id, d }
#@ persist name="t2"
source: b is raw -> { group_by: c }
`,
   ],
   [
      "a #(materialize) with an unknown key",
      `${RAW}#(materialize) nmae="t"
source: a is raw extend { where: id > 0 }
`,
   ],
];

/** What both paths must agree on, order-free. */
function judged(outcome: BuildPlanOutcome) {
   return {
      planned: Object.values(outcome.plan?.sources ?? {})
         .map((s) => s.name)
         .sort(),
      findings: outcome.annotationFindings
         .map((f) => `${f.tier}:${f.code}:${f.subject ?? ""}`)
         .sort(),
   };
}

describe("the load worker and compilePackageBuildPlan plan alike", () => {
   usePackageLoadPool();
   const packages = tempBuildPlanPackages();
   afterAll(() => packages.cleanup());

   for (const [name, model] of SHAPES) {
      it(name, async () => {
         const pkg = await packages.create(model);
         await fs.writeFile(
            path.join(pkg.getPackagePath(), "publisher.json"),
            JSON.stringify({ name: "pkg", description: "fixture" }),
         );
         const main = await computePackageBuildPlan(pkg);
         const loaded = await getPackageLoadPool().loadPackage({
            packagePath: pkg.getPackagePath(),
            packageName: "pkg",
            environmentName: "e",
            malloyConfig: pkg.getMalloyConfig(),
            defaultConnectionName: "duckdb",
            computeBuildPlan: true,
         });
         const worker = loaded.buildPlan;
         expect(worker?.ok).toBe(true);
         if (!worker?.ok) return;
         expect(judged(worker.outcome)).toEqual(judged(main));
      });
   }

   it("judges a collision before an unbuildable incremental declaration", async () => {
      const pkg = await packages.create(SHAPES[0][1]);
      const main = await computePackageBuildPlan(pkg);
      expect(
         main.annotationFindings.filter(
            (f) => f.code === "materialization-table-collision",
         ),
      ).toHaveLength(2);
   });
});
