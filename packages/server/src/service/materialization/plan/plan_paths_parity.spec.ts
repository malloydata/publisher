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
   const sorted = (values: Iterable<string>) => [...values].sort();
   return {
      planned: Object.values(outcome.plan?.sources ?? {})
         .map((s) => s.name)
         .sort(),
      refused: Object.values(outcome.plan?.refusedSources ?? {})
         .map((r) => `${r.name}:${r.tier}:${r.reason}`)
         .sort(),
      findings: outcome.annotationFindings
         .map((f) => `${f.tier}:${f.code}:${f.subject ?? ""}`)
         .sort(),
      sourceEligibility: {
         eligible: sorted(outcome.sourceEligibility.eligible),
         refused: sorted(Object.keys(outcome.sourceEligibility.refused)),
         gateKept: sorted(outcome.sourceEligibility.gateKept ?? []),
      },
      colocatedSourceEligibility: {
         eligible: sorted(outcome.colocatedSourceEligibility.eligibleEntityIds),
         refused: sorted(
            Object.keys(outcome.colocatedSourceEligibility.refused),
         ),
      },
      compilerKeys: Object.entries(outcome.compilerKeys).sort(),
   };
}

/** Both paths' outcomes for `model`, `publisher.json` holding `manifest`. */
async function bothPaths(
   packages: ReturnType<typeof tempBuildPlanPackages>,
   model: string,
   manifest: Record<string, unknown> = {},
   published?: { versionId: string; scope: string },
) {
   const pkg = await packages.create(
      model,
      published
         ? {
              getPackageMetadata: () => ({ scope: published.scope }),
              getVersionId: () => published.versionId,
           }
         : {},
   );
   await fs.writeFile(
      path.join(pkg.getPackagePath(), "publisher.json"),
      JSON.stringify({ name: "pkg", description: "fixture", ...manifest }),
   );
   const main = await computePackageBuildPlan(pkg);
   const loaded = await getPackageLoadPool().loadPackage({
      packagePath: pkg.getPackagePath(),
      packageName: "pkg",
      environmentName: "e",
      malloyConfig: pkg.getMalloyConfig(),
      defaultConnectionName: "duckdb",
      computeBuildPlan: true,
      ...(published ? { publishedVersion: true } : {}),
   });
   return { main, worker: loaded.buildPlan };
}

describe("the load worker and compilePackageBuildPlan plan alike", () => {
   usePackageLoadPool();
   const packages = tempBuildPlanPackages();
   afterAll(() => packages.cleanup());

   for (const [name, model] of SHAPES) {
      it(name, async () => {
         const { main, worker } = await bothPaths(packages, model);
         expect(worker?.ok).toBe(true);
         if (!worker?.ok) return;
         expect(judged(worker.outcome)).toEqual(judged(main));
      });
   }

   it("a published version of a scope: version package", async () => {
      // Two `name=` the same: each a readable name of its own per version,
      // so no collision on either path.
      const { main, worker } = await bothPaths(
         packages,
         SHAPES[1][1],
         { materialization: { scope: "version" } },
         { versionId: "1.0.0", scope: "version" },
      );
      expect(worker?.ok).toBe(true);
      if (!worker?.ok) return;
      expect(judged(worker.outcome)).toEqual(judged(main));
      expect(
         main.annotationFindings.filter(
            (f) => f.code === "materialization-table-collision",
         ),
      ).toEqual([]);
   });

   it("refuses a name= the readable name would rewrite in a version of a scope: version package, on both paths", async () => {
      const model = `${RAW}#@ persist name="analytics.My-Table"
source: a is raw -> { group_by: c }
#(materialize) name="2024_t"
source: b is raw extend { where: id > 0 }
#@ persist name="Plain_T"
source: c is raw -> { group_by: id }
source: d is raw extend {
  #@ persist name="by__c"
  view: by_c is { group_by: c }
}
`;
      const published = { versionId: "1.0.0", scope: "version" };
      const manifest = { materialization: { scope: "version" } };
      const { main, worker } = await bothPaths(
         packages,
         model,
         manifest,
         published,
      );
      expect(worker?.ok).toBe(true);
      if (!worker?.ok) return;
      expect(judged(worker.outcome)).toEqual(judged(main));
      // Each fails the load under its route's code; a name= lowercasing
      // alone changes (`Plain_T`) is planned.
      expect(
         main.annotationFindings
            .filter((f) => f.tier === "malformed")
            .map((f) => [f.code, f.subject])
            .sort(),
      ).toEqual([
         ["materialize-invalid", "b"],
         ["persist-invalid", "a"],
         ["persist-view-invalid", "d -> by_c"],
      ]);
      const [a] = main.annotationFindings.filter((f) => f.subject === "a");
      expect(a.message).toContain('would write "My-Table" as "my_table"');
      expect(
         Object.values(main.plan?.sources ?? {}).map((s) => s.name),
      ).toEqual(["c"]);
      // Outside a version, a name= is the table's exact name: not refused.
      const unversioned = await bothPaths(packages, model, manifest);
      expect(
         unversioned.main.annotationFindings.filter(
            (f) => f.tier === "malformed",
         ),
      ).toEqual([]);
   });

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
