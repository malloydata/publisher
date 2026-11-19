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
   [
      "a #(rollup) and a v1 #@ preaggregate",
      `${RAW}source: r is raw extend {
  #(rollup) grain=["c"]
  measure: n is count()
}
source: v is raw extend {
  #@ preaggregate grain="c"
  measure: total is id.sum()
}
`,
   ],
   [
      "a persisted view beside a #@ persist source",
      `${RAW}#@ persist name="p"
source: p is raw -> { group_by: c }
source: d is raw extend {
  #@ persist name="by_c"
  view: by_c is { group_by: c }
}
`,
   ],
   [
      "a gated #@ persist source and a gated #(materialize) source",
      `${RAW.replace("composite_sources", "composite_sources givens")}given:
  ORG :: number

#(access_filter) id = $ORG
#@ persist name="gp"
source: gp is raw -> { select: id, c } extend {}
#(access_filter) id = $ORG
#(materialize) name="gm"
source: gm is raw extend { where: id > 0 }
`,
   ],
];

/**
 * Everything a derived plan carries, as plain sorted data, so the two paths
 * are compared field by field.
 */
function whole(outcome: BuildPlanOutcome) {
   const sorted = <T>(values: Iterable<T>) => [...values].sort();
   return {
      ...outcome,
      annotationFindings: sorted(
         outcome.annotationFindings.map((f) => JSON.stringify(f)),
      ),
      databaseSourceEligibility: {
         ...outcome.databaseSourceEligibility,
         eligibleEntityIds: sorted(
            outcome.databaseSourceEligibility.eligibleEntityIds,
         ),
      },
   };
}

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
      databaseSourceEligibility: {
         eligible: sorted(outcome.databaseSourceEligibility.eligibleEntityIds),
         refused: sorted(
            Object.keys(outcome.databaseSourceEligibility.refused),
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
         // Every side table, not only the judgement.
         expect(whole(worker.outcome)).toEqual(whole(main));
      });
   }

   it("sends a plan that survives structured cloning unchanged", async () => {
      const { worker } = await bothPaths(packages, SHAPES[4][1]);
      expect(worker?.ok).toBe(true);
      if (!worker?.ok) return;
      expect(structuredClone(worker.outcome)).toEqual(worker.outcome);
   });

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

   it("refuses a pair table's name= the readable name would rewrite, and reports a name= an earlier check refused once", async () => {
      const model = `${RAW}source: lines is duckdb.sql("""SELECT 7 AS line_id, 1 AS order_id, 5 AS qty""") extend {
  primary_key: line_id
}
#(materialize) name="My-Pair" inline=["lines"]
source: orders is raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
#@ persist name='analytics."Orders"'
source: q is raw -> { group_by: c }
`;
      const { main, worker } = await bothPaths(
         packages,
         model,
         { materialization: { scope: "version" } },
         { versionId: "1.0.0", scope: "version" },
      );
      expect(worker?.ok).toBe(true);
      if (!worker?.ok) return;
      expect(judged(worker.outcome)).toEqual(judged(main));
      const malformed = main.annotationFindings
         .filter((f) => f.tier === "malformed")
         .map((f) => [f.code, f.subject]);
      expect(malformed).toContainEqual(["materialize-invalid", "orders"]);
      expect(
         malformed.filter(([code]) => code === "persist-invalid"),
      ).toHaveLength(1);
      // The pair is not planned; the source an earlier check refused stays
      // as that check left it (the load fails on its finding).
      expect(
         Object.values(main.plan?.sources ?? {}).map((s) => s.name),
      ).toEqual(["q"]);
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
