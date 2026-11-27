// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// F-03: a database table whose `name=` places it in a container (a dataset,
// a schema) its warehouse does not hold is refused at plan as
// `container_not_found`, rather than planned and failing at build. Each
// container is read once per connection; a read that fails refuses nothing;
// `storage=` is never checked.

import { afterEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import {
   type TargetContainer,
   targetContainerKey,
   type TargetContainerReading,
} from "../domain/target_containers";
import { derivePackagePlan } from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";

const MODEL = `##! experimental.persistence
source: t is duckdb.sql("SELECT 'a' AS cat, 1 AS amt")

#@ persist name="analytics.by_cat"
source: by_cat is t -> { group_by: cat; aggregate: s is amt.sum() }

#@ persist name="analytics.by_amt"
source: by_amt is t -> { group_by: amt }

#@ persist name="other_proj.reports.totals"
source: totals is t -> { aggregate: s is amt.sum() }

#@ persist name="bare"
source: bare is t -> { group_by: cat }

#@ persist name="lake_ds.stored" storage=lake
source: stored is t -> { group_by: cat }
`;

const packages = tempBuildPlanPackages();
afterEach(() => packages.cleanup());

/**
 * The compiled plan, each container read as `readingOf` answers it, with
 * every call the plan made to its reader.
 */
async function planWith(
   readingOf: (c: TargetContainer) => TargetContainerReading,
) {
   const calls: TargetContainer[][] = [];
   const pkg = await packages.create(MODEL, {
      readTargetContainers: async (containers: TargetContainer[]) => {
         calls.push(containers);
         return Object.fromEntries(
            containers.map((c) => [targetContainerKey(c), readingOf(c)]),
         );
      },
   });
   return { plan: await compilePackageBuildPlan(pkg), calls };
}

const names = (plan: Awaited<ReturnType<typeof planWith>>["plan"]) =>
   Object.values(sourcesIn(plan))
      .map((s) => s.name)
      .sort();

const refusals = (plan: Awaited<ReturnType<typeof planWith>>["plan"]) =>
   (plan.annotationFindings ?? []).filter(
      (f) => f.refused?.reason === "container_not_found",
   );

describe("F-03: a database table's container is checked at plan", () => {
   it("reads each (connection, container) once, never a storage= one or a bare name", async () => {
      const { calls } = await planWith(() => ({ found: true }));
      expect(calls).toHaveLength(1);
      expect(calls[0]).toEqual([
         { connectionName: "duckdb", container: "analytics" },
         { connectionName: "duckdb", container: "other_proj.reports" },
      ]);
   });

   it("plans every table whose container is found", async () => {
      const { plan } = await planWith(() => ({ found: true }));
      expect(refusals(plan)).toEqual([]);
      expect(names(plan)).toContain("by_cat");
      expect(names(plan)).toContain("totals");
   });

   it("refuses each table in a container that is not found, naming it and the fix", async () => {
      const { plan } = await planWith((c) =>
         c.container === "analytics" ? { notFound: true } : { found: true },
      );
      expect(names(plan)).not.toContain("by_cat");
      expect(names(plan)).not.toContain("by_amt");
      expect(names(plan)).toContain("totals");
      expect(names(plan)).toContain("bare");
      const found = refusals(plan);
      expect(found.map((f) => [f.code, f.subject]).sort()).toEqual([
         ["persist-not-built", "by_amt"],
         ["persist-not-built", "by_cat"],
      ]);
      expect(found[0].message).toContain('schema "analytics"');
      expect(found[0].message).toContain('connection "duckdb" does not hold');
      expect(found[0].message).toContain("or change name=");
      // The wire's refusal record says the same.
      const derived = derivePackagePlan(plan, null);
      expect(derived.plan?.refusedSources).toBeDefined();
      const wire = Object.values(derived.plan!.refusedSources!).filter(
         (r) => r.reason === "container_not_found",
      );
      expect(wire.map((r) => [r.name, r.tier]).sort()).toEqual([
         ["by_amt", "database"],
         ["by_cat", "database"],
      ]);
   });

   it("checks a 3-part name's container in the project it names", async () => {
      const { plan, calls } = await planWith((c) =>
         c.container === "other_proj.reports"
            ? { notFound: true }
            : { found: true },
      );
      expect(calls[0]).toContainEqual({
         connectionName: "duckdb",
         container: "other_proj.reports",
      });
      expect(refusals(plan).map((f) => f.subject)).toEqual(["totals"]);
      expect(refusals(plan)[0].message).toContain('"other_proj.reports"');
   });

   it("refuses a table whose container is in another location, naming both", async () => {
      const { plan } = await planWith((c) =>
         c.container === "analytics"
            ? { otherLocation: { location: "EU", expected: "US" } }
            : { found: true },
      );
      const found = refusals(plan);
      expect(found.map((f) => f.subject).sort()).toEqual(["by_amt", "by_cat"]);
      expect(found[0].message).toContain('location "EU"');
      expect(found[0].message).toContain('in "US"');
   });

   it("refuses nothing for a container whose read failed (fail open)", async () => {
      const { plan } = await planWith(() => ({ unread: true }));
      expect(refusals(plan)).toEqual([]);
      expect(names(plan)).toContain("by_cat");
      expect(names(plan)).toContain("totals");
   });

   it("reads nothing, and refuses nothing, for a package with no reader", async () => {
      const pkg = await packages.create(MODEL);
      const plan = await compilePackageBuildPlan(pkg);
      expect(refusals(plan)).toEqual([]);
      expect(names(plan)).toContain("by_cat");
   });
});
