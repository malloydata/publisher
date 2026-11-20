// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// R-13: a table read by a source's build that has no reading
// (`read_failed`) refuses that source alone as `upstream_unread`, rather than
// addressing it by an unknown schema; a table the dialect gives no catalog to
// read (`no_catalog`) is a stable fold and refuses nothing.

import { afterEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import {
   type UpstreamSchemaReading,
   type UpstreamTable,
   upstreamTableKey,
} from "../domain/upstream_schemas";
import { computeSourceEntityId } from "./addressing";
import { compilePackageBuildPlan } from "./build_plan_compile";

const MODEL = `##! experimental.persistence
source: t is duckdb.table('t')

#@ persist name="by_cat"
source: by_cat is t -> { group_by: cat; aggregate: s is amt.sum() }

#@ persist name="plain"
source: plain is duckdb.sql("SELECT 1 AS one") -> { group_by: one }
`;

const packages = tempBuildPlanPackages();
afterEach(() => packages.cleanup());

/** The compiled plan, with every table the plan reads read as `reading`. */
async function planWith(reading: UpstreamSchemaReading) {
   const pkg = await packages.create(MODEL, {
      readUpstreamSchemas: async (tables: UpstreamTable[]) =>
         Object.fromEntries(tables.map((t) => [upstreamTableKey(t), reading])),
   });
   const duckdb = await pkg.getMalloyConnection("duckdb");
   await duckdb.runSQL(
      "CREATE OR REPLACE TABLE t AS SELECT 'a' AS cat, 1 AS amt",
   );
   return compilePackageBuildPlan(pkg);
}

const names = (plan: Awaited<ReturnType<typeof planWith>>) =>
   Object.values(sourcesIn(plan))
      .map((s) => s.name)
      .sort();

describe("R-13: a source whose upstream table has no reading", () => {
   it("is refused alone as upstream_unread, its warning naming the table", async () => {
      const plan = await planWith({ unread: "read_failed" });
      expect(names(plan)).toEqual(["plain"]);
      const refusals = (plan.annotationFindings ?? []).filter(
         (f) => f.refused?.reason === "upstream_unread",
      );
      expect(refusals.map((f) => [f.code, f.subject])).toEqual([
         ["persist-not-built", "by_cat"],
      ]);
      expect(refusals[0].message).toContain('"t" on connection "duckdb"');
   });

   it("is planned, at a stable address, when the dialect gives it no catalog to read", async () => {
      const first = await planWith({ unread: "no_catalog" });
      const second = await planWith({ unread: "no_catalog" });
      expect(names(first)).toEqual(["by_cat", "plain"]);
      const addressOf = (plan: typeof first) => {
         const source = Object.values(sourcesIn(plan)).find(
            (s) => s.name === "by_cat",
         )!;
         return computeSourceEntityId(
            source,
            plan.connectionDigests,
            undefined,
            undefined,
            plan.upstreamSchemas,
         );
      };
      expect(addressOf(first)).toBe(addressOf(second));
   });
});
