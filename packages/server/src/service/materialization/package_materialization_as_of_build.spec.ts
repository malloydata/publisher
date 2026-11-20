// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A table stored as of its build carries `asOfBuild` on its database manifest
 * entry in the snapshot, read from the plan that serves: after a refresh that
 * fails, the current plan is gone but the last good one keeps serving, and the
 * entry keeps its flag as the bindings keep theirs.
 */

import { describe, expect, it } from "bun:test";
import {
   failedWorkerPlan,
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import type { FreshnessManifest } from "./domain/types";
import { PackageMaterialization } from "./package_materialization";

const ADDRESS = "build-orders";

function facade(): PackageMaterialization {
   return new PackageMaterialization({
      packageName: () => "pkg",
      versionId: () => undefined,
      metadata: () => ({}) as never,
      models: () => new Map(),
      modelPolicy: () => ({ setServingResolvers: () => {} }) as never,
      packagePath: () => "/pkg",
      malloyConfig: () => ({}) as never,
      connection: async () => ({}) as never,
      buildPlanPackage: {} as never,
      publishedSurface: () => undefined,
   });
}

describe("asOfBuild on a database manifest entry", () => {
   it("is read from the serving plan, so a failed refresh keeps it", async () => {
      const m = facade();
      await m.applyLoadBuildPlan(
         okWorkerPlan(
            makePlanOutcome({
               asOfBuild: { "orders@m.malloy": { name: "orders", reads: [] } },
            }),
         ),
      );
      // The bound database table, as a bind leaves it.
      (m as unknown as { databaseEntries: FreshnessManifest }).databaseEntries =
         { [ADDRESS]: { tableName: "public.orders_t" } };
      expect(m.snapshot().entries[ADDRESS]?.asOfBuild).toBe(true);

      m.markBuildPlanStale(["duckdb"]);
      expect(await m.refreshBuildPlan(async () => failedWorkerPlan())).toBe(
         false,
      );
      expect(
         (m as unknown as { plan: { buildPlan: unknown } }).plan.buildPlan,
      ).toBeNull();
      expect(m.snapshot().entries[ADDRESS]?.asOfBuild).toBe(true);
   });
});
