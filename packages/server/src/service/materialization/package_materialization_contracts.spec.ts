// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Each table a snapshot serves carries its column contract, parsed once per
 * snapshot, by content address: what read admission looks a stored column's
 * collation up in, instead of reading the table's catalog per answer.
 */

import { describe, expect, it } from "bun:test";
import {
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import type { FreshnessManifest } from "./domain/types";
import { PackageMaterialization } from "./package_materialization";
import { contractFor } from "../../test_helpers/contract_fixture";

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

describe("a snapshot's contracts", () => {
   it("hold each served database table's contract, parsed, and none for a table without one", async () => {
      const m = facade();
      await m.applyLoadBuildPlan(okWorkerPlan(makePlanOutcome({})));
      (m as unknown as { databaseEntries: FreshnessManifest }).databaseEntries =
         {
            "build-orders": {
               tableName: "public.orders_t",
               contract: contractFor(),
            },
         };
      const snapshot = m.snapshot();
      const contract = snapshot.contracts?.get("build-orders");
      expect(contract?.v).toBe(1);
      expect(snapshot.contracts?.has("nothing")).toBe(false);
   });
});
