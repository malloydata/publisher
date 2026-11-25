// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A failed run's reclaim counts its storage drops as a reclaim: the drop
 * counter's `occasion` tells a reclaim apart from a caller's delete.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import { Reclaimer } from "./reclaimer";

let harness: MetricsHarness;
beforeEach(async () => {
   resetMaterializationTelemetryForTesting();
   harness = await startMetricsHarness();
});
afterEach(async () => {
   await harness.shutdown();
});

describe("a failed run's reclaim", () => {
   it("counts each storage drop it attempts under occasion reclaim", async () => {
      const reclaimer = new Reclaimer({
         repository: () => ({ listMaterializations: async () => [] }) as never,
         host: {} as never,
         lifecycle: {} as never,
         dropManifestEntries: async () => [],
      });
      // A destination the drop cannot open: the attempt still counts.
      const environment = {
         getStorageDestination: () => ({ name: "lake", type: "ducklake" }),
         getEnvironmentPath: () => "/nonexistent",
         getEnvironmentName: () => "env",
         getApiConnection: () => ({}),
      } as never;
      await reclaimer.reclaimStorageTablesFromFailedRun(
         [
            {
               sourceEntityId: "a1",
               physicalTableName: "t1",
               storageDestinationName: "lake",
               connectionName: "warehouse",
            } as never,
         ],
         environment,
         { environmentId: "env-1", packageName: "pkg" },
      );
      const drops = await harness.collectAttributes(
         "publisher_materialization_drop_tables_total",
      );
      expect(drops).toEqual([
         expect.objectContaining({ tier: "storage", occasion: "reclaim" }),
      ]);
   });
});
