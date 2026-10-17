// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   observePackageStates,
   recordPackageReload,
   resetPackageStateGaugeForTesting,
} from "./package_state_metrics";
import type { EnvironmentStore } from "./service/environment_store";
import { resetInstrumentsForTesting } from "./telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

const environment = (loaded: number, failed: string[], stale: string[]) => ({
   getLoadedPackages: () => new Array(loaded).fill({}),
   getFailedPackages: () => new Map(failed.map((name) => [name, "boom"])),
   getStaleCompileErrors: () => new Map(stale.map((name) => [name, {}])),
});

describe("package state metrics", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
      resetPackageStateGaugeForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
      resetPackageStateGaugeForTesting();
   });

   it("sums packages by state across environments at each scrape", async () => {
      const environments = [
         environment(2, ["broken"], ["edited"]),
         environment(1, [], []),
      ];
      observePackageStates({
         getLoadedEnvironments: () => environments,
      } as unknown as EnvironmentStore);
      const values = async () => {
         const byState: Record<string, number> = {};
         for (const state of ["serving", "failed", "stale"]) {
            // One data point per state, so the filtered sum is its value.
            byState[state] = await harness.collectCounter(
               "publisher_packages",
               {
                  state,
               },
            );
         }
         return byState;
      };
      expect(await values()).toEqual({ serving: 3, failed: 1, stale: 1 });
      environments.push(environment(0, ["another"], []));
      expect((await values()).failed).toBe(2);
   });

   it("counts reloads by trigger and outcome", async () => {
      recordPackageReload("watch", "failure");
      recordPackageReload("mcp", "success");
      recordPackageReload("mcp", "success");
      const count = (trigger: string, outcome: string) =>
         harness.collectCounter("publisher_package_reloads_total", {
            trigger,
            outcome,
         });
      expect(await count("watch", "failure")).toBe(1);
      expect(await count("mcp", "success")).toBe(2);
      expect(await count("rest", "success")).toBe(0);
   });
});
