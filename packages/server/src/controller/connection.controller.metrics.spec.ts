// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { EnvironmentStore } from "../service/environment_store";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { ConnectionController } from "./connection.controller";

describe("connection operation metrics", () => {
   let harness: MetricsHarness;
   const controller = new ConnectionController({} as EnvironmentStore);
   const count = (labels: Record<string, string>) =>
      harness.collectCounter("publisher_connection_operations_total", labels);

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   it("counts a connection test by type and result", async () => {
      const result = await controller.testConnectionConfiguration({
         name: "probe",
         type: "duckdb",
      });
      await expect(
         controller.testConnectionConfiguration({} as never),
      ).rejects.toThrow();
      expect(
         await count({
            operation: "test",
            type: "duckdb",
            outcome: result.status === "ok" ? "success" : "failed",
         }),
      ).toBe(1);
      expect(
         await count({
            operation: "test",
            type: "unknown",
            outcome: "refused",
         }),
      ).toBe(1);
   });

   it("counts a refused create under its type", async () => {
      await expect(
         controller.addConnection("env", "c", { type: "postgres" } as never),
      ).rejects.toThrow();
      expect(
         await count({
            operation: "create",
            type: "postgres",
            outcome: "refused",
         }),
      ).toBe(1);
   });
});
