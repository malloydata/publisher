// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One policy for a key check that throws, on either tier: the key reads as
 * unknown. It warns, records nothing, and never fails the build.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import { DatabaseTier } from "./database_build_strategy";
import { UniquenessLedger } from "./inline_uniqueness";
import { checkAndRecordKey } from "./instructed_build";
import { StorageBuildStrategy } from "./storage_build_strategy";

const CHECK = { keyColumn: "id", sql: "SELECT 1 WHERE false" };
const MESSAGES = { notRun: "did not run", notUnique: "not unique" };

/** Record one check on a fresh ledger: what it recorded, and what it warned. */
async function record(
   runCheck: Parameters<typeof checkAndRecordKey>[3],
): Promise<{ recorded: unknown; warned: string[] }> {
   const ledger = new UniquenessLedger(undefined, "run-1");
   const recordRollupKey = sinon.spy(ledger, "recordRollupKey");
   const warn = sinon.stub(logger, "warn");
   try {
      await checkAndRecordKey(ledger, "addr", "rollup", runCheck, MESSAGES, {});
      return {
         recorded: recordRollupKey.firstCall?.args[1],
         warned: warn.getCalls().map((c) => String(c.args[0])),
      };
   } finally {
      warn.restore();
   }
}

describe("a key check that throws", () => {
   it("reads as unknown on the database tier, and the build goes on", async () => {
      const connection = {
         runSQL: sinon.stub().rejects(new Error("warehouse went away")),
      } as unknown as MalloyConnection;
      const result = await record(() =>
         new DatabaseTier().checkKey({ connection }, CHECK, "t"),
      );
      expect(result).toEqual({ recorded: undefined, warned: ["did not run"] });
   });

   it("reads as unknown on the storage tier, and the build goes on", async () => {
      // A destination the check cannot reach: its session is never opened.
      const site = {
         destinationName: "lake",
         destinationConnection: { name: "lake", type: "postgres" },
         environmentPath: "/tmp/env",
      } as Parameters<StorageBuildStrategy["checkKey"]>[0];
      await expect(
         new StorageBuildStrategy().checkKey(site, CHECK, "t"),
      ).rejects.toThrow();
      const result = await record(() =>
         new StorageBuildStrategy().checkKey(site, CHECK, "t"),
      );
      expect(result).toEqual({ recorded: undefined, warned: ["did not run"] });
   });

   it("is told from a check that ran and found the key repeated", async () => {
      const found = {
         table: "t",
         keyColumn: "id",
         digest: "d",
         unique: false,
      };
      const result = await record(async () => found);
      expect(result.warned).toEqual(["not unique"]);
      expect(result.recorded).toMatchObject(found);
   });
});

describe("publisher_materialization_key_check_total", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   it("counts each check once, by its kind and what it found", async () => {
      const found = { table: "t", keyColumn: "id", digest: "d" };
      await record(async () => ({ ...found, unique: true }));
      await record(async () => ({ ...found, unique: false }));
      await record(async () => undefined);
      await record(async () => {
         throw new Error("warehouse went away");
      });
      const count = (outcome: string) =>
         harness.collectCounter("publisher_materialization_key_check_total", {
            kind: "rollup",
            outcome,
         });
      expect(await count("unique")).toBe(1);
      expect(await count("not_unique")).toBe(1);
      expect(await count("not_run")).toBe(2);
   });
});
