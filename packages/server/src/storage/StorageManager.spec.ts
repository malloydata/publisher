// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "../logger";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../test_helpers/metrics_harness";
import { DuckDBConnection } from "./duckdb/DuckDBConnection";
import { MaterializationRepository } from "./duckdb/MaterializationRepository";
import { StorageManager } from "./StorageManager";

/**
 * The startup sweep of interrupted materialization runs is housekeeping: when
 * it throws, the server must still start, with a usable store.
 */
describe("StorageManager.initialize when the interrupted-run sweep throws", () => {
   let dir: string;
   let manager: StorageManager | undefined;
   let harness: MetricsHarness;
   const restores: Array<() => void> = [];

   beforeEach(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "storage-manager-"));
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });

   afterEach(async () => {
      for (const restore of restores.splice(0)) restore();
      await manager?.close();
      manager = undefined;
      await harness.shutdown();
      resetInstrumentsForTesting();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   it("still initializes, logs the error and counts it", async () => {
      const sweep = spyOn(
         MaterializationRepository.prototype,
         "failInterrupted",
      ).mockImplementation(() =>
         Promise.reject(new Error("unparseable metadata row")),
      );
      restores.push(() => sweep.mockRestore());
      const error = spyOn(logger, "error");
      restores.push(() => error.mockRestore());

      manager = new StorageManager({
         type: "duckdb",
         duckdb: { path: path.join(dir, "publisher.db") },
      });
      await manager.initialize();

      expect(sweep).toHaveBeenCalledTimes(1);
      expect(manager.isInitialized()).toBe(true);
      // The store works.
      const repository = manager.getRepository();
      await repository.createEnvironment({
         name: "env",
         path: "/tmp/env",
         description: undefined,
         metadata: undefined,
      } as never);
      expect(
         (await repository.listEnvironments()).map((env) => env.name),
      ).toEqual(["env"]);

      const logged = (error.mock.calls as unknown[][]).find(([message]) =>
         String(message).includes("materialization runs a restart interrupted"),
      );
      expect(logged).toBeDefined();
      expect(JSON.stringify(logged?.[1])).toContain("unparseable metadata row");
      expect(
         await harness.collectCounter(
            "publisher_interrupted_run_sweep_failures_total",
         ),
      ).toBe(1);
   });

   it("closes the database it opened when the schema cannot be initialized", async () => {
      const schema = spyOn(
         DuckDBConnection.prototype,
         "isInitialized",
      ).mockImplementation(() => Promise.reject(new Error("schema broken")));
      restores.push(() => schema.mockRestore());
      const close = spyOn(DuckDBConnection.prototype, "close");
      restores.push(() => close.mockRestore());

      manager = new StorageManager({
         type: "duckdb",
         duckdb: { path: path.join(dir, "publisher.db") },
      });
      await expect(manager.initialize()).rejects.toThrow("schema broken");
      expect(manager.isInitialized()).toBe(false);
      expect(close).toHaveBeenCalledTimes(1);
   });
});
