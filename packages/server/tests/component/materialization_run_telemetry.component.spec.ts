// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as sinon from "sinon";
import { logger } from "../../src/logger";
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../src/test_helpers/metrics_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

/**
 * The run and build telemetry of a real materialization run (plan section 5,
 * "Step 4 specification"): a package with one source that builds and one
 * whose build fails, run through the service against a real DuckDB, twice.
 * Asserts the counters, the histogram and the structured events, and that
 * nothing a build reads or writes reaches a log: the failing source's SQL
 * and the value its error quotes are planted to be found.
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
// Planted in the failing source's SQL and in the error the warehouse raises,
// so a log field that carries either is found.
const PLANTED = "PLANTED_ROW_VALUE_4242";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const MODEL = [
   "##! experimental.persistence",
   `source: base is warehouse.sql("SELECT 'a' AS who, 1 AS k")`,
   "#@ persist",
   "source: good is base -> { group_by: who }",
   // Compiles (the schema read never evaluates error()), fails at the CTAS
   // with a message that quotes the planted value.
   `source: bad_base is warehouse.sql("SELECT error('${PLANTED}')::VARCHAR AS who")`,
   "#@ persist",
   "source: bad is bad_base -> { group_by: who }",
   "",
].join("\n");

type LogCall = {
   level: string;
   message: string;
   meta: Record<string, unknown>;
};

describe("materialization run telemetry, through a real build", () => {
   let root: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let harness: MetricsHarness;
   let restoreSettings: () => void;
   let logs: LogCall[];
   const stubs: sinon.SinonStub[] = [];

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "mat-run-telemetry-")),
      );
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage(PKG);
      service = new MaterializationService({
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore);

      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      logs = [];
      for (const level of ["debug", "info", "warn", "error"] as const) {
         stubs.push(
            sinon.stub(logger, level).callsFake(((
               message: unknown,
               meta?: unknown,
            ) => {
               logs.push({
                  level,
                  message: String(message),
                  meta: (meta ?? {}) as Record<string, unknown>,
               });
               return logger;
            }) as never),
         );
      }
   });

   afterEach(async () => {
      for (const stub of stubs.splice(0)) stub.restore();
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
      await env.closeAllConnections().catch(() => undefined);
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
      restoreSettings();
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
      const deadline = Date.now() + 60_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   const events = (name: string) => logs.filter((l) => l.message === name);
   const builds = (labels: Record<string, string>) =>
      harness.collectCounter("publisher_materialization_build_total", labels);

   it("counts, times and logs each source once per run, and logs no SQL or value", async () => {
      const first = await build();
      expect(first.status).toBe("MANIFEST_FILE_READY");

      // One built, one failed: counted once each, by kind and tier.
      expect(
         await builds({
            kind: "persist",
            tier: "colocated",
            outcome: "built",
            failure_class: "none",
         }),
      ).toBe(1);
      expect(
         await builds({
            kind: "persist",
            tier: "colocated",
            outcome: "failed",
            failure_class: "unclassified",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_materialization_build_total"),
      ).toBe(2);
      // Both builds ran, so both are timed.
      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_duration_ms",
               { kind: "persist", tier: "colocated", method: "full" },
            )
         ).count,
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_materialization_runs_total", {
            mode: "auto",
            outcome: "partial",
         }),
      ).toBe(1);

      const [started] = events("materialization.run.started");
      expect(started).toMatchObject({
         level: "info",
         meta: {
            runId: first.id,
            mode: "auto",
            trigger: "api",
            sourceCount: 2,
         },
      });
      const finished = events("materialization.build.finished");
      expect(finished).toHaveLength(1);
      expect(finished[0]).toMatchObject({
         level: "info",
         meta: {
            runId: first.id,
            kind: "persist",
            tier: "colocated",
            method: "full",
            sourceName: "good",
         },
      });
      expect(finished[0].meta.sourceEntityId).toEqual(expect.any(String));
      expect(finished[0].meta.durationMs).toEqual(expect.any(Number));
      expect(finished[0].meta.physicalTable).toEqual(expect.any(String));
      const failed = events("materialization.build.failed");
      expect(failed).toHaveLength(1);
      expect(failed[0]).toMatchObject({
         // Nothing recognized the failure, so it is logged as an error.
         level: "error",
         meta: {
            runId: first.id,
            sourceName: "bad",
            outcome: "failed",
            failureClass: "unclassified",
         },
      });
      expect(failed[0].meta.code).toMatch(/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/);
      const runFinished = events("materialization.run.finished");
      expect(runFinished).toHaveLength(1);
      expect(runFinished[0]).toMatchObject({
         level: "warn",
         meta: {
            runId: first.id,
            outcome: "partial",
            built: 1,
            reused: 0,
            refused: 0,
            failed: 1,
         },
      });

      // The second run carries the built table forward and fails the bad
      // one again: with nothing built, the run fails. A failed run commits
      // nothing, so the carried table is not counted as reused.
      const second = await build();
      expect(second.status).toBe("FAILED");
      expect(await builds({ outcome: "failed" })).toBe(2);
      expect(await builds({ outcome: "reused" })).toBe(0);
      expect(
         await harness.collectCounter("publisher_materialization_runs_total", {
            mode: "auto",
            outcome: "failed",
         }),
      ).toBe(1);
      const secondFinished = events("materialization.run.finished").at(-1);
      expect(secondFinished).toMatchObject({
         level: "error",
         meta: {
            runId: second.id,
            outcome: "failed",
            built: 0,
            failed: 1,
            code: "BuildRunFailedError",
         },
      });
      // Reported once: the background runner does not log it again.
      expect(
         logs.filter((l) => l.message === "Materialization run failed"),
      ).toHaveLength(0);

      // The record keeps the reason; no log carries the SQL or the value.
      expect(JSON.stringify(first.manifest ?? first)).toContain(PLANTED);
      for (const call of logs) {
         const text = JSON.stringify([call.message, call.meta]);
         expect(text).not.toContain(PLANTED);
         expect(text.toUpperCase()).not.toContain("SELECT ");
      }
   });
});
