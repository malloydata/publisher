// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { InvalidStateTransitionError } from "../../../errors";
import { makeMaterialization } from "../../../test_helpers/materialization_test_fixtures";
import {
   buildEnvironmentStub,
   createMocks,
   UNVERSIONED_ENVIRONMENT,
   withQueryMetadataOn,
} from "../../../test_helpers/materialization_service_harness";
import { MaterializationService } from "./materialization_service";
import { isReclaimableStorageTable } from "./materialization_run_support";
import { Reclaimer } from "./reclaimer";
import { logger } from "../../../logger";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import { installSettings } from "../../../test_helpers/settings_env";

/**
 * Delete and cleanup: deleteMaterialization (state guards, record-only,
 * serve-binding rebinds), the reclaim's table drop and its telemetry, and the
 * reclaim of storage tables a failed run left behind.
 */

describe("deleteMaterialization", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   describe("deleteMaterialization rebinds serve bindings (both tiers)", () => {
      const storageEntry = {
         se1: {
            sourceEntityId: "se1",
            sourceName: "daily",
            physicalTableName: "daily__mabc",
            connectionName: "wh",
            storageDestinationName: "lake",
            schema: [{ name: "a", type: "BIGINT" }],
         },
      };
      // A colocated entry carries NO storageDestinationName.
      const colocatedEntry = {
         ce1: {
            sourceEntityId: "ce1",
            sourceName: "daily",
            physicalTableName: "daily__cabc",
            connectionName: "wh",
         },
      };

      function setupEnv(): {
         storageSpy: sinon.SinonStub;
         colocatedSpy: sinon.SinonStub;
      } {
         const storageSpy = sinon.stub().resolves();
         const colocatedSpy = sinon.stub().resolves();
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            ...UNVERSIONED_ENVIRONMENT,
            getPackage: sinon
               .stub()
               .resolves({ getMalloyConnection: async () => ({}) }),
            getApiConnection: () => ({ name: "lake", type: "duckdb" }),
            getEnvironmentName: () => "env",
            getEnvironmentPath: () => "/test",
            // What the rebind read binds: colocated always, storage= when
            // the read returns it.
            rebindPackageFromStore: async (
               packageName: string,
               read: () => Promise<{
                  colocated: unknown;
                  storage?: unknown;
               }>,
            ) => {
               const { colocated, storage } = await read();
               await colocatedSpy(packageName, colocated);
               if (storage !== undefined)
                  await storageSpy(packageName, storage);
            },
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               manifest: { entries: {} } as any,
            }),
         );
         return { storageSpy, colocatedSpy };
      }

      it("re-derives storage bindings from the next-latest materialization (on)", async () => {
         const { restore } = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         });
         try {
            const { storageSpy, colocatedSpy } = setupEnv();
            // The latest REMAINING successful run carries a storage entry.
            ctx.repository.listMaterializations.resolves([
               makeMaterialization({
                  id: "m-prev",
                  status: "MANIFEST_FILE_READY",
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  manifest: { entries: storageEntry } as any,
               }),
            ]);

            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

            expect(storageSpy.calledOnce).toBe(true);
            expect(Object.keys(storageSpy.firstCall.args[1])).toContain("se1");
            // Colocated is re-derived too (with no colocated entries ⇒ cleared).
            expect(colocatedSpy.calledOnce).toBe(true);
            expect(colocatedSpy.firstCall.args[1]).toEqual({});
         } finally {
            restore();
         }
      });

      it("re-derives colocated bindings regardless of tier mode (off)", async () => {
         // PERSIST_STORAGE_MODE unset ⇒ off. Colocated is the v0 path, not gated
         // by the storage kill switch, so it is still re-derived; storage is not.
         const { storageSpy, colocatedSpy } = setupEnv();
         ctx.repository.listMaterializations.resolves([
            makeMaterialization({
               id: "m-prev",
               status: "MANIFEST_FILE_READY",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               manifest: { entries: colocatedEntry } as any,
            }),
         ]);

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

         expect(colocatedSpy.calledOnce).toBe(true);
         expect(Object.keys(colocatedSpy.firstCall.args[1])).toContain("ce1");
         // Storage rebind is skipped when the tier is off.
         expect(storageSpy.called).toBe(false);
      });

      it("clears both tiers' bindings when no successful materialization remains (on)", async () => {
         const { restore } = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         });
         try {
            const { storageSpy, colocatedSpy } = setupEnv();
            ctx.repository.listMaterializations.resolves([]); // none remain

            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

            expect(storageSpy.calledOnce).toBe(true);
            expect(storageSpy.firstCall.args[1]).toEqual({}); // empty ⇒ serve live
            expect(colocatedSpy.calledOnce).toBe(true);
            expect(colocatedSpy.firstCall.args[1]).toEqual({});
         } finally {
            restore();
         }
      });

      it("skips only the storage rebind when the tier is off (dark-ship)", async () => {
         const { storageSpy, colocatedSpy } = setupEnv(); // off
         ctx.repository.listMaterializations.resolves([]); // none remain
         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");
         // Storage is skipped when off; colocated still re-derives (to clear).
         expect(storageSpy.called).toBe(false);
         expect(colocatedSpy.calledOnce).toBe(true);
         expect(colocatedSpy.firstCall.args[1]).toEqual({});
      });
   });

   describe("deleteMaterialization", () => {
      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"] as const;
      for (const status of terminal) {
         it(`deletes a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");
            expect(
               ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
            ).toBe(true);
         });
      }

      const active = ["PENDING", "MANIFEST_ROWS_READY"] as const;
      for (const status of active) {
         it(`rejects deleting a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await expect(
               ctx.service.deleteMaterialization("my-env", "pkg", "mat-1"),
            ).rejects.toThrow(InvalidStateTransitionError);
            expect(ctx.repository.deleteMaterialization.called).toBe(false);
         });
      }

      it("deletes the record only, never dropping a table", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon.stub().resolves({ runSQL });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

         expect(getMalloyConnection.called).toBe(false);
         expect(runSQL.called).toBe(false);
         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });
   });
});

/**
 * The drop a reclaim makes (an archived or deleted version's tables, a table
 * a rename replaced), called directly: a run's delete never drops a table.
 */
describe("dropManifestEntries", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   const ORDERS = {
      sourceEntityId: "b1",
      physicalTableName: "orders_mz",
      connectionName: "duckdb",
   };

   function reclaimer(): Reclaimer {
      return new Reclaimer({
         repository: () => ctx.repository,
         host: {} as never,
         lifecycle: {} as never,
         dropManifestEntries: async () => [],
      });
   }

   function drop(
      runSQL: sinon.SinonStub,
      entries: Parameters<Reclaimer["dropManifestEntries"]>[5],
      dialectName = "duckdb",
      environment: unknown = { getApiConnection: () => undefined },
   ) {
      return reclaimer().dropManifestEntries(
         environment as never,
         "my-env",
         "pkg",
         "mat-1",
         "reclaim",
         entries,
         new Set(),
         async () => ({ runSQL, dialectName }) as never,
      );
   }

   it("routes a storage entry to a destination-aware RW drop, best-effort", async () => {
      // A non-storage destination type refuses the RW drop at its gate
      // (pre-session), exercising the routing and the best-effort failure
      // handling without a live DuckDB attach.
      const environment = buildEnvironmentStub({
         destinations: { lake: { name: "lake", type: "postgres" } },
         environmentPath: "/test",
      });
      const entry = {
         sourceEntityId: "se1",
         sourceName: "daily",
         physicalTableName: "daily__mabc123",
         connectionName: "wh",
         storageDestinationName: "lake",
      };

      const owed = await drop(sinon.stub().resolves(), [entry], "duckdb", {
         ...UNVERSIONED_ENVIRONMENT,
         ...environment,
      });

      expect(environment.getStorageDestination.calledWith("lake")).toBe(true);
      expect(owed).toEqual([entry]);
   });

   it("drops a manifest table and its staging", async () => {
      const runSQL = sinon.stub().resolves();
      expect(await drop(runSQL, [ORDERS])).toEqual([]);
      const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(dropped).toContain('DROP TABLE IF EXISTS "orders_mz"');
      expect(dropped.some((s) => s.includes('"orders_mz_'))).toBe(true);
   });

   it("dialect-quotes the drop DDL from the live connection (backtick + container path)", async () => {
      const runSQL = sinon.stub().resolves();
      // Container path on a hyphenated BigQuery project: each segment must
      // be backtick-quoted independently.
      await drop(
         runSQL,
         [
            {
               sourceEntityId: "b1",
               physicalTableName: "my-proj.ds.engaged",
               connectionName: "bq",
            },
         ],
         "standardsql",
      );
      const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(dropped).toContain(
         "DROP TABLE IF EXISTS `my-proj`.`ds`.`engaged`",
      );
   });

   it("returns a table whose drop failed as owed, and never throws", async () => {
      const owed = await drop(sinon.stub().rejects(new Error("boom")), [
         ORDERS,
      ]);
      expect(owed).toEqual([ORDERS]);
   });
});

describe("table drop telemetry", () => {
   let ctx: ReturnType<typeof createMocks>;
   let harness: MetricsHarness;
   withQueryMetadataOn();

   beforeEach(async () => {
      ctx = createMocks();
      harness = await startMetricsHarness();
      // Drop cached instruments so recordDropTables re-binds to this provider.
      resetMaterializationTelemetryForTesting();
   });

   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   const DROP_COUNTER = "publisher_materialization_drop_tables_total";

   function dropOrders(runSQL: sinon.SinonStub) {
      return new Reclaimer({
         repository: () => ctx.repository,
         host: {} as never,
         lifecycle: {} as never,
         dropManifestEntries: async () => [],
      }).dropManifestEntries(
         { getApiConnection: () => undefined } as never,
         "my-env",
         "pkg",
         "mat-1",
         "reclaim",
         [
            {
               sourceEntityId: "b1",
               physicalTableName: "orders_mz",
               connectionName: "duckdb",
            },
         ],
         new Set(),
         async () => ({ runSQL, dialectName: "duckdb" }) as never,
      );
   }

   it("meters a drop Publisher makes on its own as occasion=reclaim", async () => {
      const runSQL = sinon.stub().resolves();
      const reclaimer = new Reclaimer({
         repository: () => ctx.repository,
         host: {} as never,
         lifecycle: {} as never,
         dropManifestEntries: async () => [],
      });
      const owed = await reclaimer.dropManifestEntries(
         { getApiConnection: () => undefined } as never,
         "my-env",
         "pkg",
         "mat-1",
         "reclaim",
         [
            {
               sourceEntityId: "b1",
               physicalTableName: "orders_mz",
               connectionName: "duckdb",
            },
         ],
         new Set(),
         async () => ({ runSQL, dialectName: "duckdb" }) as never,
      );

      expect(owed).toEqual([]);
      expect(
         await harness.collectCounter(DROP_COUNTER, {
            outcome: "success",
            engine: "in_warehouse",
            occasion: "reclaim",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter(DROP_COUNTER, { occasion: "delete" }),
      ).toBe(0);
   });

   it("tags the retiring drops as ops work", async () => {
      // A drop is warehouse work someone accounts for later, and it is the one
      // statement an operator goes looking for after a table disappears.
      const runSQL = sinon.stub().resolves();
      await dropOrders(runSQL);

      expect(runSQL.callCount).toBeGreaterThan(0);
      for (const call of runSQL.getCalls()) {
         expect(call.args[1]?.queryMetadata).toMatchObject({
            class: "ops",
            environment: "my-env",
            package: "pkg",
            run_id: "mat-1",
         });
      }
   });

   it("meters a failed drop as outcome=failure", async () => {
      // The drop is best-effort: a failure is metered but never surfaces.
      await dropOrders(sinon.stub().rejects(new Error("drop boom")));

      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "failure" }),
      ).toBe(1);
   });
});

// The only path here that issues a DROP against a customer's warehouse, so its
// guards get assertions rather than a trace. Scenario 64 covers it end to end,
// but hammer does not run in CI — and the failure mode is silent data loss, not
// a broken build.
describe("isReclaimableStorageTable", () => {
   type Entry = Parameters<typeof isReclaimableStorageTable>[0];
   const entry = (over: Partial<Entry>) =>
      ({
         sourceEntityId: "eid",
         physicalTableName: "daily_v1",
         ...over,
      }) as Entry;

   it("reclaims a stored table from a source that is not refreshed incrementally", () => {
      // A fresh generational name nothing else knows about: exactly what the
      // reclaim exists for.
      expect(
         isReclaimableStorageTable(entry({ storageDestinationName: "lake" })),
      ).toBe(true);
   });

   it("never reclaims a table an incremental source owns, whatever this run did", () => {
      // Its name is stable across runs by contract, so it is not a fresh
      // generation — a prior manifest may bind it, and a refresh writes it in
      // place either way: a delta as DML, a re-seed as CREATE OR REPLACE. A later
      // source failing the run must not take the source off its stored table, and
      // `full` is as exposed as `delta` here.
      for (const refresh of ["delta", "none", "full"] as const) {
         expect(
            isReclaimableStorageTable(
               entry({ storageDestinationName: "lake", refresh }),
            ),
         ).toBe(false);
      }
   });

   it("ignores a colocated entry, which this sweep does not own", () => {
      expect(isReclaimableStorageTable(entry({}))).toBe(false);
      expect(isReclaimableStorageTable(entry({ refresh: "delta" }))).toBe(
         false,
      );
   });
});

describe("reclaimStorageTablesFromFailedRun", () => {
   let ctx: ReturnType<typeof createMocks>;
   let infoLog: sinon.SinonStub;
   let warnLog: sinon.SinonStub;

   beforeEach(() => {
      ctx = createMocks();
      infoLog = ctx.sandbox.stub(logger, "info");
      warnLog = ctx.sandbox.stub(logger, "warn");
   });
   afterEach(() => ctx.sandbox.restore());

   const entry = (table: string) =>
      ({
         sourceEntityId: `eid-${table}`,
         sourceName: "daily",
         physicalTableName: table,
         storageDestinationName: "lake",
      }) as unknown as Parameters<
         MaterializationService["collaborators"]["reclaimStorageTablesFromFailedRun"]
      >[0][number];

   /** A destination whose drop would fail loudly if one were ever attempted. */
   const environment = buildEnvironmentStub({
      destinations: { lake: { name: "lake", type: "duckdb" } },
      environmentPath: "/test",
   }) as unknown as Parameters<
      MaterializationService["collaborators"]["reclaimStorageTablesFromFailedRun"]
   >[1];

   const reclaim = (
      built: ReturnType<typeof entry>[],
      others: unknown[],
   ): Promise<void> => {
      (ctx.repository.listMaterializations as sinon.SinonStub).resolves(others);
      return (
         ctx.service.collaborators as unknown as {
            reclaimStorageTablesFromFailedRun: (
               b: unknown,
               e: unknown,
               o: unknown,
            ) => Promise<void>;
         }
      ).reclaimStorageTablesFromFailedRun(built, environment, {
         environmentId: "env-1",
         packageName: "pkg",
      });
   };

   const said = (stub: sinon.SinonStub, needle: RegExp) =>
      stub.getCalls().some((c) => needle.test(String(c.args[0])));

   it("never drops a table a live manifest still serves", async () => {
      // The carried-forward case: a previous successful run's manifest names the
      // same destination + table. Dropping it would delete a table that is being
      // served right now.
      await reclaim(
         [entry("daily_v1")],
         [
            {
               status: "MANIFEST_FILE_READY",
               manifest: {
                  entries: {
                     other: {
                        storageDestinationName: "lake",
                        physicalTableName: "daily_v1",
                     },
                  },
               },
            },
         ],
      );

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(true);
      expect(said(infoLog, /Dropped a materialized storage table/)).toBe(false);
      expect(said(warnLog, /Failed to drop a materialized storage table/)).toBe(
         false,
      );
   });

   it("never drops a table whose name a live manifest names through another destination", async () => {
      // A table's name does not say where it is: `lake_alias` may be another
      // name for the very DuckLake `lake` names, so a committed run that names
      // `daily_v1` through it may be serving this table. Judged by the name's
      // last segment, whatever the destination or container.
      await reclaim(
         [entry("daily_v1")],
         [
            {
               status: "MANIFEST_FILE_READY",
               manifest: {
                  entries: {
                     other: {
                        storageDestinationName: "lake_alias",
                        physicalTableName: "main.daily_v1",
                     },
                  },
               },
            },
         ],
      );

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(true);
      expect(said(infoLog, /Dropped a materialized storage table/)).toBe(false);
      expect(said(warnLog, /Failed to drop a materialized storage table/)).toBe(
         false,
      );
   });

   it("does attempt the drop when nothing references the table", async () => {
      // The inverse, so the test above is known to be the still-referenced check
      // doing the work rather than the reclaim being inert. No real destination
      // exists here, so the attempt surfaces as the drop-failure warning.
      await reclaim([entry("daily_v2")], []);

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(false);
      expect(
         said(warnLog, /Failed to drop a materialized storage table/) ||
            said(infoLog, /Dropped a materialized storage table/),
      ).toBe(true);
   });

   it("skips a table whose destination is no longer configured and keeps sweeping", async () => {
      // A destination can be un-registered between the build and the reclaim.
      // Aborting the sweep there would leave every table after it stranded too,
      // so the entry is skipped and the rest are still attempted.
      const withoutDestinations = buildEnvironmentStub({
         destinations: {},
         environmentPath: "/test",
      }) as unknown as Parameters<
         MaterializationService["collaborators"]["reclaimStorageTablesFromFailedRun"]
      >[1];
      (ctx.repository.listMaterializations as sinon.SinonStub).resolves([]);

      await (
         ctx.service.collaborators as unknown as {
            reclaimStorageTablesFromFailedRun: (
               b: unknown,
               e: unknown,
               o: unknown,
            ) => Promise<void>;
         }
      ).reclaimStorageTablesFromFailedRun(
         [entry("daily_v4"), entry("daily_v5")],
         withoutDestinations,
         { environmentId: "env-1", packageName: "pkg" },
      );

      expect(said(warnLog, /destination is no longer configured/)).toBe(true);
      // Both entries were considered, not just the one that tripped the skip.
      expect(
         warnLog
            .getCalls()
            .filter((c) => /no longer configured/.test(String(c.args[0]))),
      ).toHaveLength(2);
      // And the sweep did not report itself as having blown up.
      expect(said(warnLog, /did not complete/)).toBe(false);
   });

   it("ignores a superseded run's entries when deciding", async () => {
      // Only MANIFEST_FILE_READY runs count as live. A superseded run naming the
      // same table must not pin it forever.
      await reclaim(
         [entry("daily_v3")],
         [
            {
               status: "SUPERSEDED",
               manifest: {
                  entries: {
                     old: {
                        storageDestinationName: "lake",
                        physicalTableName: "daily_v3",
                     },
                  },
               },
            },
         ],
      );

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(false);
   });
});
