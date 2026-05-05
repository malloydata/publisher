import type { Connection } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   EnvironmentNotFoundError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import {
   ManifestEntry,
   Materialization,
   MaterializationStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { EnvironmentStore } from "./environment_store";
import { ManifestService } from "./manifest_service";
import {
   manifestTableKey,
   MaterializationService,
   tablePhysicallyExists,
} from "./materialization_service";

function makeExecution(
   overrides: Partial<Materialization> = {},
): Materialization {
   return {
      id: "exec-1",
      environmentId: "proj-1",
      packageName: "pkg",
      status: "PENDING",
      startedAt: null,
      completedAt: null,
      error: null,
      metadata: null,
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
      ...overrides,
   };
}

type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

function createMocks() {
   const sandbox = sinon.createSandbox();

   const repository: MockRepo = {
      listEnvironments: sandbox.stub(),
      getEnvironmentById: sandbox.stub(),
      getEnvironmentByName: sandbox.stub(),
      createEnvironment: sandbox.stub(),
      updateEnvironment: sandbox.stub(),
      deleteEnvironment: sandbox.stub(),
      listPackages: sandbox.stub(),
      getPackageById: sandbox.stub(),
      getPackageByName: sandbox.stub(),
      createPackage: sandbox.stub(),
      updatePackage: sandbox.stub(),
      deletePackage: sandbox.stub(),
      listConnections: sandbox.stub(),
      getConnectionById: sandbox.stub(),
      getConnectionByName: sandbox.stub(),
      createConnection: sandbox.stub(),
      updateConnection: sandbox.stub(),
      deleteConnection: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
      listManifestEntries: sandbox.stub(),
      upsertManifestEntry: sandbox.stub(),
      deleteManifestEntry: sandbox.stub(),
   } as unknown as MockRepo;

   const storageManager = {
      getRepository: () => repository,
      getManifestStore: sandbox.stub(),
   };

   const environmentStore = {
      storageManager,
      getEnvironment: sandbox.stub(),
   } as unknown as EnvironmentStore;

   const manifestService = {
      getManifest: sandbox.stub().resolves({ entries: {}, strict: false }),
      writeEntry: sandbox.stub().resolves(),
      deleteEntry: sandbox.stub().resolves(),
      reloadManifest: sandbox.stub().resolves({ entries: {}, strict: false }),
      listEntries: sandbox.stub().resolves([]),
   } as unknown as sinon.SinonStubbedInstance<ManifestService>;

   const service = new MaterializationService(
      environmentStore,
      manifestService as unknown as ManifestService,
   );

   // Default: resolveEnvironmentId succeeds
   repository.getEnvironmentByName.resolves({
      id: "proj-1",
      name: "my-environment",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });

   return { sandbox, repository, environmentStore, manifestService, service };
}

describe("MaterializationService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   // ==================== resolveEnvironmentId ====================

   describe("resolveEnvironmentId (via listMaterializations)", () => {
      it("should throw EnvironmentNotFoundError when environment is not in DB", async () => {
         ctx.repository.getEnvironmentByName.resolves(null);

         await expect(
            ctx.service.listMaterializations("unknown", "pkg"),
         ).rejects.toThrow(EnvironmentNotFoundError);
      });
   });

   // ==================== BUILD QUERIES ====================

   describe("listMaterializations", () => {
      it("should list builds for a package", async () => {
         const builds = [makeExecution(), makeExecution({ id: "exec-2" })];
         ctx.repository.listMaterializations.resolves(builds);

         const result = await ctx.service.listMaterializations(
            "my-environment",
            "pkg",
         );

         expect(result).toEqual(builds);
      });
   });

   describe("getMaterialization", () => {
      it("should return a specific build", async () => {
         const exec = makeExecution();
         ctx.repository.getMaterializationById.resolves(exec);

         const result = await ctx.service.getMaterialization(
            "my-environment",
            "pkg",
            "exec-1",
         );

         expect(result).toEqual(exec);
      });

      it("should throw when build not found", async () => {
         ctx.repository.getMaterializationById.resolves(null);

         await expect(
            ctx.service.getMaterialization("my-environment", "pkg", "missing"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });

      it("should throw when build belongs to a different package", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeExecution({ packageName: "other-pkg" }),
         );

         await expect(
            ctx.service.getMaterialization("my-environment", "pkg", "exec-1"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });

   // ==================== STATE MACHINE ====================

   describe("state transitions", () => {
      const validTransitions: [MaterializationStatus, MaterializationStatus][] =
         [
            ["PENDING", "RUNNING"],
            ["PENDING", "CANCELLED"],
            ["RUNNING", "SUCCESS"],
            ["RUNNING", "FAILED"],
            ["RUNNING", "CANCELLED"],
         ];

      const invalidTransitions: [
         MaterializationStatus,
         MaterializationStatus,
      ][] = [
         ["PENDING", "SUCCESS"],
         ["PENDING", "FAILED"],
         ["RUNNING", "PENDING"],
         ["SUCCESS", "RUNNING"],
         ["SUCCESS", "FAILED"],
         ["FAILED", "RUNNING"],
         ["FAILED", "SUCCESS"],
         ["CANCELLED", "RUNNING"],
         ["CANCELLED", "SUCCESS"],
      ];

      for (const [from, to] of validTransitions) {
         it(`should allow ${from} -> ${to}`, async () => {
            const exec = makeExecution({ status: from });
            ctx.repository.getMaterializationById.resolves(exec);
            ctx.repository.updateMaterialization.resolves(
               makeExecution({ status: to }),
            );

            // Trigger via stopMaterialization for RUNNING->CANCELLED (orphaned path)
            if (from === "RUNNING" && to === "CANCELLED") {
               const result = await ctx.service.stopMaterialization(
                  "my-environment",
                  "pkg",
                  exec.id,
               );
               expect(result).not.toBeNull();
            }
         });
      }

      for (const [from, to] of invalidTransitions) {
         it(`should reject ${from} -> ${to}`, async () => {
            const exec = makeExecution({ status: from });
            ctx.repository.getMaterializationById.resolves(exec);

            await expect(
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               (ctx.service as any).transitionExecution(exec.id, to),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
   });

   // ==================== CREATE / START / STOP ====================

   describe("createMaterialization", () => {
      it("should create a PENDING build", async () => {
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeExecution({
            status: "PENDING",
            metadata: { forceRefresh: false, autoLoadManifest: true },
         });
         ctx.repository.createMaterialization.resolves(pending);

         const result = await ctx.service.createMaterialization(
            "my-environment",
            "pkg",
            {
               autoLoadManifest: true,
            },
         );

         expect(result.status).toBe("PENDING");
         expect(
            (result.metadata as Record<string, unknown>)?.autoLoadManifest,
         ).toBe(true);
         // Options persist on the INSERT itself; no follow-up update.
         expect(ctx.repository.createMaterialization.calledOnce).toBe(true);
         expect(ctx.repository.createMaterialization.firstCall.args).toEqual([
            "proj-1",
            "pkg",
            "PENDING",
            { forceRefresh: false, autoLoadManifest: true },
         ]);
         expect(ctx.repository.updateMaterialization.called).toBe(false);
      });

      it("should reject creation when an active materialization exists", async () => {
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         ctx.repository.getActiveMaterialization.resolves(
            makeExecution({ id: "existing", status: "RUNNING" }),
         );

         await expect(
            ctx.service.createMaterialization("my-environment", "pkg"),
         ).rejects.toThrow(MaterializationConflictError);
      });

      it("should translate DuplicateActiveMaterializationError from a lost race", async () => {
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         // The pre-check finds nothing (race is still possible), but the
         // atomic insert loses to a concurrent create and the repository
         // raises DuplicateActiveMaterializationError.
         ctx.repository.getActiveMaterialization
            .onFirstCall()
            .resolves(null)
            .onSecondCall()
            .resolves(makeExecution({ id: "winner", status: "PENDING" }));
         ctx.repository.createMaterialization.rejects(
            new DuplicateActiveMaterializationError("proj-1", "pkg"),
         );

         await expect(
            ctx.service.createMaterialization("my-environment", "pkg"),
         ).rejects.toThrow(/winner/);
      });
   });

   describe("startMaterialization", () => {
      it("should transition PENDING to RUNNING", async () => {
         const pending = makeExecution({
            status: "PENDING",
            metadata: { forceRefresh: false, autoLoadManifest: false },
         });
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(pending);
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.updateMaterialization.resolves(running);

         const result = await ctx.service.startMaterialization(
            "my-environment",
            "pkg",
            "exec-1",
         );

         expect(result.status).toBe("RUNNING");
      });

      it("should reject non-PENDING builds", async () => {
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(running);

         await expect(
            ctx.service.startMaterialization("my-environment", "pkg", "exec-1"),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("should throw MaterializationConflictError when another is already running", async () => {
         const pending = makeExecution({
            status: "PENDING",
            metadata: { forceRefresh: false, autoLoadManifest: false },
         });
         ctx.repository.getMaterializationById.resolves(pending);
         ctx.repository.getActiveMaterialization.resolves(
            makeExecution({ id: "other", status: "RUNNING" }),
         );

         await expect(
            ctx.service.startMaterialization("my-environment", "pkg", "exec-1"),
         ).rejects.toThrow(MaterializationConflictError);
      });
   });

   describe("stopMaterialization", () => {
      it("should cancel a PENDING build", async () => {
         const pending = makeExecution({ status: "PENDING" });
         ctx.repository.getMaterializationById.resolves(pending);
         ctx.repository.updateMaterialization.resolves(
            makeExecution({ status: "CANCELLED" }),
         );

         const result = await ctx.service.stopMaterialization(
            "my-environment",
            "pkg",
            "exec-1",
         );

         expect(result.status).toBe("CANCELLED");
      });

      it("should force-cancel an orphaned RUNNING build", async () => {
         const running = makeExecution({ id: "orphan", status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(running);
         ctx.repository.updateMaterialization.resolves(
            makeExecution({ id: "orphan", status: "CANCELLED" }),
         );

         const result = await ctx.service.stopMaterialization(
            "my-environment",
            "pkg",
            "orphan",
         );

         expect(result.status).toBe("CANCELLED");
      });

      it("should reject stopping a terminal build", async () => {
         const succeeded = makeExecution({ status: "SUCCESS" });
         ctx.repository.getMaterializationById.resolves(succeeded);

         await expect(
            ctx.service.stopMaterialization("my-environment", "pkg", "exec-1"),
         ).rejects.toThrow(InvalidStateTransitionError);
      });
   });

   // ==================== DELETE ====================

   describe("deleteMaterialization", () => {
      it("should delete a SUCCESS materialization", async () => {
         const succeeded = makeExecution({ status: "SUCCESS" });
         ctx.repository.getMaterializationById.resolves(succeeded);
         ctx.repository.deleteMaterialization.resolves();

         await ctx.service.deleteMaterialization(
            "my-environment",
            "pkg",
            "exec-1",
         );

         expect(ctx.repository.deleteMaterialization.calledOnce).toBe(true);
         expect(ctx.repository.deleteMaterialization.firstCall.args[0]).toBe(
            "exec-1",
         );
      });

      it("should delete a FAILED materialization", async () => {
         const failed = makeExecution({ status: "FAILED" });
         ctx.repository.getMaterializationById.resolves(failed);
         ctx.repository.deleteMaterialization.resolves();

         await ctx.service.deleteMaterialization(
            "my-environment",
            "pkg",
            "exec-1",
         );

         expect(ctx.repository.deleteMaterialization.calledOnce).toBe(true);
      });

      it("should delete a CANCELLED materialization", async () => {
         const cancelled = makeExecution({ status: "CANCELLED" });
         ctx.repository.getMaterializationById.resolves(cancelled);
         ctx.repository.deleteMaterialization.resolves();

         await ctx.service.deleteMaterialization(
            "my-environment",
            "pkg",
            "exec-1",
         );

         expect(ctx.repository.deleteMaterialization.calledOnce).toBe(true);
      });

      it("should reject deleting a PENDING materialization", async () => {
         const pending = makeExecution({ status: "PENDING" });
         ctx.repository.getMaterializationById.resolves(pending);

         await expect(
            ctx.service.deleteMaterialization(
               "my-environment",
               "pkg",
               "exec-1",
            ),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("should reject deleting a RUNNING materialization", async () => {
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(running);

         await expect(
            ctx.service.deleteMaterialization(
               "my-environment",
               "pkg",
               "exec-1",
            ),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("should throw when materialization not found", async () => {
         ctx.repository.getMaterializationById.resolves(null);

         await expect(
            ctx.service.deleteMaterialization(
               "my-environment",
               "pkg",
               "missing",
            ),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });

   // ==================== TEARDOWN ====================

   describe("teardownPackage", () => {
      it("refuses to run while a materialization is active", async () => {
         ctx.repository.getActiveMaterialization.resolves(
            makeExecution({ status: "RUNNING" }),
         );

         await expect(
            ctx.service.teardownPackage("my-environment", "pkg"),
         ).rejects.toThrow(MaterializationConflictError);
      });

      it("drops every manifest entry for the package and deletes its row", async () => {
         const runSQL = sinon.stub().resolves();
         const connection = {
            dialectName: "duckdb",
            runSQL,
         } as unknown as Connection;
         const pkg = {
            getMalloyConnection: async (name: string): Promise<Connection> => {
               if (name === "conn") return connection;
               throw new Error(`unknown connection: ${name}`);
            },
         };
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves(pkg),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         const entries: ManifestEntry[] = [
            {
               id: "entry-1",
               environmentId: "proj-1",
               packageName: "pkg",
               buildId: "abcdef1234567890abcdef1234567890",
               tableName: "table_a",
               sourceName: "src",
               connectionName: "conn",
               createdAt: new Date(),
               updatedAt: new Date(),
            },
            {
               id: "entry-2",
               environmentId: "proj-1",
               packageName: "pkg",
               buildId: "1234567890abcdef1234567890abcdef",
               tableName: "table_b",
               sourceName: "src",
               connectionName: "conn",
               createdAt: new Date(),
               updatedAt: new Date(),
            },
         ];
         (ctx.manifestService.listEntries as sinon.SinonStub).resolves(entries);

         const result = await ctx.service.teardownPackage(
            "my-environment",
            "pkg",
         );

         expect(result.dropped).toHaveLength(2);
         expect(result.errors).toHaveLength(0);
         // Each entry triggers target DROP + staging DROP = 2 calls per entry.
         expect(runSQL.callCount).toBe(4);
         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).callCount,
         ).toBe(2);
      });

      it("deletes rows whose connection is no longer registered (teardown force-delete)", async () => {
         const runSQL = sinon.stub().resolves();
         const livingConn = {
            dialectName: "duckdb",
            runSQL,
         } as unknown as Connection;
         // Only "live_conn" is registered; the manifest row below points at
         // a vanished "ghost_conn", which used to be impossible to tear down.
         // `teardownPackage` must force-delete the row anyway so teardown
         // can complete.
         const pkg = {
            getMalloyConnection: async (name: string): Promise<Connection> => {
               if (name === "live_conn") return livingConn;
               throw new Error(`unknown connection: ${name}`);
            },
         };
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves(pkg),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         const entries: ManifestEntry[] = [
            {
               id: "entry-ghost",
               environmentId: "proj-1",
               packageName: "pkg",
               buildId: "abcdef1234567890abcdef1234567890",
               tableName: "table_ghost",
               sourceName: "src",
               connectionName: "ghost_conn",
               createdAt: new Date(),
               updatedAt: new Date(),
            },
         ];
         (ctx.manifestService.listEntries as sinon.SinonStub).resolves(entries);

         const result = await ctx.service.teardownPackage(
            "my-environment",
            "pkg",
         );

         expect(result.dropped).toHaveLength(1);
         expect(result.dropped[0].targetDropSkipped).toBe(true);
         expect(result.errors).toHaveLength(0);
         // No DROP on the vanished connection, but the manifest row is gone.
         expect(runSQL.called).toBe(false);
         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).calledOnce,
         ).toBe(true);
      });

      it("dryRun reports would-drop entries without running DROP or deleting rows", async () => {
         const runSQL = sinon.stub().resolves();
         const connection = {
            dialectName: "duckdb",
            runSQL,
         } as unknown as Connection;
         const pkg = {
            getMalloyConnection: async (name: string): Promise<Connection> => {
               if (name === "conn") return connection;
               throw new Error(`unknown connection: ${name}`);
            },
         };
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves(pkg),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         const entry: ManifestEntry = {
            id: "entry-1",
            environmentId: "proj-1",
            packageName: "pkg",
            buildId: "abcdef1234567890abcdef1234567890",
            tableName: "orphan",
            sourceName: "src",
            connectionName: "conn",
            createdAt: new Date(),
            updatedAt: new Date(),
         };
         (ctx.manifestService.listEntries as sinon.SinonStub).resolves([entry]);

         const result = await ctx.service.teardownPackage(
            "my-environment",
            "pkg",
            {
               dryRun: true,
            },
         );

         expect(result.dropped).toHaveLength(1);
         expect(result.dropped[0].tableName).toBe("orphan");
         expect(runSQL.called).toBe(false);
         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).called,
         ).toBe(false);
      });
   });
});

// ==================== HELPER UNIT TESTS ====================

describe("manifestTableKey", () => {
   it("returns connectionName::tableName", () => {
      expect(manifestTableKey("duckdb", "orders")).toBe("duckdb::orders");
   });

   it("handles schema-qualified table names", () => {
      expect(manifestTableKey("pg", "analytics.summary")).toBe(
         "pg::analytics.summary",
      );
   });
});

describe("tablePhysicallyExists", () => {
   it("returns true when SELECT succeeds", async () => {
      const conn = {
         runSQL: sinon.stub().resolves({ rows: [] }),
      } as unknown as Connection;

      expect(await tablePhysicallyExists(conn, '"my_table"')).toBe(true);
      expect((conn.runSQL as sinon.SinonStub).calledOnce).toBe(true);
      expect((conn.runSQL as sinon.SinonStub).firstCall.args[0]).toBe(
         'SELECT 1 FROM "my_table" WHERE 1=0',
      );
   });

   it("returns false when SELECT throws (table does not exist)", async () => {
      const conn = {
         runSQL: sinon.stub().rejects(new Error("table not found")),
      } as unknown as Connection;

      expect(await tablePhysicallyExists(conn, '"missing"')).toBe(false);
   });
});
