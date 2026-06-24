import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   BadRequestError,
   EnvironmentNotFoundError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import {
   BuildInstruction,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { EnvironmentStore } from "./environment_store";
import {
   compiledWith,
   fakeSource,
   makeBuildPlan,
   makeInstruction,
   makeMaterialization,
} from "./materialization_test_fixtures";
import {
   MaterializationService,
   stagingSuffix,
} from "./materialization_service";

type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

function createMocks() {
   const sandbox = sinon.createSandbox();

   const repository = {
      getEnvironmentByName: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
   } as unknown as MockRepo;

   const storageManager = { getRepository: () => repository };

   const environmentStore = {
      storageManager,
      getEnvironment: sandbox.stub(),
   } as unknown as EnvironmentStore;

   // Default: environment resolves and yields a package whose build plan is
   // empty (no persist sources). Individual tests override getBuildPlan to
   // exercise the orchestrated path. getModelPaths()=>[] keeps the background
   // build from touching the Malloy runtime.
   repository.getEnvironmentByName.resolves({
      id: "env-1",
      name: "my-env",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });
   setPackage(environmentStore, { getBuildPlan: () => null });

   const service = new MaterializationService(environmentStore);

   return { sandbox, repository, environmentStore, service };
}

/** Point environmentStore.getEnvironment at a package with the given overrides. */
function setPackage(
   environmentStore: EnvironmentStore,
   pkgOverrides: Record<string, unknown>,
): void {
   const pkg = {
      getBuildPlan: () => null,
      getModelPaths: () => [],
      getPackagePath: () => "/test",
      getMalloyConfig: () => ({}),
      getMalloyConnection: async () => ({}),
      ...pkgOverrides,
   };
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      getPackage: sinon.stub().resolves(pkg),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) =>
         fn(),
      reloadAllModelsForPackage: sinon.stub().resolves(),
   });
}

describe("MaterializationService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   describe("resolveEnvironmentId", () => {
      it("throws EnvironmentNotFoundError when the environment is missing", async () => {
         ctx.repository.getEnvironmentByName.resolves(null);
         await expect(
            ctx.service.listMaterializations("unknown", "pkg"),
         ).rejects.toThrow(EnvironmentNotFoundError);
      });
   });

   describe("queries", () => {
      it("lists materializations for a package", async () => {
         const rows = [
            makeMaterialization(),
            makeMaterialization({ id: "m2" }),
         ];
         ctx.repository.listMaterializations.resolves(rows);
         expect(
            await ctx.service.listMaterializations("my-env", "pkg"),
         ).toEqual(rows);
      });

      it("returns a specific materialization", async () => {
         const m = makeMaterialization();
         ctx.repository.getMaterializationById.resolves(m);
         expect(
            await ctx.service.getMaterialization("my-env", "pkg", "mat-1"),
         ).toEqual(m);
      });

      it("throws when the materialization is missing", async () => {
         ctx.repository.getMaterializationById.resolves(null);
         await expect(
            ctx.service.getMaterialization("my-env", "pkg", "nope"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });

      it("throws when the materialization belongs to another package", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({ packageName: "other" }),
         );
         await expect(
            ctx.service.getMaterialization("my-env", "pkg", "mat-1"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });

   describe("createMaterialization (auto-run)", () => {
      it("creates a PENDING record and persists options with mode=auto", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeMaterialization({ status: "PENDING" });
         ctx.repository.createMaterialization.resolves(pending);

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            {
               forceRefresh: true,
               sourceNames: ["orders"],
            },
         );

         expect(result.status).toBe("PENDING");
         expect(ctx.repository.createMaterialization.firstCall.args).toEqual([
            "env-1",
            "pkg",
            "PENDING",
            {
               forceRefresh: true,
               sourceNames: ["orders"],
               mode: "auto",
            },
         ]);
      });

      it("defaults to auto mode in metadata", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         await ctx.service.createMaterialization("my-env", "pkg", {});

         expect(ctx.repository.createMaterialization.firstCall.args[3]).toEqual(
            {
               forceRefresh: false,
               sourceNames: null,
               mode: "auto",
            },
         );
      });

      it("rejects when an active materialization already exists", async () => {
         ctx.repository.getActiveMaterialization.resolves(
            makeMaterialization({
               id: "existing",
               status: "MANIFEST_ROWS_READY",
            }),
         );
         await expect(
            ctx.service.createMaterialization("my-env", "pkg"),
         ).rejects.toThrow(MaterializationConflictError);
      });

      it("translates a lost create race into a conflict", async () => {
         ctx.repository.getActiveMaterialization
            .onFirstCall()
            .resolves(null)
            .onSecondCall()
            .resolves(makeMaterialization({ id: "winner" }));
         ctx.repository.createMaterialization.rejects(
            new DuplicateActiveMaterializationError("env-1", "pkg"),
         );
         await expect(
            ctx.service.createMaterialization("my-env", "pkg"),
         ).rejects.toThrow(/winner/);
      });
   });

   describe("createMaterialization (orchestrated)", () => {
      it("creates a PENDING record with mode=orchestrated for valid instructions", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            { buildInstructions: [makeInstruction()] },
         );

         expect(result.status).toBe("PENDING");
         expect(
            ctx.repository.createMaterialization.firstCall.args[3],
         ).toMatchObject({ mode: "orchestrated" });
      });

      it("rejects instructions referencing an unknown buildId before creating", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [makeInstruction({ buildId: "ghost" })],
            }),
         ).rejects.toThrow(BadRequestError);
         expect(ctx.repository.createMaterialization.called).toBe(false);
      });

      it("rejects SNAPSHOT realization", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [
                  makeInstruction({ realization: "SNAPSHOT" }),
               ],
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("rejects buildInstructions when the package has no persist sources", async () => {
         setPackage(ctx.environmentStore, { getBuildPlan: () => null });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [makeInstruction()],
            }),
         ).rejects.toThrow(BadRequestError);
      });
   });

   describe("stopMaterialization", () => {
      const cancellable = ["PENDING", "MANIFEST_ROWS_READY"] as const;

      for (const status of cancellable) {
         it(`cancels a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            ctx.repository.updateMaterialization.resolves(
               makeMaterialization({ status: "CANCELLED" }),
            );
            const result = await ctx.service.stopMaterialization(
               "my-env",
               "pkg",
               "mat-1",
            );
            expect(result.status).toBe("CANCELLED");
         });
      }

      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"] as const;
      for (const status of terminal) {
         it(`rejects stopping a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await expect(
               ctx.service.stopMaterialization("my-env", "pkg", "mat-1"),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
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

      it("does not drop tables by default (record-only delete)", async () => {
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
                        buildId: "b1",
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

      it("drops manifest tables (and staging) when dropTables is set", async () => {
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
                        buildId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         expect(getMalloyConnection.calledOnceWith("duckdb")).toBe(true);
         const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
         expect(dropped).toContain("DROP TABLE IF EXISTS orders_mz");
         expect(dropped.some((s) => s.includes("orders_mz_"))).toBe(true);
         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });

      it("still deletes the record when a table drop fails", async () => {
         const runSQL = sinon.stub().rejects(new Error("boom"));
         const getMalloyConnection = sinon.stub().resolves({ runSQL });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "FAILED",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        buildId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });
   });
});

describe("stagingSuffix", () => {
   it("derives a short, stable suffix from the buildId", () => {
      expect(stagingSuffix("abcdef1234567890")).toBe("_abcdef123456");
   });
});

describe("deriveSelfInstructions", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("carries forward unchanged buildIds and builds the rest (deduping repeats)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", buildId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", buildId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"], ["s2"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            buildId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1_prev",
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries);

      expect(instructions).toHaveLength(1);
      expect(instructions[0].buildId).toBe("b2bbbbbbbbbbbbbb");
      expect(instructions[0].physicalTableName).toBe("s2");
      expect(instructions[0].realization).toBe("COPY");
      expect(instructions[0].materializedTableId).toBe("local-b2bbbbbbbbbb");
      expect(
         (carried as Record<string, unknown>)["b1aaaaaaaaaaaaaa"],
      ).toBeDefined();
   });

   it("honors the sourceNames filter (excluded sources are neither built nor carried)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", buildId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", buildId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, ["s2"], {});

      expect(instructions).toHaveLength(1);
      expect(instructions[0].buildId).toBe("b2bbbbbbbbbbbbbb");
      expect(Object.keys(carried as Record<string, unknown>)).toHaveLength(0);
   });
});

describe("getMostRecentManifestEntries (skip-if-unchanged)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("returns entries from the most recent successful run, excluding the in-flight one", async () => {
      const entries = {
         b1: {
            buildId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
      };
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "in-flight",
            status: "MANIFEST_ROWS_READY",
         }),
         makeMaterialization({
            id: "old-success",
            status: "MANIFEST_FILE_READY",
            manifest: { builtAt: "t", strict: false, entries },
         }),
      ]);

      const result = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               e: string,
               p: string,
               x: string,
            ) => Promise<unknown>;
         }
      ).getMostRecentManifestEntries("env-1", "pkg", "in-flight");

      expect(result).toEqual(entries);
   });

   it("returns {} when the only successful run is the excluded one", async () => {
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "self",
            status: "MANIFEST_FILE_READY",
            manifest: {
               builtAt: "t",
               strict: false,
               entries: { b1: { buildId: "b1", physicalTableName: "t1" } },
            },
         }),
      ]);

      const result = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               e: string,
               p: string,
               x: string,
            ) => Promise<unknown>;
         }
      ).getMostRecentManifestEntries("env-1", "pkg", "self");

      expect(result).toEqual({});
   });
});

describe("stopMaterialization (in-flight)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("aborts a running build cooperatively without forcing a transition", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "MANIFEST_ROWS_READY" }),
      );
      const controller = new AbortController();
      (
         ctx.service as unknown as {
            runningAbortControllers: Map<string, AbortController>;
         }
      ).runningAbortControllers.set("mat-1", controller);

      const result = await ctx.service.stopMaterialization(
         "my-env",
         "pkg",
         "mat-1",
      );

      expect(controller.signal.aborted).toBe(true);
      // The background run records the terminal state; stop must not also write.
      expect(ctx.repository.updateMaterialization.called).toBe(false);
      expect(result.status).toBe("MANIFEST_ROWS_READY");
   });
});

describe("executeInstructedBuild", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callExecute(
      compiled: unknown,
      instructions: BuildInstruction[],
      seed: Record<string, unknown>,
   ): Promise<Record<string, { physicalTableName?: string }>> {
      return (
         ctx.service as unknown as {
            executeInstructedBuild: (
               c: unknown,
               i: BuildInstruction[],
               s: Record<string, unknown>,
               sig: AbortSignal,
            ) => Promise<Record<string, { physicalTableName?: string }>>;
         }
      ).executeInstructedBuild(
         compiled,
         instructions,
         seed,
         new AbortController().signal,
      );
   }

   it("builds instructed sources in dependency order and seeds carried entries", async () => {
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const s1 = fakeSource({ name: "s1", buildId: "b1aaaaaaaaaaaaaa" });
      const s2 = fakeSource({ name: "s2", buildId: "b2bbbbbbbbbbbbbb" });
      const compiled = compiledWith(
         { s1, s2 },
         [["s1"], ["s2"]],
         new Map([["duckdb", connection]]),
      );

      const entries = await callExecute(
         compiled,
         [
            {
               buildId: "b1aaaaaaaaaaaaaa",
               materializedTableId: "mt-1",
               physicalTableName: "s1_v1",
               realization: "COPY",
            },
            {
               buildId: "b2bbbbbbbbbbbbbb",
               materializedTableId: "mt-2",
               physicalTableName: "s2_v1",
               realization: "COPY",
            },
         ],
         {
            carried0: {
               buildId: "carried0",
               physicalTableName: "carried_tbl",
               connectionName: "duckdb",
            },
         },
      );

      // Both instructed sources built, plus the carried seed entry retained.
      expect(entries["b1aaaaaaaaaaaaaa"].physicalTableName).toBe("s1_v1");
      expect(entries["b2bbbbbbbbbbbbbb"].physicalTableName).toBe("s2_v1");
      expect(entries["carried0"].physicalTableName).toBe("carried_tbl");
   });

   it("throws when an instructed graph's connection is missing", async () => {
      const s1 = fakeSource({ name: "s1", buildId: "b1aaaaaaaaaaaaaa" });
      const compiled = compiledWith({ s1 }, [["s1"]], new Map());

      await expect(
         callExecute(
            compiled,
            [
               {
                  buildId: "b1aaaaaaaaaaaaaa",
                  materializedTableId: "mt-1",
                  physicalTableName: "s1_v1",
                  realization: "COPY",
               },
            ],
            {},
         ),
      ).rejects.toThrow(BadRequestError);
   });
});

describe("buildOneSource", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callBuildOneSource(
      connection: { runSQL: sinon.SinonStub },
      physicalTableName: string,
   ): Promise<{ buildId: string; physicalTableName: string }> {
      const source = fakeSource({
         name: "orders",
         buildId: "abcdef1234567890",
         sql: "SELECT * FROM t",
      });
      const instruction: BuildInstruction = {
         buildId: "abcdef1234567890",
         materializedTableId: "mt-1",
         physicalTableName,
         realization: "COPY",
      };
      return (
         ctx.service as unknown as {
            buildOneSource: (
               s: unknown,
               i: BuildInstruction,
               c: unknown,
               d: Record<string, string>,
               m: Manifest,
            ) => Promise<{ buildId: string; physicalTableName: string }>;
         }
      ).buildOneSource(
         source,
         instruction,
         connection,
         { duckdb: "dig" },
         new Manifest(),
      );
   }

   it("stages, swaps, and renames in a crash-safe order", async () => {
      const runSQL = sinon.stub().resolves();
      const entry = await callBuildOneSource({ runSQL }, "orders_v1");

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         "DROP TABLE IF EXISTS orders_v1_abcdef123456",
         "CREATE TABLE orders_v1_abcdef123456 AS (SELECT * FROM t)",
         "DROP TABLE IF EXISTS orders_v1",
         "ALTER TABLE orders_v1_abcdef123456 RENAME TO orders_v1",
      ]);
      expect(entry.physicalTableName).toBe("orders_v1");
      expect(entry.buildId).toBe("abcdef1234567890");
   });

   it("drops the staging table and rethrows when the build SQL fails", async () => {
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("create boom")); // CREATE TABLE AS
      runSQL.onCall(2).resolves(); // cleanup staging drop
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "create boom",
      );
      expect(runSQL.lastCall.args[0]).toBe(
         "DROP TABLE IF EXISTS orders_v1_abcdef123456",
      );
   });
});

describe("runBuild (branch behavior)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   // Exercise runBuild's orchestration in isolation: the build engine and
   // manifest commit are stubbed so the test asserts which instructions flow
   // through and whether the manifest is auto-loaded, not how a source is built.
   type RunBuildInternals = {
      executeInstructedBuild: sinon.SinonStub;
      commitManifest: sinon.SinonStub;
      autoLoadManifest: sinon.SinonStub;
      runBuild: (
         id: string,
         env: string,
         pkg: string,
         opts: {
            sourceNames: string[] | undefined;
            forceRefresh: boolean;
            buildInstructions: BuildInstruction[] | undefined;
         },
         signal: AbortSignal,
      ) => Promise<void>;
   };

   function stubEngine(): RunBuildInternals {
      const entries = {
         "build-orders": {
            buildId: "build-orders",
            physicalTableName: '"orders_v1"',
            connectionName: "duckdb",
         },
      };
      const svc = ctx.service as unknown as RunBuildInternals;
      svc.executeInstructedBuild = sinon.stub().resolves(entries);
      svc.commitManifest = sinon.stub().resolves();
      svc.autoLoadManifest = sinon.stub().resolves();
      return svc;
   }

   it("orchestrated: builds caller instructions and does not auto-load", async () => {
      const svc = stubEngine();
      const instructions = [makeInstruction()];

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: instructions,
         },
         new AbortController().signal,
      );

      expect(svc.executeInstructedBuild.calledOnce).toBe(true);
      // Caller instructions pass straight through; nothing is carried/reused.
      expect(svc.executeInstructedBuild.firstCall.args[1]).toEqual(
         instructions,
      );
      expect(svc.executeInstructedBuild.firstCall.args[2]).toEqual({});
      expect(svc.commitManifest.firstCall.args[2]).toMatchObject({
         mode: "orchestrated",
         sourcesBuilt: 1,
         sourcesReused: 0,
      });
      // Orchestrated leaves distribution to the caller.
      expect(svc.autoLoadManifest.called).toBe(false);
   });

   it("auto-run: derives instructions and auto-loads the manifest", async () => {
      const svc = stubEngine();

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: true,
            buildInstructions: undefined,
         },
         new AbortController().signal,
      );

      expect(svc.commitManifest.firstCall.args[2]).toMatchObject({
         mode: "auto",
      });
      // Auto-run owns distribution: it loads the fresh manifest into the models.
      expect(svc.autoLoadManifest.calledOnce).toBe(true);
   });
});

describe("autoLoadManifest", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("loads only entries with a physical table into the package models", async () => {
      const reloadAllModelsForPackage = sinon.stub().resolves();
      const entries = {
         b1: {
            buildId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
         b2: { buildId: "b2", physicalTableName: undefined },
      };

      await (
         ctx.service as unknown as {
            autoLoadManifest: (
               env: { reloadAllModelsForPackage: sinon.SinonStub },
               pkg: string,
               entries: Record<string, unknown>,
            ) => Promise<void>;
         }
      ).autoLoadManifest({ reloadAllModelsForPackage }, "pkg", entries);

      expect(reloadAllModelsForPackage.calledOnce).toBe(true);
      expect(reloadAllModelsForPackage.firstCall.args).toEqual([
         "pkg",
         { b1: { tableName: "orders_v1" } },
      ]);
   });

   it("swallows a reload failure (best-effort; run already succeeded)", async () => {
      const reloadAllModelsForPackage = sinon
         .stub()
         .rejects(new Error("reload boom"));
      await expect(
         (
            ctx.service as unknown as {
               autoLoadManifest: (
                  env: { reloadAllModelsForPackage: sinon.SinonStub },
                  pkg: string,
                  entries: Record<string, unknown>,
               ) => Promise<void>;
            }
         ).autoLoadManifest({ reloadAllModelsForPackage }, "pkg", {
            b1: { buildId: "b1", physicalTableName: "t" },
         }),
      ).resolves.toBeUndefined();
   });
});

describe("runInBackground (terminal recording)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   const flush = () => new Promise((r) => setTimeout(r, 20));

   function background(): {
      runInBackground: (
         id: string,
         run: (signal: AbortSignal) => Promise<void>,
      ) => void;
      runningAbortControllers: Map<string, AbortController>;
   } {
      return ctx.service as unknown as {
         runInBackground: (
            id: string,
            run: (signal: AbortSignal) => Promise<void>,
         ) => void;
         runningAbortControllers: Map<string, AbortController>;
      };
   }

   it("records FAILED with the error message when the run rejects", async () => {
      background().runInBackground("bg-1", async () => {
         throw new Error("boom");
      });
      await flush();

      expect(ctx.repository.updateMaterialization.calledOnce).toBe(true);
      expect(ctx.repository.updateMaterialization.firstCall.args[0]).toBe(
         "bg-1",
      );
      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "FAILED", error: "boom" });
   });

   it("records CANCELLED when the run aborts cooperatively", async () => {
      const bg = background();
      bg.runInBackground("bg-2", async () => {
         bg.runningAbortControllers.get("bg-2")!.abort();
         throw new Error("boom");
      });
      await flush();

      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "CANCELLED", error: "Cancelled" });
   });

   it("clears the abort controller once the run settles", async () => {
      const bg = background();
      bg.runInBackground("bg-3", async () => {});
      await flush();
      expect(bg.runningAbortControllers.has("bg-3")).toBe(false);
   });
});
