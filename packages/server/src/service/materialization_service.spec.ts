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
   BuildPlan,
   Materialization,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { EnvironmentStore } from "./environment_store";
import {
   MaterializationService,
   stagingSuffix,
} from "./materialization_service";

function makeMaterialization(
   overrides: Partial<Materialization> = {},
): Materialization {
   return {
      id: "mat-1",
      environmentId: "env-1",
      packageName: "pkg",
      pauseBetweenPhases: false,
      status: "PENDING",
      buildPlan: null,
      manifest: null,
      startedAt: null,
      completedAt: null,
      error: null,
      metadata: null,
      createdAt: new Date("2026-01-01"),
      updatedAt: new Date("2026-01-01"),
      ...overrides,
   };
}

function makeBuildPlan(overrides: Partial<BuildPlan> = {}): BuildPlan {
   return {
      graphs: [
         {
            connectionName: "duckdb",
            nodes: [[{ sourceID: "orders@m.malloy", dependsOn: [] }]],
         },
      ],
      sources: {
         "orders@m.malloy": {
            name: "orders",
            sourceID: "orders@m.malloy",
            connectionName: "duckdb",
            dialect: "duckdb",
            buildId: "build-orders",
            sql: "SELECT 1",
            columns: [],
         },
      },
      ...overrides,
   };
}

function makeInstruction(
   overrides: Partial<BuildInstruction> = {},
): BuildInstruction {
   return {
      buildId: "build-orders",
      materializedTableId: "mt-1",
      physicalTableName: '"orders_v1"',
      realization: "COPY",
      ...overrides,
   };
}

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

   // Default: environment resolves and yields a package.
   repository.getEnvironmentByName.resolves({
      id: "env-1",
      name: "my-env",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      getPackage: sinon.stub().resolves({}),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) =>
         fn(),
   });

   const service = new MaterializationService(environmentStore);

   return { sandbox, repository, environmentStore, service };
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

   describe("createMaterialization (Round 1)", () => {
      it("creates a PENDING record and persists options", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeMaterialization({ status: "PENDING" });
         ctx.repository.createMaterialization.resolves(pending);

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            {
               forceRefresh: true,
               sourceNames: ["orders"],
               pauseBetweenPhases: true,
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
               pauseBetweenPhases: true,
            },
         ]);
      });

      it("defaults pauseBetweenPhases to false (auto-run) in metadata", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         await ctx.service.createMaterialization("my-env", "pkg", {});

         expect(ctx.repository.createMaterialization.firstCall.args[3]).toEqual(
            {
               forceRefresh: false,
               sourceNames: null,
               pauseBetweenPhases: false,
            },
         );
      });

      it("rejects when an active materialization already exists", async () => {
         ctx.repository.getActiveMaterialization.resolves(
            makeMaterialization({ id: "existing", status: "BUILD_PLAN_READY" }),
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

   describe("buildMaterialization (Round 2)", () => {
      it("rejects action=build on an auto-run materialization", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "BUILD_PLAN_READY",
               pauseBetweenPhases: false,
               buildPlan: makeBuildPlan(),
            }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction(),
            ]),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("rejects when not at BUILD_PLAN_READY", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "PENDING",
               pauseBetweenPhases: true,
            }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction(),
            ]),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("rejects instructions that reference an unknown buildId", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "BUILD_PLAN_READY",
               pauseBetweenPhases: true,
               buildPlan: makeBuildPlan(),
            }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction({ buildId: "ghost" }),
            ]),
         ).rejects.toThrow(BadRequestError);
      });

      it("rejects SNAPSHOT when the connection does not support it", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "BUILD_PLAN_READY",
               pauseBetweenPhases: true,
               buildPlan: makeBuildPlan(),
            }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction({ realization: "SNAPSHOT" }),
            ]),
         ).rejects.toThrow(BadRequestError);
      });

      it("accepts a valid build and returns the record (202)", async () => {
         const m = makeMaterialization({
            status: "BUILD_PLAN_READY",
            pauseBetweenPhases: true,
            buildPlan: makeBuildPlan(),
         });
         ctx.repository.getMaterializationById.resolves(m);
         const result = await ctx.service.buildMaterialization(
            "my-env",
            "pkg",
            "mat-1",
            [makeInstruction()],
         );
         expect(result.id).toBe("mat-1");
      });
   });

   describe("stopMaterialization", () => {
      const cancellable = [
         "PENDING",
         "BUILD_PLAN_READY",
         "MANIFEST_ROWS_READY",
      ] as const;

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

      const active = [
         "PENDING",
         "BUILD_PLAN_READY",
         "MANIFEST_ROWS_READY",
      ] as const;
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

// Characterization (Pass 0): lock in the auto-run instruction-derivation and
// the single-source build SQL sequence before the simplify pass moves them.
// deriveSelfInstructions / buildOneSource are private; we exercise them via a
// typed cast rather than the heavyweight runtime path.

// A minimal stand-in for a Malloy PersistSource exposing only what the build
// internals touch.
function fakeSource(opts: {
   name: string;
   buildId: string;
   sql?: string;
   connectionName?: string;
}): unknown {
   return {
      name: opts.name,
      sourceID: opts.name,
      connectionName: opts.connectionName ?? "duckdb",
      makeBuildId: () => opts.buildId,
      getSQL: () => opts.sql ?? "SELECT 1",
      annotations: {
         parseAsTag: () => ({ tag: { text: () => undefined } }),
      },
   };
}

describe("deriveSelfInstructions (characterization)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function compiledWith(sources: Record<string, unknown>, levels: string[][]) {
      return {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: levels.map((level) =>
                  level.map((sourceID) => ({ sourceID, dependsOn: [] })),
               ),
            },
         ],
         sources,
         connectionDigests: { duckdb: "dig" },
      };
   }

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
         makeMaterialization({ id: "in-flight", status: "MANIFEST_ROWS_READY" }),
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

describe("buildOneSource (characterization)", () => {
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
      ).buildOneSource(source, instruction, connection, { duckdb: "dig" }, new Manifest());
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
